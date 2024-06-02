package shardkv

import (
	"bytes"
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	// deadlock "github.com/sasha-s/go-deadlock"
)

type KvOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId RequestId
	Type  string
	Key   string
	Value string
}

type KvStore = map[string]string
type DupTable = map[int64]ExecutionResult

type ShardData struct {
	ConfigVersion int
	Shard         int
	KvData        KvStore
	OpResults     DupTable
}

func copyShardData(kvData KvStore, opResults DupTable) (KvStore, DupTable) {
	newKvData := make(KvStore)
	newOpResults := make(DupTable)
	for k, v := range kvData {
		newKvData[k] = v
	}
	for clientId, execResult := range opResults {
		newOpResults[clientId] = execResult
	}
	return newKvData, newOpResults
}

type RecvShardDataArgs = ShardData
type RecvShardDataReply struct {
	Err Err
}

// configuration related ops (used in raft log)
type ReConfigOp struct {
	Config shardctrler.Config
}

type AcceptShardOp = ShardData

type RemoveShardOp struct {
	ConfigVersion int
	Shard         int
}

type ShardState = int

const (
	Missing  = 0
	Normal   = 1
	Retiring = 2
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	dead         int32
	shardCtrl    *shardctrler.Clerk

	// Your definitions here.
	// persisted states
	kvData      map[int]KvStore
	opResults   map[int]DupTable
	shardsState map[int]ShardState
	config      shardctrler.Config
	// prevConfig  shardctrler.Config
	lastApplied int

	// ephemeral states
	resultChs map[int]chan ExecutionResult
	notifyChs map[int]chan bool
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) initState() {
	kv.kvData = make(map[int]KvStore)
	kv.opResults = make(map[int]DupTable)
	kv.shardsState = make(map[int]ShardState)
	kv.config = shardctrler.Config{} // zero initialization
	// kv.prevConfig = shardctrler.Config{} // zero initialization
	kv.lastApplied = 0
}

func (kv *ShardKV) isInServiceNoLock(shard int) Err {
	if kv.config.Shards[shard] != kv.gid {
		return ErrWrongGroup
	}
	switch kv.shardsState[shard] {
	case Normal:
		return OK
	case Missing:
		return ErrNotReady
	default:
		return ErrWrongGroup
	}
}

func (kv *ShardKV) isInService(shard int) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.isInServiceNoLock(shard)
}

func (kv *ShardKV) readyForNewConfig() bool {
	for _, state := range kv.shardsState {
		if state != Normal {
			return false
		}
	}
	return true
}

func (kv *ShardKV) queryDuplicateTableNoLock(shard int, reqId RequestId) (string, bool) {
	recent, exist := kv.opResults[shard][reqId.ClientId]

	if !exist || reqId.SeqNo > recent.Id.SeqNo {
		return "", false
	} else if reqId.SeqNo == recent.Id.SeqNo {
		return recent.Value, true
	} else {
		log.Printf("(S%d, group %d): shard %d, recent %v, reqId %v", kv.me, kv.gid, shard, recent, reqId)
		panic("should not happen")
	}
}

func (kv *ShardKV) queryDuplicateTable(shard int, reqId RequestId) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.queryDuplicateTableNoLock(shard, reqId)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	if errMsg := kv.isInService(shard); errMsg != OK {
		reply.Err = errMsg
		return
	}

	result, duplicate := kv.queryDuplicateTable(shard, args.ReqId)
	if duplicate {
		reply.Err = OK
		reply.Value = result
		return
	}

	op := KvOp{
		Type:  "Get",
		ReqId: args.ReqId,
		Key:   args.Key,
	}
	kv.commonKvHandler(&op, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	if errMsg := kv.isInService(shard); errMsg != OK {
		reply.Err = errMsg
		return
	}

	_, duplicate := kv.queryDuplicateTable(shard, args.ReqId)
	if duplicate {
		reply.Err = OK
		return
	}

	op := KvOp{
		Type:  args.Op,
		ReqId: args.ReqId,
		Key:   args.Key,
		Value: args.Value,
	}
	internalReply := GetReply{}
	kv.commonKvHandler(&op, &internalReply)
	reply.Err = internalReply.Err
}

func (kv *ShardKV) RecvShardData(args *RecvShardDataArgs, reply *RecvShardDataReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// TODO: simplify kv.mu.Unlock()
	kv.mu.Lock()
	if kv.config.Num > args.ConfigVersion {
		// already a new config version, the previous shard was accepted
		reply.Err = OK
		kv.mu.Unlock()
		return
	} else if kv.config.Num < args.ConfigVersion {
		// version mismatch
		reply.Err = ErrReject
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.shardsState[args.Shard]; !ok {
		panic("no shard state")
	}
	if kv.shardsState[args.Shard] == Normal {
		// already accepted
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	log.Printf("(S%d, group %d): RecvShard: shardsState=%v", kv.me, kv.gid, kv.shardsState)
	// assert kv.shardsState[args.Shard] == Missing
	kv.mu.Unlock()

	// TODO: do we really need to copy here?
	newKvData, newOpResults := copyShardData(args.KvData, args.OpResults)
	op := AcceptShardOp{
		ConfigVersion: args.ConfigVersion,
		Shard:         args.Shard,
		KvData:        newKvData,
		OpResults:     newOpResults,
	}

	// NOTE: don't call kv.rf.Start when posessing kv.mu!
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan bool)
	kv.mu.Lock()
	kv.notifyChs[index] = notifyCh
	kv.mu.Unlock()

	timeout := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case ok := <-notifyCh:
		if !ok {
			reply.Err = ErrReject
			return
		}
	case <-ctx.Done():
		reply.Err = ErrTimeout
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) commonKvHandler(op *KvOp, reply *GetReply) {
	logIndex, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resultCh := make(chan ExecutionResult)

	kv.mu.Lock()
	kv.resultChs[logIndex] = resultCh
	kv.mu.Unlock()

	timeout := 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case execResult := <-resultCh:
		if execResult.Id != op.ReqId {
			// NOTE: we might need to add error reason to ExecutionResult
			reply.Err = ErrNotReady
			return
		}
		reply.Value = execResult.Value
	case <-ctx.Done():
		reply.Err = ErrTimeout
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) executeKvOp(shard int, op *KvOp) string {
	switch op.Type {
	case "Get":
		return kv.kvData[shard][op.Key]
	case "Put":
		kv.kvData[shard][op.Key] = op.Value
	case "Append":
		kv.kvData[shard][op.Key] += op.Value
	default:
		panic("unsupported op")
	}
	return ""
}

func (kv *ShardKV) processKvOp(op *KvOp, logIndex int) {
	var execResult ExecutionResult
	shard := key2shard(op.Key)
	if err := kv.isInServiceNoLock(shard); err == OK {
		reqId := op.ReqId
		result, duplicate := kv.queryDuplicateTableNoLock(shard, reqId)
		if !duplicate {
			result = kv.executeKvOp(shard, op)
			log.Printf("(S%d, group %d): index=%d execute op: %v", kv.me, kv.gid, logIndex, *op)
		}

		// update duplicate table
		execResult = ExecutionResult{reqId, result}
		kv.opResults[shard][reqId.ClientId] = execResult
	} else {
		// we are not responsible for this request now
		// intentionally set a non-existing request id
		execResult.Id = RequestId{-1, -1}
	}

	// wake up waiting RPC handlers
	ch, hasWaiter := kv.resultChs[logIndex]
	if hasWaiter {
		delete(kv.resultChs, logIndex)

		select {
		case ch <- execResult:
		default:
		}
		close(ch)
	}
}

func (kv *ShardKV) notifyWaitingHandlers(logIndex int, value bool) {
	ch, hasWaiter := kv.notifyChs[logIndex]
	if hasWaiter {
		delete(kv.notifyChs, logIndex)

		select {
		case ch <- value:
		default:
		}
		close(ch)
	}
}

func (kv *ShardKV) acceptShardData(op *AcceptShardOp) bool {
	if op.ConfigVersion != kv.config.Num {
		log.Printf("~~~~~ current version: %d incoming version: %d", kv.config.Num, op.ConfigVersion)
		return false
	}

	shard := op.Shard
	if kv.shardsState[shard] == Missing {
		log.Printf("(S%d, group %d): accept shard %d cfgVer: %d, oldTable: %v, newTable: %v",
			kv.me, kv.gid, shard, op.ConfigVersion, kv.opResults[shard], op.OpResults)

		kv.kvData[shard], kv.opResults[shard] = copyShardData(op.KvData, op.OpResults)
		kv.shardsState[shard] = Normal
	} else {
		log.Printf("(S%d, group %d): NoAccept shard %d cfgVer: %d, oldTable: %v, newTable: %v",
			kv.me, kv.gid, shard, op.ConfigVersion, kv.opResults[shard], op.OpResults)
	}
	return true
}

func (kv *ShardKV) removeShardData(op *RemoveShardOp) {
	if op.ConfigVersion != kv.config.Num {
		return
	}

	shard := op.Shard
	if kv.shardsState[shard] == Retiring {
		kv.kvData[shard] = nil
		kv.opResults[shard] = nil
		kv.shardsState[shard] = Normal

		log.Printf("(S%d, group %d): remove shard %d", kv.me, kv.gid, shard)
	}
}

func (kv *ShardKV) updateConfig(newConfig *shardctrler.Config) {
	logIndex := kv.lastApplied
	log.Printf("(S%d, group %d): updateConfig: index=%d, old=%v, new=%v", kv.me, kv.gid, logIndex, kv.config, *newConfig)

	if newConfig.Num != kv.config.Num+1 {
		log.Printf("... refuse config version %d, expected %d", newConfig.Num, kv.config.Num+1)
		return
	}

	oldConfig := &kv.config

	for i := range newConfig.Shards {
		prevGid := oldConfig.Shards[i]
		prev := oldConfig.Shards[i] == kv.gid
		curr := newConfig.Shards[i] == kv.gid

		var state ShardState
		if !prev && curr {
			if _, ok := oldConfig.Groups[prevGid]; ok {
				state = Missing
			} else {
				// invalid previous gid, initializing from empty shard
				state = Normal
				kv.kvData[i] = make(KvStore)
				kv.opResults[i] = make(DupTable)
			}
		} else if prev && !curr {
			state = Retiring
		} else {
			state = Normal
		}
		kv.shardsState[i] = state
	}
	log.Printf("(S%d, group %d): newCfgVer=%d shardsState: %v", kv.me, kv.gid, newConfig.Num, kv.shardsState)

	// kv.prevConfig = *oldConfig
	kv.config = *newConfig
}

func (kv *ShardKV) executorLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		kv.mu.Lock()
		if msg.CommandValid {
			logIndex := msg.CommandIndex
			if logIndex < kv.lastApplied {
				panic("apply index out of order")
			}
			kv.lastApplied = logIndex

			if op, ok := msg.Command.(KvOp); ok {
				kv.processKvOp(&op, logIndex)
			} else if op, ok := msg.Command.(ReConfigOp); ok {
				kv.updateConfig(&op.Config)
			} else if op, ok := msg.Command.(AcceptShardOp); ok {
				ok := kv.acceptShardData(&op)
				kv.notifyWaitingHandlers(logIndex, ok)
			} else if op, ok := msg.Command.(RemoveShardOp); ok {
				kv.removeShardData(&op)
			} else {
				panic("unknown op")
			}

			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
			}
		} else if msg.SnapshotValid {
			if msg.SnapshotIndex > kv.lastApplied {
				kv.applySnapshot(msg.Snapshot)
			}
		} else {
			panic("todo")
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) sendShardData(cfg *shardctrler.Config, shardData *ShardData) {
	shard := shardData.Shard
	for {
		gid := cfg.Shards[shard]
		for _, serverName := range cfg.Groups[gid] {
			service := kv.make_end(serverName)
			var reply RecvShardDataReply
			ok := service.Call("ShardKV.RecvShardData", shardData, &reply)
			if ok && reply.Err == OK {
				kv.rf.Start(RemoveShardOp{cfg.Num, shard})
				return
			}
			if ok && reply.Err == ErrReject {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) transferShardData() {
	kv.mu.Lock()
	config := shardctrler.CopyConfig(&kv.config)
	data := make([]ShardData, 0, shardctrler.NShards)
	for shard := range config.Shards {
		if kv.shardsState[shard] == Retiring {
			// copy shard data
			localKvData, localOpResults := copyShardData(kv.kvData[shard], kv.opResults[shard])
			data = append(data, ShardData{config.Num, shard, localKvData, localOpResults})
		}
	}
	kv.mu.Unlock()

	// TODO: allow shared read of config fields? (assume they are read-only)
	for i := range data {
		go kv.sendShardData(config, &data[i])
	}
}

// NOTE: we need this loop to retry in case leader changed
func (kv *ShardKV) transferShardDataLoop() {
	waitInterval := 50 * time.Millisecond

	for !kv.killed() {
		time.Sleep(waitInterval)

		if !kv.isLeader() {
			continue
		}

		kv.transferShardData()
	}
}

// NOTE: we want to observer new config as soon as possible
func (kv *ShardKV) configWatcher() {
	pollInterval := 20 * time.Millisecond
	waitInterval := 100 * time.Millisecond

	for !kv.killed() {
		waitLonger := func() bool {
			if !kv.isLeader() {
				return false
			}

			kv.mu.Lock()
			oldVersion := kv.config.Num
			ready := kv.readyForNewConfig()
			kv.mu.Unlock()

			if !ready {
				kv.mu.Lock()
				log.Printf("(S%d, group %d): config=%v shardsState=%v", kv.me, kv.gid, kv.config, kv.shardsState)
				kv.mu.Unlock()
				return true
			}

			// NOTE: we can only move on to the next consecutive config
			newConfig := kv.shardCtrl.Query(oldVersion + 1)
			if newConfig.Num != oldVersion+1 {
				return true
			}

			// NOTE: config query may take a long time and current config can be already changed
			// consider checking current config again or refuse stale config in executor
			kv.mu.Lock()
			ok := (kv.config.Num == oldVersion)
			kv.mu.Unlock()

			if ok {
				kv.rf.Start(ReConfigOp{newConfig})
			}
			return ok
		}()

		if waitLonger {
			time.Sleep(waitInterval)
		} else {
			time.Sleep(pollInterval)
		}
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(kv.kvData)
	e.Encode(kv.opResults)
	e.Encode(kv.shardsState)
	e.Encode(kv.config)
	// e.Encode(kv.prevConfig)
	e.Encode(kv.lastApplied)
	return buf.Bytes()
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	d := labgob.NewDecoder(bytes.NewBuffer(snapshot))
	if d.Decode(&kv.kvData) != nil ||
		d.Decode(&kv.opResults) != nil ||
		d.Decode(&kv.shardsState) != nil ||
		d.Decode(&kv.config) != nil ||
		// d.Decode(&kv.prevConfig) != nil ||
		d.Decode(&kv.lastApplied) != nil {
		panic("bad snapshot")
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(KvOp{})
	labgob.Register(ReConfigOp{})
	labgob.Register(AcceptShardOp{})
	labgob.Register(RemoveShardOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// Your initialization code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.applySnapshot(snapshot)
	} else {
		kv.initState()
	}
	kv.resultChs = make(map[int]chan ExecutionResult)
	kv.notifyChs = make(map[int]chan bool)

	// Use something like this to talk to the shardctrler:
	kv.shardCtrl = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.configWatcher()
	go kv.executorLoop()
	go kv.transferShardDataLoop()
	return kv
}
