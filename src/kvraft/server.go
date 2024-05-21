package kvraft

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqNo    int
	Type     string // "Get", "Put" or "Append"
	Key      string
	Value    string
}

type RecentResult struct {
	seqNo int
	value string
}

type DuplicateTable struct {
	inner map[int64]RecentResult // map client id --> recent result
}

type ReqId struct {
	clientId int64
	seqNo    int
}

type ExecutionResult struct {
	id    ReqId
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state     map[string]string
	opResults DuplicateTable
	resultChs map[ReqId]chan ExecutionResult
}

func makeDuplicateTable() DuplicateTable {
	return DuplicateTable{make(map[int64]RecentResult)}
}

func (dt *DuplicateTable) getClientResult(clientId int64) RecentResult {
	res, exist := dt.inner[clientId]
	if !exist {
		res = RecentResult{seqNo: 0}
		dt.inner[clientId] = res
	}
	return res
}

func (dt *DuplicateTable) trimOutdated(clientId int64, seqNo int) {
	// do nothing. but panic if sequence number violate our assumption
	recent := dt.getClientResult(clientId)

	if seqNo < recent.seqNo {
		panic("sequence out of order")
	}
}

func (dt *DuplicateTable) query(clientId int64, seqNo int) (string, bool) {
	recent := dt.getClientResult(clientId)

	if seqNo > recent.seqNo {
		return "", false
	} else if seqNo == recent.seqNo {
		return recent.value, true
	} else {
		panic("sequence out of order")
	}
}

func (dt *DuplicateTable) update(clientId int64, seqNo int, result string) {
	if seqNo <= 0 {
		panic("bad sequence number")
	}

	dt.inner[clientId] = RecentResult{seqNo, result}
}

func (kv *KVServer) queryDuplicateTable(clientId int64, seqNo int) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result, ok := kv.opResults.query(clientId, seqNo)
	kv.opResults.trimOutdated(clientId, seqNo)
	return result, ok
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	clientId, seqNo := args.ClientId, args.SeqNo
	result, duplicate := kv.queryDuplicateTable(clientId, seqNo)
	if duplicate {
		reply.Err = OK
		reply.Value = result
		return
	}

	op := Op{
		ClientId: clientId,
		SeqNo:    seqNo,
		Type:     "Get",
		Key:      args.Key,
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resultCh := make(chan ExecutionResult)
	reqId := ReqId{clientId, seqNo}

	kv.mu.Lock()
	kv.resultChs[reqId] = resultCh
	kv.mu.Unlock()

	timeout := 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case execResult := <-resultCh:
		// FIXME: sometimes bad value come out of channel
		if execResult.id != reqId {
			reply.Err = ErrRetry
			return
		}
		result = execResult.value
	case <-ctx.Done():
		// log.Printf("KVServer: Get timeout!\n")
		reply.Err = ErrTimeout
		return
	}

	// TODO: do we need to check index (prev == current) ?
	_, isLeaderNow := kv.rf.GetState()
	if !isLeaderNow {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = result

	// log.Printf("kvs final return Get: val=%v client=%v seq=%d", result, clientId, seqNo)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	clientId, seqNo := args.ClientId, args.SeqNo
	_, duplicate := kv.queryDuplicateTable(clientId, seqNo)
	if duplicate {
		reply.Err = OK
		return
	}

	op := Op{
		ClientId: clientId,
		SeqNo:    seqNo,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resultCh := make(chan ExecutionResult)
	reqId := ReqId{clientId, seqNo}

	kv.mu.Lock()
	kv.resultChs[reqId] = resultCh
	kv.mu.Unlock()

	timeout := 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case execResult := <-resultCh:
		// FIXME: sometimes bad value come out of channel
		if execResult.id != reqId {
			reply.Err = ErrRetry
			return
		}
	case <-ctx.Done():
		// log.Printf("KVServer: PutAppend timeout!\n")
		reply.Err = ErrTimeout
		return
	}

	// TODO: do we need to check index (prev == current) ?
	_, isLeaderNow := kv.rf.GetState()
	if !isLeaderNow {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *KVServer) executeOp(op *Op) string {
	switch op.Type {
	case "Get":
		return kv.state[op.Key] // return empty string if key doesn't exist
	case "Put":
		kv.state[op.Key] = op.Value
	case "Append":
		kv.state[op.Key] += op.Value
	default:
		panic("unknown op")
	}
	return ""
}

func (kv *KVServer) executorLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			clientId, seqNo := op.ClientId, op.SeqNo

			if clientId == 0 && seqNo == 0 {
				log.Printf("bad op: %v", op)
				panic("bad op")
			}

			// guard against re-execution
			var result string
			recentResult := kv.opResults.getClientResult(clientId)
			if seqNo > recentResult.seqNo {
				result = kv.executeOp(&op)
				kv.opResults.update(clientId, seqNo, result)

				// log.Printf("@ S%d EXECUTE client=%v seq=%d op=%s key=%s result=%s", kv.me, op.ClientId, op.SeqNo, op.Type, op.Key, result)
			} else if seqNo == recentResult.seqNo {
				result = recentResult.value
			} else {
				panic("???")
			}

			// wake up waiting RPC handlers
			reqId := ReqId{clientId, seqNo}
			ch, hasWaiter := kv.resultChs[reqId]
			if hasWaiter {
				delete(kv.resultChs, reqId)

				select {
				case ch <- ExecutionResult{reqId, result}:
				default:
					// log.Printf("warn: no peer\n")
				}
				close(ch)
			}
		} else {
			panic("todo")
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.state = make(map[string]string)
	kv.opResults = makeDuplicateTable()
	kv.resultChs = make(map[ReqId]chan ExecutionResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.executorLoop()
	return kv
}
