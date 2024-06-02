package shardctrler

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ExecutionResult struct {
	id     RequestId
	config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs   []Config // indexed by config num
	opResults map[int64]ExecutionResult
	resultChs map[int]chan ExecutionResult
}

// TODO: consider using different types for different ops?
type Op struct {
	// Your data here.
	ReqId   RequestId
	Type    string
	Servers map[int][]string // for Join
	Gids    []int            // for Leave/Move
	Shard   int              // for Move
	CfgVer  int              // for Query
}

func (sc *ShardCtrler) queryDuplicateTableNoLock(reqId RequestId) (*Config, bool) {
	recent, exist := sc.opResults[reqId.ClientId]
	if !exist || reqId.SeqNo > recent.id.SeqNo {
		return nil, false
	} else if reqId.SeqNo == recent.id.SeqNo {
		return &recent.config, true
	} else {
		panic("should not happen")
	}
}

func (sc *ShardCtrler) queryDuplicateTable(reqId RequestId) (*Config, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.queryDuplicateTableNoLock(reqId)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, duplicate := sc.queryDuplicateTable(args.ReqId); duplicate {
		reply.Err = OK
		return
	}

	op := Op{
		ReqId:   args.ReqId,
		Type:    "Join",
		Servers: args.Servers,
	}
	internalReply := QueryReply{}
	sc.commonHandler(&op, &internalReply, false)
	reply.Err = internalReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, duplicate := sc.queryDuplicateTable(args.ReqId); duplicate {
		reply.Err = OK
		return
	}

	op := Op{
		ReqId: args.ReqId,
		Type:  "Leave",
		Gids:  args.GIDs,
	}
	internalReply := QueryReply{}
	sc.commonHandler(&op, &internalReply, false)
	reply.Err = internalReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, duplicate := sc.queryDuplicateTable(args.ReqId); duplicate {
		reply.Err = OK
		return
	}

	op := Op{
		ReqId: args.ReqId,
		Type:  "Move",
		Shard: args.Shard,
		Gids:  []int{args.GID},
	}
	internalReply := QueryReply{}
	sc.commonHandler(&op, &internalReply, false)
	reply.Err = internalReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if cfg, duplicate := sc.queryDuplicateTable(args.ReqId); duplicate {
		reply.Err = OK
		reply.Config = *cfg
		return
	}

	op := Op{
		ReqId:  args.ReqId,
		Type:   "Query",
		CfgVer: args.Num,
	}
	sc.commonHandler(&op, reply, true)
}

func (sc *ShardCtrler) commonHandler(op *Op, reply *QueryReply, isQuery bool) {
	logIndex, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	resultCh := make(chan ExecutionResult)

	sc.mu.Lock()
	sc.resultChs[logIndex] = resultCh
	sc.mu.Unlock()

	timeout := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case execResult := <-resultCh:
		// FIXME: sometimes bad value come out of channel
		if execResult.id != op.ReqId {
			reply.Err = ErrRetry
			return
		}
		if isQuery {
			reply.Config = execResult.config
		}
	case <-ctx.Done():
		reply.Err = ErrTimeout
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) currentConfig() *Config {
	return &sc.configs[len(sc.configs)-1]
}

func copyGroupsMap(dst, src map[int][]string) {
	for k, v := range src {
		dst[k] = v
	}
}

func assignShardMappingNaive(gids []int) [NShards]int {
	var shards [NShards]int
	if len(gids) > 0 {
		sort.Ints(gids)
		for i := 0; i < NShards; i++ {
			shards[i] = gids[i%len(gids)]
		}
	}
	return shards
}

func rebalanceShardMapping(shards [NShards]int, newGroups map[int][]string) [NShards]int {
	// return all-zero shard mapping if no valid group id
	if len(newGroups) == 0 {
		return [NShards]int{}
	}

	gids := make([]int, 0, len(newGroups))
	for gid := range newGroups {
		gids = append(gids, gid)
	}

	invalidShards := make([]int, 0, NShards)
	for shardId, gid := range shards {
		if _, valid := newGroups[gid]; !valid {
			invalidShards = append(invalidShards, shardId)
		}
	}

	groupShardCount := make(map[int]int)
	for _, gid := range shards {
		groupShardCount[gid]++
	}

	isBalanced := func() bool {
		minCount, maxCount := NShards, 0
		for _, gid := range gids {
			count := groupShardCount[gid]
			if minCount > count {
				minCount = count
			}
			if maxCount < count {
				maxCount = count
			}
		}
		return maxCount-minCount <= 1 && len(invalidShards) == 0
	}

	for !isBalanced() {
		sort.Slice(gids, func(i, j int) bool {
			g1, g2 := gids[i], gids[j]
			c1, c2 := groupShardCount[g1], groupShardCount[g2]
			if c1 != c2 {
				return c1 < c2
			}
			return g1 < g2
		})

		dst := gids[0]
		shardId := -1
		if len(invalidShards) > 0 {
			shardId = invalidShards[0]
			invalidShards = invalidShards[1:]
		} else {
			src := gids[len(gids)-1]
			groupShardCount[src]--
			for i, gid := range shards {
				if gid == src {
					shardId = i
					break
				}
			}
		}
		shards[shardId] = dst
		groupShardCount[dst]++
	}
	return shards
}

func (sc *ShardCtrler) doJoin(op *Op) {
	cfg := sc.currentConfig()

	newGroups := make(map[int][]string)
	copyGroupsMap(newGroups, cfg.Groups)
	copyGroupsMap(newGroups, op.Servers)

	newCfg := Config{
		Num:    len(sc.configs),
		Shards: rebalanceShardMapping(cfg.Shards, newGroups),
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) doLeave(op *Op) {
	cfg := sc.currentConfig()

	newGroups := make(map[int][]string)
	copyGroupsMap(newGroups, cfg.Groups)
	for _, gid := range op.Gids {
		delete(newGroups, gid)
	}

	newCfg := Config{
		Num:    len(sc.configs),
		Shards: rebalanceShardMapping(cfg.Shards, newGroups),
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) doMove(op *Op) {
	cfg := sc.currentConfig()

	newGroups := make(map[int][]string)
	copyGroupsMap(newGroups, cfg.Groups)

	newShards := cfg.Shards
	newShards[op.Shard] = op.Gids[0]

	newCfg := Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) executeOp(op *Op) *Config {
	switch op.Type {
	case "Join":
		sc.doJoin(op)
	case "Leave":
		sc.doLeave(op)
	case "Move":
		sc.doMove(op)
	case "Query":
		if op.CfgVer < 0 || op.CfgVer >= len(sc.configs) {
			return sc.currentConfig()
		} else {
			return &sc.configs[op.CfgVer]
		}
	default:
		panic("unknown op")
	}
	return nil
}

func (sc *ShardCtrler) executorLoop() {
	for !sc.killed() {
		msg := <-sc.applyCh

		sc.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)

			result := ExecutionResult{id: op.ReqId}
			cfg, duplicate := sc.queryDuplicateTableNoLock(op.ReqId)
			if !duplicate {
				cfg = sc.executeOp(&op)
			}
			if cfg != nil {
				result.config = *cfg
			}
			sc.opResults[op.ReqId.ClientId] = result

			logIndex := msg.CommandIndex
			ch, chanExist := sc.resultChs[logIndex]
			if chanExist {
				delete(sc.resultChs, logIndex)

				select {
				case ch <- result:
				default:
				}
				close(ch)
			}
		} else {
			panic("todo")
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opResults = make(map[int64]ExecutionResult)
	sc.resultChs = make(map[int]chan ExecutionResult)

	go sc.executorLoop()
	return sc
}
