package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNotReady"
	ErrTimeout     = "ErrTimeout"
	ErrReject      = "ErrReject"
)

type Err string

type RequestId struct {
	ClientId int64
	SeqNo    int
}

type ExecutionResult struct {
	Id    RequestId
	Value string
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ReqId RequestId
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ReqId RequestId
	Key   string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
