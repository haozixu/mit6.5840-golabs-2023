package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CopyConfig(src *Config) *Config {
	newGroups := make(map[int][]string)
	for k, v := range src.Groups {
		newGroups[k] = v
	}

	return &Config{
		Num:    src.Num,
		Shards: src.Shards,
		Groups: newGroups,
	}
}

const (
	OK             = "OK"
	ErrRetry       = "ErrRetry"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type RequestId struct {
	ClientId int64
	SeqNo    int
}

type JoinArgs struct {
	ReqId   RequestId
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ReqId RequestId
	GIDs  []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ReqId RequestId
	Shard int
	GID   int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ReqId RequestId
	Num   int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}
