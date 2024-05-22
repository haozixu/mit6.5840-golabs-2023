package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mtx       sync.Mutex // TODO: I'm not sure if this is needed
	clientId  int64
	curSeqNo  int // current sequence number
	curLeader int // current known leader (default guess 0)
	timeBegin time.Time
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.curSeqNo = 0
	ck.curLeader = 0
	ck.timeBegin = time.Now()
	return ck
}

func (ck *Clerk) newSeqNo() int {
	ck.curSeqNo++
	return ck.curSeqNo
}

func (ck *Clerk) newUid() int64 {
	ns := time.Since(ck.timeBegin).Nanoseconds()
	return (nrand() << 32) | (ns & 0xffffffff)
}

func (ck *Clerk) makeServerOrder() []int {
	serverIds := make([]int, len(ck.servers))
	for i := range serverIds {
		serverIds[i] = i
	}
	serverIds[0] = ck.curLeader
	serverIds[ck.curLeader] = 0
	return serverIds
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mtx.Lock()
	defer ck.mtx.Unlock()

	seqNo := ck.newSeqNo()
	// log.Printf("clerk::Get key=%s, client=%v, #seq=%d", key, ck.clientId, seqNo)

	for {
		serverIds := ck.makeServerOrder()
		for _, serverId := range serverIds {
			args := GetArgs{Key: key, ClientId: ck.clientId, SeqNo: seqNo}
			reply := GetReply{}

			// TODO: handle timeout
			ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

			if !ok || reply.Err != OK {
				continue
			}

			if reply.Err == OK {
				ck.curLeader = serverId

				// vvv := reply.Value
				// if vvv == "" {
				// 	vvv = "(empty)"
				// }
				// log.Printf("clerk::Get key=%s, client=%v, #seq=%d, retval=%s", key, ck.clientId, seqNo, vvv)
				return reply.Value
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mtx.Lock()
	defer ck.mtx.Unlock()

	seqNo := ck.newSeqNo()
	// log.Printf("clerk::PutAppend op=%s, key=%s, value=%s, client=%v, #seq=%d", op, key, value, ck.clientId, seqNo)

	for {
		serverIds := ck.makeServerOrder()
		for _, serverId := range serverIds {
			args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqNo: seqNo}
			reply := PutAppendReply{}

			// TODO: handle timeout
			ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

			if !ok || reply.Err != OK {
				continue
			}

			if reply.Err == OK {
				ck.curLeader = serverId
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
