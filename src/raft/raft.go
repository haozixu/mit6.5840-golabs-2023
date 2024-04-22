package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverRole int32

const (
	rFollower  serverRole = 0
	rLeader    serverRole = 1
	rCandidate serverRole = 2
)

func (r serverRole) String() string {
	switch r {
	case rFollower:
		return "follower"
	case rLeader:
		return "leader"
	case rCandidate:
		return "candidate"
	default:
		return "unknown"
	}
}

// log entry structure
type logEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state

	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term (or -1 of none)
	logs        []logEntry

	// volatile state on all servers

	// TODO: correct?
	role          serverRole
	lastHeartbeat time.Time
	heartbeatLock sync.Mutex

	// volatile state on leaders
}

// used for debugging
func (rf *Raft) dbg(topic logTopic, format string, a ...interface{}) {
	prefix := fmt.Sprintf("S%d ", rf.me)
	DPrint(topic, prefix+format, a...)
}

func (rf *Raft) getRole() serverRole {
	return serverRole(atomic.LoadInt32((*int32)(&rf.role)))
}

func (rf *Raft) setRole(newRole serverRole) {
	atomic.StoreInt32((*int32)(&rf.role), int32(newRole))
}

func (rf *Raft) getLastHeartbeat() time.Time {
	rf.heartbeatLock.Lock()
	defer rf.heartbeatLock.Unlock()
	return rf.lastHeartbeat
}

func (rf *Raft) updateHeartbeat() {
	rf.heartbeatLock.Lock()
	rf.lastHeartbeat = time.Now()
	rf.heartbeatLock.Unlock()
}

// no lock operations
func (rf *Raft) enterNewTerm(newTerm int, role serverRole) {
	rf.dbg(dTerm, "enter new term %d as %s", newTerm, role.String())
	rf.currentTerm = newTerm
	rf.votedFor = -1 // not yet voted in this new term

	rf.setRole(role)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	// TODO: reconsider this design where currentTerm needs mutual access
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := (rf.getRole() == rLeader)
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currectTerm, for candidate to update itself
	VoteGranted bool // whether candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject requests from previous terms
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// update currentTerm if term of the incoming request is larger, and become a follower
	if rf.currentTerm < args.Term {
		rf.enterNewTerm(args.Term, rFollower)
	}

	voteGranted := false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// TODO: check candidate’s log is at least as up-to-date as receiver’s log
		rf.votedFor = args.CandidateId
		voteGranted = true

		// NOTE: is this correct?
		// "If election timeout elapses without receiving
		//  AppendEntries RPC from current leader or granting vote to candidate"
		rf.updateHeartbeat()
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	rf.dbg(dVote, "vote %v to S%d", voteGranted, args.CandidateId)
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int

	// TODO: complete this structure
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject requests from previous terms
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		reply.Success = false
		return
	}

	// update currentTerm if term of the incoming request is larger, and become a follower
	if rf.currentTerm < args.Term {
		rf.enterNewTerm(args.Term, rFollower)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.updateHeartbeat()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	t1 := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	elapsed := time.Since(t1)

	rf.dbg(dWarn, "sendRequestVote ret: %v target: %d elapsed: %d ms", ok, server, elapsed.Milliseconds())
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() bool {
	// turns into a candidate
	rf.mu.Lock()
	rf.enterNewTerm(rf.currentTerm+1, rCandidate)
	rf.votedFor = rf.me
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	// reset election timer
	rf.updateHeartbeat()

	votes := 1
	replyCh := make(chan *RequestVoteReply)

	rpcTimeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{Term: curTerm, CandidateId: rf.me}
		reply := RequestVoteReply{}
		go func(peer int) {
			if ok := rf.sendRequestVote(peer, &args, &reply); ok {
				replyCh <- &reply
			}
		}(i)
	}

recvLoop:
	for {
		select {
		case reply := <-replyCh:
			if !reply.VoteGranted && curTerm < reply.Term {
				rf.mu.Lock()
				rf.enterNewTerm(reply.Term, rFollower)
				rf.mu.Unlock()
				return false
			}

			if reply.VoteGranted {
				votes++
				if votes == len(rf.peers) {
					break recvLoop
				}
			}
		case <-ctx.Done():
			break recvLoop
		}
	}

	rf.dbg(dInfo, "got %d votes", votes)
	return votes > len(rf.peers)/2
}

func (rf *Raft) sendHeartbeats() {
	curTerm, isLeader := rf.GetState()
	if !isLeader {
		return
	}

	replyCh := make(chan *AppendEntriesReply)

	rpcTimeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{Term: curTerm, LeaderId: rf.me}
		reply := AppendEntriesReply{}
		go func(peer int) {
			if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
				replyCh <- &reply
			}
		}(i)
	}

	nRecv := 0
	for {
		select {
		case reply := <-replyCh:
			if !reply.Success && curTerm < reply.Term {
				rf.mu.Lock()
				rf.enterNewTerm(reply.Term, rFollower)
				rf.mu.Unlock()
				return
			}

			nRecv++
			if nRecv == len(rf.peers)-1 {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) electionTicker() {
	electionTimeout := 400 * time.Millisecond

	rf.updateHeartbeat()
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// condition: election timeout & is follower or candidate
		if rf.getRole() != rLeader && time.Since(rf.getLastHeartbeat()) > electionTimeout {
			rf.dbg(dTimer, "election timeout, start election")

			if ok := rf.startElection(); ok {
				rf.setRole(rLeader)
				rf.dbg(dLeader, "is now leader!")

				// send heartbeats immediately
				rf.sendHeartbeats()
			} else {
				rf.dbg(dInfo, "election failed, still %s", rf.getRole().String())
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeatTicker() {
	heartbeatInterval := 125 * time.Millisecond

	for !rf.killed() {
		if rf.getRole() == rLeader {
			rf.sendHeartbeats()
		}

		time.Sleep(heartbeatInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.enterNewTerm(0, rFollower)
	rf.logs = make([]logEntry, 0)

	InitDebug()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start heartbeat ticker
	go rf.heartbeatTicker()

	return rf
}
