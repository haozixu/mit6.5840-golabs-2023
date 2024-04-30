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
type LogEntry struct {
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
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state

	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term (or -1 if none)
	logs        []LogEntry

	// volatile state on all servers
	// NOTE: all log indices start from 1 to reduce boundary checks

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// NOTE: these variables use independent locks, which may be unnecessary
	role          serverRole
	lastHeartbeat time.Time
	heartbeatLock sync.Mutex

	// volatile state on leaders (reinitialized after election)

	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
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

// NOTE: no lock operations
func (rf *Raft) commitNewLogs() {
	for rf.lastApplied < rf.commitIndex {
		i := rf.lastApplied + 1
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i

		rf.dbg(dCommit, "commit index %d", i)
	}
}

func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs) // leader's last log index + 1
		rf.matchIndex[i] = 0           // no known log entry replicated
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

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
		// check whether candidate’s log is at least as up-to-date as receiver’s log
		selfLastLogIndex := len(rf.logs) - 1
		selfLastLogTerm := rf.logs[selfLastLogIndex].Term
		candidateLogUpToDate := false

		if args.LastLogTerm > selfLastLogTerm {
			candidateLogUpToDate = true
		} else if (args.LastLogTerm == selfLastLogTerm) && (args.LastLogIndex >= selfLastLogIndex) {
			candidateLogUpToDate = true
		}

		if candidateLogUpToDate {
			rf.votedFor = args.CandidateId
			voteGranted = true

			// "If election timeout elapses without receiving
			//  AppendEntries RPC from current leader or granting vote to candidate"
			rf.updateHeartbeat()
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	rf.dbg(dVote, "vote %v to S%d", voteGranted, args.CandidateId)
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	LeaderCommit int // leader's commitIndex
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry
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
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// AppendEntries RPC received from current leader
	rf.updateHeartbeat()

	// update currentTerm if term of the incoming request is larger, and become a follower
	if rf.currentTerm < args.Term {
		rf.enterNewTerm(args.Term, rFollower)
	}
	reply.Term = rf.currentTerm

	// reply false if previous logs do not match
	if !(args.PrevLogIndex < len(rf.logs)) ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// discard conflicting entries
	selfLogIndex := args.PrevLogIndex + 1
	recvLogIndex := 0
	hasConflict := false
	for selfLogIndex < len(rf.logs) && recvLogIndex < len(args.Entries) {
		if rf.logs[selfLogIndex].Term != args.Entries[recvLogIndex].Term {
			hasConflict = true
			break
		}

		selfLogIndex++
		recvLogIndex++
	}
	if hasConflict {
		rf.logs = rf.logs[:selfLogIndex]
	}

	// append new entries not already in the log
	if len(rf.logs)-selfLogIndex >= len(args.Entries)-recvLogIndex &&
		len(args.Entries)-recvLogIndex > 0 {
		panic("???") // assert this should not happen
	}
	newEntries := args.Entries[recvLogIndex:]
	rf.logs = append(rf.logs, newEntries...)

	// stupid min/max. go 1.21 provides them
	min := func(a int, b int) int {
		if a < b {
			return a
		}
		return b
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(newEntries))
		rf.commitNewLogs()
	}
	reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	isLeader := false

	// Your code here (2B).

	// TODO: how to correctly handle killed instance?
	if rf.killed() {
		return index, term, isLeader
	}

	isLeader = (rf.getRole() == rLeader)
	// not the leader, returns directly
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.logs)
	term = rf.currentTerm

	// append entry to local logs
	rf.logs = append(rf.logs, LogEntry{command, term})
	go rf.replicateLogs()

	return index, term, isLeader
}

func (rf *Raft) replicateLogs() {
	rf.broadcastAppendEntries(false)
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	type Bundle struct {
		peer  int
		args  *AppendEntriesArgs
		reply *AppendEntriesReply
	}
	respCh := make(chan Bundle)

	// requires to be invoked when lock is obtained
	makeRpcBundle := func(peer int, nextIndex int, isHeartbeat bool) Bundle {
		prevLogIndex := nextIndex - 1
		entries := []LogEntry{}
		if !isHeartbeat {
			entries = rf.logs[nextIndex:]
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.logs[prevLogIndex].Term,
			Entries:      entries,
		}
		reply := AppendEntriesReply{}
		return Bundle{peer, &args, &reply}
	}

	doRpc := func(b Bundle) {
		if ok := rf.sendAppendEntries(b.peer, b.args, b.reply); ok {
			respCh <- b
		}
	}

	nTargets := 0
	rf.mu.Lock() // enter critical section
	// send AppendEntries RPC concurrently
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// no need to send AppendEntries RPC if nextIndex[i] indicate
		// that logs on peer i is already update to date
		if !isHeartbeat && rf.nextIndex[i] >= len(rf.logs) {
			continue
		}

		nTargets++
		b := makeRpcBundle(i, rf.nextIndex[i], isHeartbeat)
		go doRpc(b)
	}
	rf.mu.Unlock() // leave critical section

	rpcTimeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	nRecv := 0
recvLoop:
	for {
		select {
		case b := <-respCh:
			if rf.getRole() != rLeader {
				return
			}

			rf.mu.Lock() // enter critical section
			if b.args.Term < rf.currentTerm {
				panic("outdated RPC")
			}

			if rf.currentTerm < b.reply.Term {
				// step down immediately
				rf.enterNewTerm(b.reply.Term, rFollower)
				rf.mu.Unlock() // leave critical section
				return
			}

			// failed due to log inconsistency
			i := b.peer
			if !b.reply.Success {
				// skip logs that have the same term with the rejected entry
				prevIndex := b.args.PrevLogIndex
				for prevIndex > 0 && rf.logs[prevIndex].Term == b.args.PrevLogTerm {
					prevIndex--
				}
				rf.nextIndex[i] = prevIndex + 1

				b := makeRpcBundle(i, rf.nextIndex[i], isHeartbeat)
				go doRpc(b)
				rf.mu.Unlock() // leave critical section
				continue recvLoop
			}

			// NOTE: rf.logs may be already updated, use info from args instead
			// NOTE: and replies may arrive out of order
			newMatchIndex := b.args.PrevLogIndex + len(b.args.Entries)
			if rf.matchIndex[i] < newMatchIndex {
				rf.matchIndex[i] = newMatchIndex
				rf.nextIndex[i] = newMatchIndex + 1
			}
			rf.mu.Unlock() // leave critical section

			nRecv++
			if nRecv >= nTargets {
				break recvLoop
			}
		case <-ctx.Done():
			break recvLoop
		}
	}

	rf.leaderTryCommit()
}

func (rf *Raft) leaderTryCommit() {
	if rf.getRole() != rLeader { // guard
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if we can commit new logs
	newIndex := len(rf.logs) - 1
	canCommit := false
	rf.matchIndex[rf.me] = newIndex
	for newIndex > rf.commitIndex {
		nPeers := 0
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= newIndex {
				nPeers++
			}
		}

		if nPeers > len(rf.peers)/2 && rf.logs[newIndex].Term == rf.currentTerm {
			canCommit = true
			break
		}
		newIndex--
	}
	if canCommit {
		rf.commitIndex = newIndex
		rf.commitNewLogs()
	}
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
	replyCh := make(chan *RequestVoteReply)

	rf.mu.Lock() // enter critical section
	// turns into a candidate
	rf.enterNewTerm(rf.currentTerm+1, rCandidate)
	rf.votedFor = rf.me

	curTerm := rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term

	// reset election timer
	rf.updateHeartbeat()

	// send RequestVote RPC to peers concurrently
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         curTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}
		go func(peer int) {
			if ok := rf.sendRequestVote(peer, &args, &reply); ok {
				replyCh <- &reply
			}
		}(i)
	}
	rf.mu.Unlock() // leave critical section

	votes := 1
	nRecv := 0

	rpcTimeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

recvLoop:
	for {
		select {
		case reply := <-replyCh:
			if curTerm < reply.Term {
				rf.mu.Lock()
				rf.enterNewTerm(reply.Term, rFollower)
				rf.mu.Unlock()
				return false
			}

			if reply.VoteGranted {
				votes++
			}
			nRecv++
			if nRecv == len(rf.peers)-1 {
				break recvLoop
			}
		case <-ctx.Done():
			break recvLoop
		}

		// if current role is no longer candidate, exit
		if rf.getRole() != rCandidate {
			return false
		}
	}

	rf.dbg(dInfo, "got %d votes", votes)
	return votes > len(rf.peers)/2
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

			if win := rf.startElection(); win {
				rf.setRole(rLeader)
				rf.dbg(dLeader, "is now leader!")

				rf.initLeaderState()

				// send heartbeats immediately
				rf.broadcastAppendEntries(true)
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
		time.Sleep(heartbeatInterval)

		if rf.getRole() == rLeader {
			rf.broadcastAppendEntries(true)
		}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	InitDebug()

	if me == 0 {
		// make peer 0 the initial leader
		rf.enterNewTerm(1, rLeader)
		rf.votedFor = me
	} else {
		rf.enterNewTerm(1, rFollower)
	}

	// add a dummy entry in the front of logs
	rf.logs = []LogEntry{{Command: nil, Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.initLeaderState()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start heartbeat ticker
	go rf.heartbeatTicker()

	return rf
}
