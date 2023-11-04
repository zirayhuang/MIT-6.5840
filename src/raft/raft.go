package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"time"

	"sync"
	"sync/atomic"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var Nnode int

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// ApplyMsg as each Raft peer becomes aware that successive log Entries are
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

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	resetCh   chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	state       State
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile states
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// Volatile state on Candidate
	votesResponded []int
	votesGranted   []int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatalf("Node %v: Failed to read persistent state \n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		fmt.Printf("Node %v: Read persist state, ct = %v, votedFor = %v, logs = %v\n", rf.me, currentTerm, votedFor, logs)
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("Node %v : Receive vote request from Node %v, c.term = %v, myterm = %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		//fmt.Printf("Node %v: reject vote request because %v < my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			//fmt.Printf("Node %v : Have been voted for this Term %v, Not grant vote \n", rf.me, args.Term)
			return
		}
	}

	rf.currentTerm = args.Term
	rf.changeState(Follower)
	rf.persist()

	// 5.4
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}

	// First, compare the term of the last log entry
	if lastLogTerm > args.LastLogTerm {
		fmt.Printf("Node %v: reject vote from %v because args.lastLogTerm = %v and my lastLogTerm = %v \n",
			rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm)
		return
	}

	// If the terms are the same, then compare the length of the log
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		fmt.Printf("Node %v: reject vote from %v because args.lastLogIndex = %v and my lastLogIndex = %v \n",
			rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
		return
	}

	// if args.Term > rf.currentTerm || hasn't vote
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
	rf.resetTimeout()
	//fmt.Printf("Node %v: Grant vote for Node %v, ReTerm = %v, MyTerm = %v \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	//fmt.Printf("Node %v: while granting, my lastLogTerm = %v, lastLogIndex = %v\n", rf.me, lastLogTerm, lastLogIndex)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		fmt.Printf("Node %v: Reject received AppendEntries from node %v with Term %v, my term = %v \n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	rf.resetTimeout()

	if rf.state == Candidate {
		fmt.Printf("Node %v: candidate become follower because of receive vote reqeust from %v", rf.me, args.LeaderId)
		rf.changeState(Follower)
		rf.votedFor = -1
	}

	if rf.state == Leader && args.Entries == nil {
		fmt.Printf("Node %v: leader receive empty heartbeat reqeust from, wait for update %v", rf.me, args.LeaderId)
		return
	}

	// check if PrevLogIndex matches
	adjustedIndex := args.PrevLogIndex - 1
	if adjustedIndex >= 0 && (adjustedIndex >= len(rf.logs) || rf.logs[adjustedIndex].Term != args.PrevLogTerm) {
		// Optimization
		if adjustedIndex >= len(rf.logs) {
			//fmt.Printf("Node %v: Reject received AppendEntries from node %v with Term %v, No PrevIndex = %v, conflictIndec = %v \n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(rf.logs))
			reply.ConflictIndex = len(rf.logs)
			return
		}
		if rf.logs[adjustedIndex].Term != args.PrevLogTerm {
			//fmt.Printf("Node %v: Reject received AppendEntries from node %v with Term %v, PrevLogTerm not match = %v:%v \n", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, rf.logs[adjustedIndex].Term)
			reply.ConflictTerm = rf.logs[adjustedIndex].Term
			for _, entry := range rf.logs {
				if entry.Term == reply.ConflictTerm {
					reply.ConflictIndex = entry.Index
					//fmt.Printf("Node %v: Reject received AppendEntries, conflict index = %v\n", rf.me, reply.ConflictIndex)
					return
				}
			}
		}
		return
	}

	//fmt.Printf("Node %v: received AE from leader %v, with entry %v\n", rf.me, args.LeaderId, args.Entries)
	// truncate log
	if args.Entries != nil {
		// Detect conflict starting from prevLogIndex+1 since f.Log is 0-indexed
		conflictIndex := -1
		for _, entry := range args.Entries {
			if entry.Index <= len(rf.logs) && rf.logs[entry.Index-1].Term != entry.Term {
				conflictIndex = entry.Index
				break
			}
		}

		// If a conflict is found, delete the conflicting entry and all that follow it
		if conflictIndex != -1 {
			rf.logs = rf.logs[:conflictIndex-1]
			//fmt.Printf("Node %v: conflict index at %v, after cutting %v \n", rf.me, conflictIndex, rf.logs)
		}
	}

	for i, entry := range args.Entries {
		if entry.Index > len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			//fmt.Printf("Node %v: append %v entries to %v\n", rf.me, len(args.Entries)-i, rf.logs)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("Node %v: args.LC = %v, myC =%v myLastEntry = %v \n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.logs))
		lastCommit := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
		//fmt.Printf("Node %v: update commitIndex to %v from lastCommit %v \n", rf.me, rf.commitIndex, lastCommit)
		for i := lastCommit; i < rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{Command: rf.logs[i].Command, CommandIndex: i + 1, CommandValid: true}
			//fmt.Printf("Node %v: %v \n", rf.me, rf.logs)
			//fmt.Printf("Node %v: Applied Message: %v\n", rf.me, i+1)
		}
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.changeState(Follower)
	rf.persist()
	return
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
// within a getTimeout interval, Call() returns true; otherwise
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, 0, false
	}

	term := rf.currentTerm
	index := len(rf.logs) + 1

	rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
	rf.persist()
	rf.mu.Unlock()

	//fmt.Printf("Node %v: Leader received Start Command, start update followers entry, logs = %v \n", rf.me, rf.logs)
	rf.updateFollowerEntries()
	return index, term, rf.state == Leader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 250 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case <-rf.resetCh: // Reset the timeout if a Command is received on the reset channel
			//fmt.Printf("[%v] Node %v : Received heartbeat, ticker pass\n", time.Now(), rf.me)
		default:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader == false {
				//fmt.Printf("[%v] Node %v : No request being received, start election\n", time.Now(), rf.me)
				go rf.startElection()
			}
		}
	}
}

// Make the service or tester wants to create a Raft server. the ports
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
	Nnode = len(peers)
	rf := &Raft{currentTerm: 0, votedFor: -1, commitIndex: 0, lastApplied: 0}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.resetCh = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	go rf.ticker()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Printf("Node %v : Start Election with term %v \n", rf.me, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	voteCount := 1
	voteTerm := rf.currentTerm
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogIndex = len(rf.logs)
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	rf.mu.Unlock()

	voteChannel := make(chan RequestVoteReply, Nnode-1)
	for server, _ := range rf.peers {
		if server != rf.me {
			go rf.elect(server, voteTerm, lastLogIndex, lastLogTerm, voteChannel)
		}
	}

	for i := 0; i < Nnode-1; i++ {
		reply := <-voteChannel
		//fmt.Printf("Node %v : Received vote reply from Node \n", rf.me)
		rf.mu.Lock()
		if reply.Term > voteTerm {
			//fmt.Printf("Node %v: Vote Fail, because reply.Term > currentTerm", rf.me)
			rf.currentTerm = reply.Term
			rf.changeState(Follower)
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted == true {
			voteCount += 1
		}
		rf.mu.Unlock()
		if voteCount > Nnode/2 {
			break
		}
	}
	//fmt.Printf("Node %v : All vote request replies received \n", rf.me)

	rf.mu.Lock()
	//fmt.Printf("Node %v : Received %v votes \n", rf.me, voteCount)
	if voteCount > Nnode/2 && rf.state == Candidate && rf.currentTerm == voteTerm {
		fmt.Printf("Node %v : becoming leader, new term = %v \n", rf.me, rf.currentTerm)
		rf.changeState(Leader)
		lastIndex := len(rf.logs)
		fmt.Printf("Node %v: while promote to leader, last index = %v, committed index = %v \n", rf.me, lastIndex, rf.commitIndex)
		for id, _ := range rf.peers {
			rf.nextIndex[id] = lastIndex + 1
			rf.matchIndex[id] = 0
		}
		rf.mu.Unlock()
		go rf.leaderHeartbeatLoop()
	} else {
		fmt.Printf("Node %v: Didn't get enough votes - %v, election failed. State = %v \n", rf.me, voteCount, rf.state)
		rf.votedFor = -1
		rf.persist()
		rf.changeState(Follower)
		rf.mu.Unlock()
	}

}

func (rf *Raft) elect(server int, voteTerm int, lastLogIndex int, lastLogTerm int, ch chan RequestVoteReply) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         voteTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok == false {
		fmt.Printf("Node %v, Failed to invoke send Request Vote RPC call to %v \n", rf.me, server)
	}
	ch <- reply
}

func (rf *Raft) leaderHeartbeatLoop() {
	for rf.killed() == false {
		//fmt.Printf("Node %v: Start new round heart beat\n", rf.me)
		rf.mu.Lock()
		if rf.state != Leader {
			//fmt.Printf("Node %v - No longer Leader, stop heartbeat \n", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.updateFollowerEntries()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) changeState(state State) {
	rf.state = state
}

func (rf *Raft) resetTimeout() {
	select {
	case rf.resetCh <- struct{}{}:
		//fmt.Printf("[%v] Node %v: send to resetCh \n", time.Now(), rf.me)
		// Send a Command to reset the timeout
	default: // Non-blocking. If nobody is listening or the channel is full, continue.
	}
	//fmt.Printf("Node %v: reset timer \n", rf.me)
}

func (rf *Raft) updateFollowerEntries() {
	for server, _ := range rf.peers {
		if server != rf.me {
			go rf.updateEntry(server)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		fmt.Printf("Node %v - No longer Leader, stop update commit index \n", rf.me)
		return
	}

	N := len(rf.logs)
	committedTill := rf.commitIndex
	for n := rf.commitIndex + 1; n <= N; n++ {
		count := 1 // Start with 1 to count the leader itself
		for _, matchIdx := range rf.matchIndex {
			if matchIdx >= n {
				count++
			}
		}

		// Check if a majority has replicated this log entry
		// and the entry is from the leader's current Term
		if count > len(rf.peers)/2 && rf.logs[n-1].Term == rf.currentTerm {
			committedTill = n
		} else {
			//fmt.Printf("Node %v: Failed to update committed index for %v; "+
			//	"count = %v, logTerm = %v, currentTerm = %v \n", rf.me, n, count, rf.logs[n-1].Term, rf.currentTerm)
		}
	}

	if committedTill > rf.commitIndex {
		lastCommitted := rf.commitIndex
		rf.commitIndex = committedTill
		fmt.Printf("Node %v: Leader Update committed log to %v \n", rf.me, committedTill)
		for i := lastCommitted + 1; i <= committedTill; i++ {
			rf.applyCh <- ApplyMsg{Command: rf.logs[i-1].Command, CommandIndex: i, CommandValid: true}
			//fmt.Printf("Node %v: %v \n", rf.me, rf.logs)
			//fmt.Printf("Node %v: Applied Message: %v \n", rf.me, i)
		}
	}

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) updateEntry(peer int) {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := 0
		var entries []LogEntry
		// check if there's anything to send.
		if prevLogIndex > 0 {
			prevLogTerm = rf.logs[prevLogIndex-1].Term
		}
		if prevLogIndex >= 0 {
			entries = rf.logs[prevLogIndex:]
		}
		args := AppendEntriesArgs{
			Entries:      entries,
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex}

		//fmt.Printf("Node %v: AE RPC Check point prevLogIndex = %v, "+
		//	"prevLogTerm = %v of Follower %v, current log length = %v, to send = %v \n", rf.me, prevLogIndex, prevLogTerm, peer, len(rf.logs), args.Entries)

		nextIndex := rf.nextIndex[peer]
		matchIndex := rf.matchIndex[peer]
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok == false {
			fmt.Printf("Node %v: Failed to invoke Append Entries RPC call to %v\n", rf.me, peer)
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			fmt.Printf("Node %v: AE RPC to Node %v Failed because %v > %v, step down...\n", rf.me, peer, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.changeState(Follower)
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// Every time a follower successfully appends entries sent by the leader (indicated in the followers' responses to AppendEntries RPCs),
		// the leader updates the corresponding matchIndex for that follower.
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Success == true {
			if len(args.Entries) > 0 {
				if rf.matchIndex[peer] == matchIndex && rf.nextIndex[peer] == nextIndex {
					rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[peer] = max(rf.nextIndex[peer], args.Entries[len(args.Entries)-1].Index+1)
					//fmt.Printf("Node %v: update matchIndex of follower %v, to %v, nextIndex to %v \n", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
				}
			}
			rf.updateCommitIndex()
			rf.mu.Unlock()
			return
		} else {
			fmt.Printf("Node %v: AE RPC to follower %v failed, decrement nextIndex to %v \n", rf.me, peer, rf.nextIndex[peer]-1)
			if rf.nextIndex[peer] == nextIndex {
				//rf.nextIndex[peer] = rf.nextIndex[peer] - 1
				// Optimization

				if reply.ConflictTerm == 0 {
					rf.nextIndex[peer] = 1
					fmt.Printf("Node %v: AE RPC to follower %v failed, decrement nextIndex to %v \n", rf.me, peer, rf.nextIndex[peer])
				} else {
					findConflictTerm := false
					for _, entry := range rf.logs {
						if entry.Term == reply.ConflictTerm {
							findConflictTerm = true
							break
						}
					}

					if findConflictTerm == true {
						for _, entry := range rf.logs {
							if entry.Term > reply.ConflictTerm {
								rf.nextIndex[peer] = entry.Index
								fmt.Printf("Node %v: AE RPC to follower %v failed, decrement nextIndex to %v \n", rf.me, peer, rf.nextIndex[peer])
								break
							}
						}
						fmt.Printf("Node %v: reaching here: No!\n", rf.me)
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
						fmt.Printf("Node %v: AE RPC to follower %v failed, decrement nextIndex to %v \n", rf.me, peer, rf.nextIndex[peer])
					}
				}
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			continue
		}
	}
}
