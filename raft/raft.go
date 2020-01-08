package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"math/rand"
	"time"
	"log"
	"sort"
	"bytes"
	"labgob"
)


const (
    STATE_FOLLOWER = iota + 1
    STATE_CANDIDATE
    STATE_LEADER

	HEARTBEAT_CYCLE = time.Duration(150) * time.Millisecond
	VOTED_FOR_NONE = -1
)



// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ReplicateLog struct {
	Command      interface{}
	Term	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state	int
	currentTerm		int
	votedFor	int
	logs	[]ReplicateLog

	commitIndex		int
	lastApplied		int
	applyMsgCh chan ApplyMsg
	applyMu	 sync.RWMutex

	nextIndex	[]int
	matchIndex	[]int
	indexMus    []sync.RWMutex          // Lock to protect nextIndex matchIndex 

	newTermCh chan struct{}
	appendEntryCh chan struct{}
	lastHbTime time.Time

	working bool
	id int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	e.Encode(rf.lastHbTime.UnixNano())
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	log.Printf("[persist] {%v} persist finish\n", rf.me)
}

//
// restore previously persisted state.
//
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state int 
	var term int
	var commitIndex int
	var votedFor int
	var logs []ReplicateLog
	var lastApplied int
	var nextIndex []int
	var matchIndex []int
	var lastHbTime int64
	if d.Decode(&state) != nil ||
	   d.Decode(&term) != nil ||
	   d.Decode(&commitIndex) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil ||
	   d.Decode(&lastApplied) != nil ||
	   d.Decode(&nextIndex) != nil ||
	   d.Decode(&matchIndex) != nil ||
	   d.Decode(&lastHbTime) != nil {
		log.Printf("[readPersist]%v persist decode error\n", rf.me)
	} else {
		//rf.state = state
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.logs = logs
		rf.lastApplied = lastApplied
		rf.nextIndex = nextIndex
		rf.matchIndex = matchIndex
		//rf.lastHbTime = time.Unix(0, lastHbTime)
		log.Printf("[readPersist]{%v} logs={%v}\n", rf.me, rf.logs)
		log.Println(state, term, votedFor, lastHbTime)
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.handleRpcTerm(args.Term)
	voted := false 
	rf.mu.Lock()
	//log.Printf("[RequestVote]%v recive RequestVote args={%v}, rf.currentTerm={%v}", rf.me, args, rf.currentTerm)
	defer func() {
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = voted
		//log.Printf("[RequestVote]%v response RequestVote args={%v}, rf.currentTerm={%v}", rf.me, args, rf.currentTerm)
	}()

	if args.Term < rf.currentTerm {
		return
	}
	if rf.commitIndex >= len(rf.logs) {
		log.Printf("[newRequestVoteArgs]%v rf.commitIndex={%v} rf.logs={%v}", rf.me, rf.commitIndex, rf.logs)
	}
	lastLogIndex := len(rf.logs) - 1
	if (rf.votedFor == VOTED_FOR_NONE || rf.votedFor == args.CandidateId) &&
		//(args.LastLogIndex > rf.commitIndex || args.LastLogIndex == rf.commitIndex && args.LastLogTerm >= rf.logs[rf.commitIndex].Term) {
		(args.LastLogTerm > rf.logs[lastLogIndex].Term || args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
		rf.votedFor = args.CandidateId
		rf.persist()
		voted = true
	}
}

func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex := rf.commitIndex
	if rf.commitIndex >= len(rf.logs) {
		log.Printf("[newRequestVoteArgs]%v rf.commitIndex={%v} rf.logs={%v}", rf.me, rf.commitIndex, rf.logs)
	}
	lastLogIndex = len(rf.logs) - 1
	return &RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: rf.logs[lastLogIndex].Term,
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//log.Printf("[sendRequestVote]%v call Raft.RequestVote success args={%v}, reply={%v}", rf.me, args, reply)
	rf.handleRpcTerm(reply.Term)
	return ok
}

type RequestAppendEntriesArgs struct {
	Term	int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries		[]ReplicateLog
	LeaderCommit	int
}

type RequestAppendEntriesReply struct {
	Term	int
	Success	bool
}

func (rf *Raft) newRequestAppendEntriesArgs(server int, entries []ReplicateLog) *RequestAppendEntriesArgs{
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if rf.nextIndex[server] >= len(rf.logs) {
		rf.nextIndex[server] = len(rf.logs) - 1
	}
	if prevLogIndex > 0 {
		prevLogTerm = rf.logs[prevLogIndex].Term
	}
	return &RequestAppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) sendAppendEntries (server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.handleRpcTerm(reply.Term)
	return ok
}

func (rf *Raft) AppendEntries (args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	//log.Printf("[AppendEntries]%v recive AppendEntries args={%v}", rf.me, args)
	//defer log.Printf("[AppendEntries]%v finish AppendEntries args={%v}", rf.me, args)
	success := true
	rf.handleRpcTerm(args.Term)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		// reply
		reply.Term = rf.currentTerm
		reply.Success = success
	}()
	if len(args.Entries) > 0 {
		log.Printf("[AppendEntries]%v recive AppendEntries args={%v}, rf.currentTerm={%v}", rf.me, args, rf.currentTerm)
	}
	if args.Term < rf.currentTerm {
		success = false
	} else {
		go func() {rf.appendEntryCh <- struct{}{}}()
		if args.PrevLogIndex >= 0 && (args.PrevLogIndex >= len(rf.logs) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) {
			//prevLogIndex and prevLogTerm not match
			success = false
		} else {
			for i, replicateLog	:= range args.Entries {
				idx := args.PrevLogIndex + 1 + i
				if (idx < len(rf.logs) && rf.logs[idx].Term != replicateLog.Term) {
					// delete log to end
					rf.logs = rf.logs[:idx]
					success = false
					break
				}
				if idx >= len(rf.logs) {
					rf.logs = append(rf.logs, replicateLog)
				}
			}
			rf.persist()
		}
	}
	if success && args.LeaderCommit > rf.commitIndex {
		// learn commit from leader
		lastCommitIndex := rf.commitIndex
		newLogIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > newLogIndex {
			rf.commitIndex = newLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if lastCommitIndex != rf.commitIndex {
			log.Printf("[AppendEntries]%v update commit last={%v} now={%v} log={%v} args={%v}", rf.me, lastCommitIndex, rf.commitIndex, rf.logs, args)
		}
		go rf.applyLog()
	}
}

func (rf *Raft) applyLog () {
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()
	from := rf.lastApplied+1
	end := rf.commitIndex
	if from > end {
		return
	}
	log.Printf("[applyLog]%v applyLog from={%v} end={%v} log={%v}", rf.me, from, end, rf.logs[from:end+1])
	rf.persist()
	for i:=from; i<=end; i++ {
		//log.Printf("[applyLog]%v start index={%v} msg={%v}", rf.me, i, applyMsg)
		applyMsg := ApplyMsg{true, rf.logs[i].Command, i}
		rf.applyMsgCh <- applyMsg
		//log.Printf("[applyLog]%v success index={%v} msg={%v}", rf.me, i, applyMsg)
	}
	rf.lastApplied = end
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER

	// Your code here (2B).
	if !isLeader {
		return -1, term, isLeader 
	}
	index := len(rf.logs)
	log.Printf("[Start]%v recive command={%v}", rf.me, command)
	rf.logs = append(rf.logs, ReplicateLog{command, rf.currentTerm})
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.nextIndex[rf.me] = len(rf.logs)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Printf("[Kill]rf.me={%v}, rf.commitIndex={%v}, rf.logs={%v}, rf.nextIndex={%v}\n", rf.me, rf.commitIndex, rf.logs, rf.nextIndex)
	rf.state = -1
	rf.newTermCh <- struct{}{}
}

func (rf *Raft) handleRpcTerm(rpcTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rpcTerm > rf.currentTerm {
		log.Printf("[handleRpcTerm] rf.me={%v} rf.currentTerm={%v} rpcTerm={%v}", rf.me, rf.currentTerm, rpcTerm)
		rf.working = false
		rf.currentTerm = rpcTerm
		rf.state = STATE_FOLLOWER
		rf.votedFor = VOTED_FOR_NONE
		rf.persist()
		rf.newTermCh <- struct{}{}
		for{
			if rf.working {
				break
			}
			time.Sleep(time.Millisecond*20)
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := rf.newRequestAppendEntriesArgs(i, []ReplicateLog{})
		go func(server int, args *RequestAppendEntriesArgs) {
			// send heartbeat
			reply := &RequestAppendEntriesReply {}
			rf.sendAppendEntries(server, args, reply)
		}(i, args)
	}
}

func (rf *Raft) sendNextLog() {
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		currentLastIdx := len(rf.logs) -1
		idx := rf.nextIndex[i]
		//log.Printf("[sendNextLog]%v send log to %v, rf.nextIndex={%v} currentLastIdx={%v}", rf.me, i, rf.nextIndex, currentLastIdx)
		if idx > currentLastIdx {
			continue
		}
		args := rf.newRequestAppendEntriesArgs(i, rf.logs[idx:currentLastIdx+1])
		//log.Printf("[sendNextLog][before lock]%v send %v to %v args={%v}", rf.me, args.Entries, i, args)
		go func (server int, args *RequestAppendEntriesArgs) {
			reply := &RequestAppendEntriesReply {}
			ok := rf.sendAppendEntries(server, args, reply)
			log.Printf("[sendNextLog]%v send %v to %v, res={%v} reply={%v} args={%v}", rf.me, args.Entries, server, ok, reply, args)
			rf.indexMus[server].Lock()
			defer rf.indexMus[server].Unlock()
			if ok {
				if reply.Success {
					rf.nextIndex[server] = currentLastIdx + 1
					rf.matchIndex[server] = currentLastIdx
					//log.Println(rf.matchIndex)
				} else if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
			}
		}(i, args)
	}
}

func (rf *Raft) workAsLeader() {
	log.Printf("%v becomme a leader, term={%v} p={%v} logs={%v}\n", rf.me, rf.currentTerm, rf.id, rf.logs)
	rf.working = true
	rf.sendHeartbeat()
	// heartbeat rand timeout 150 millisecond
	heartbeatDuration := 100 * time.Millisecond
	heartbeatTicker := time.NewTicker(heartbeatDuration)
loop:
	for {
		select {
		case <-rf.newTermCh:
			// discover new term 
			// change state 2 follower
			//rf.sendHeartbeat()
			//rf.votedFor = VOTED_FOR_NONE
			break loop
		case <-heartbeatTicker.C:
			//log.Println(t)
			rf.mu.Lock()
			rf.sendHeartbeat()
			rf.sendNextLog()
			rf.mu.Unlock()
		default:
		}
		idxs := make([]int, len(rf.peers))
		for i, idx := range rf.matchIndex {
			idxs[i] = idx
		}
		sort.Ints(idxs)
		majorIndex := idxs[len(idxs)/2]
		if (majorIndex > rf.commitIndex && rf.logs[majorIndex].Term == rf.currentTerm) {
			log.Printf("[workAsLeader]%v get major commit rf.matchIndex={%v}", rf.me, rf.matchIndex)
			rf.commitIndex = majorIndex 
			go rf.applyLog()
			log.Printf("[workAsLeader]%v leader commitIndex={%v}", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) startElection(replyChan chan <-*RequestVoteReply) {
	//log.Printf("[startElection]%v ========", rf.me)
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			//log.Printf("[startElection]%v send vote request to %v", rf.me, server)
			args := rf.newRequestVoteArgs()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			//log.Printf("[startElection]%v recive vote response to %v, res={%v}, args={%v}, reply={%v}", rf.me, server, ok, args, reply)
			if ok {
				log.Printf("[startElection]%v send vote request to %v, res={%v}, args={%v}, reply={%v}", rf.me, server, ok, args, reply)
				replyChan<-reply
			}
		}(i)
	}
}

func (rf *Raft) workAsCandidate() {
	log.Printf("%v becomme a candidate time={%v} p={%v}\n", rf.me, time.Now(), rf.id)
	elecTimeout := time.Duration(rand.Intn(300)+300) * time.Millisecond
	log.Printf("%v elecTimeout={%v}\n", rf.me, elecTimeout)
	elecTicker := time.NewTicker(50*time.Millisecond)
	rf.currentTerm++
	rf.votedFor = rf.me
	agreeNum := 1
	replyChan := make(chan *RequestVoteReply)
	startTime := time.Now()
	rf.working = true
	rf.startElection(replyChan)
loop:
	for {
		select {
		case <-elecTicker.C:
			// timeout
			// reset data && restart election 
			/*
			if time.Now().Sub(startTime) > elecTimeout/2 {
				replyChan = make(chan *RequestVoteReply)
				agreeNum = 0
				rf.startElection(replyChan)
			}*/
			if time.Now().Sub(startTime) > elecTimeout {
				break loop
			}
		case <-rf.newTermCh:
			// discover new term 
			log.Printf("[workAsCandidate]%v receive new term\n", rf.me)
			//rf.votedFor = VOTED_FOR_NONE
			break loop
		case reply := <-replyChan:
			//log.Printf("%v recive reply={%v}\n", rf.me, reply)
			if reply.Term == rf.currentTerm && reply.VoteGranted{
				agreeNum++
			}
			if agreeNum > len(rf.peers)/2 {
				// recive major servers vote
				// change state 2 leader
				rf.state = STATE_LEADER
				break loop
			}
		case <-rf.appendEntryCh:
			//log.Printf("%v recive a heartbeat\n", rf.me)
			rf.state = STATE_FOLLOWER
			//rf.votedFor = VOTED_FOR_NONE
			break loop
		default:
		}
	}
}

func (rf *Raft) workAsFollower() {
	log.Printf("%v becomme a follower time={%v} id={%v}\n", rf.me, time.Now(), rf.id)
	// elect rand timeout 300 - 500 millisecond
	hbTimeout := time.Duration(rand.Intn(300)+300) * time.Millisecond
	elecTicker := time.NewTicker(50*time.Millisecond)
	//rf.votedFor = VOTED_FOR_NONE
	rf.lastHbTime = time.Now()
	rf.working = true
loop:
	for {
		select {
		case <-elecTicker.C:
			//log.Printf("follower: %v elect timeout, rf.heartbeat={%v}\n", rf.me, rf.heartbeat)
			//be a candidate && new leader elect
			timeSub := time.Now().Sub(rf.lastHbTime)
			if timeSub > hbTimeout {
				//log.Printf("[workAsFollower] wait hb timeout timeSub={%v} hbTimeout={%v} res={%v}\n", timeSub, hbTimeout, timeSub>hbTimeout)
				rf.state = STATE_CANDIDATE
				break loop
			}
		case <-rf.newTermCh:
			//rf.votedFor = VOTED_FOR_NONE
			break loop
		//	elecTicker.Reset(elecTimeout)
		case <-rf.appendEntryCh:
			//log.Printf("%v recive a heartbeat\n", rf.me)
			//rf.votedFor = VOTED_FOR_NONE
			rf.lastHbTime = time.Now()
		}
	}
}

func (rf *Raft) work() {
loop:
	for {
		switch rf.state {
		case STATE_FOLLOWER:
			rf.workAsFollower()
		case STATE_CANDIDATE:
			rf.workAsCandidate()
		case STATE_LEADER:
			rf.workAsLeader()
		default:
			break loop
		}
	}
	log.Printf("%v p={%v} exit\n", rf.me, rf.id)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	//log.Println(peers, me)
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyMsgCh = applyCh
	rf.state = STATE_FOLLOWER
	rf.votedFor = VOTED_FOR_NONE
	rf.newTermCh = make(chan struct{})
	rf.appendEntryCh = make(chan struct{})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.indexMus = make([]sync.RWMutex, len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.logs = []ReplicateLog{ReplicateLog{}}
	rf.id = rand.Intn(8999999999)+1000000000
	log.Printf("%v join p={%v}\n", rf.me, rf.id)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.work()

	return rf
}
