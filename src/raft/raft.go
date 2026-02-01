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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	// replicate interval, 30ms is enough for the network to replicate the log
	replicateInterval time.Duration = 30 * time.Millisecond
)

// resetElectionTimeoutLocked sets a new random election timeout duration for this Raft peer.
// Should be called while holding rf.mu. It also resets the electionStart time.
// The new timeout is randomly chosen between electionTimeoutMin and electionTimeoutMax.
func (rf *Raft) resetElectionTimeoutLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// isElectionTimeout checks if the current time has exceeded the election timeout duration.
// Returns true if the elapsed time is greater than the timeout, false otherwise.
func (rf *Raft) isElectionTimeout() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// Role defines the role/state of a Raft server (Follower, Leader, or Candidate).
type Role string

const (
	Follower  Role = "Follower"  // Server is a follower, listening to leader
	Leader    Role = "Leader"    // Server is the current leader, managing log replication
	Candidate Role = "Candidate" // Server is a candidate, requesting votes for leadership
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role            Role
	currentTerm     int
	votedFor        int           // -1 if no one, otherwise candidate's id
	electionStart   time.Time     // time when election started, used for election timeout
	electionTimeout time.Duration // duration of election timeout, random between 150-300ms
}

// becomeFollowerLocked transitions the Raft server to the Follower role.
// If the given term is less than the current term, logs an error and does not transition roles.
// If the term is higher, resets the votedFor field.
// Updates the currentTerm to the specified term.
func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, term, DError, "Can't become follower in term %d, current term is %d", term, rf.currentTerm)
	}

	LOG(rf.me, term, DLog, "%s->Follower, For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

// becomeCandidateLocked transitions the Raft server to the Candidate role.
// If the server is currently the Leader, it logs an error and returns without changing state.
// Otherwise, it updates its role to Candidate, increments its term, votes for itself,
// and records the time the election started.
func (rf *Raft) becomeCandidateLocked(term int) {
	if rf.role == Leader {
		LOG(rf.me, term, DError, "Can't become candidate, already leader")
		return
	}

	rf.currentTerm++
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d->T%d", rf.role, rf.currentTerm-1, rf.currentTerm)
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimeoutLocked()
}

// becomeLeaderLocked transitions the Raft server to the Leader role.
// It should only be called when the server is a Candidate and is to become the Leader.
// Logs an error and does not proceed if not currently a Candidate.
// Updates the role to Leader and logs the transition.
func (rf *Raft) becomeLeaderLocked(term int) {
	if rf.role != Candidate {
		LOG(rf.me, term, DError, "Can't become leader, not a candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "%s->Leader, For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
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
	// Your code here (PartC).
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
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote for current term
}

// RequestVote is the RPC handler invoked when a server receives a RequestVote RPC from a candidate.
// It decides whether to grant its vote to the requesting candidate according to the Raft protocol.
// This includes term comparison, log up-to-dateness check, and vote tracking for the current term.
// This function should be called with the server's lock held to ensure state consistency.
// Candidate.startElection
//
//	-> sendRequestVote(peer, args, reply)
//	   -> labrpc.Call("Raft.RequestVote", ...)
//	      -> Raft.RequestVote(args, reply)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Acquire lock for concurrent safety
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Always set reply term to this server's current term (so candidate can update itself)
	reply.Term = rf.currentTerm

	// Default to not granting the vote
	reply.VoteGranted = false

	// If candidate's term is less, reject immediately as outdated
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, reject vote request, term %d > %d", args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// If candidate's term is higher, convert to follower and update term
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// If already voted for another candidate in this term, reject, make sure the voted for candidate is the same as the candidate requesting vote
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, reject vote request, already voted for S%d", args.CandidateId, rf.votedFor)
		return
	}

	// Otherwise, grant vote to candidate, record vote, and reset election timer
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimeoutLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, vote granted", args.CandidateId)
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

	// Your code here (PartB).

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

// contextLostLocked checks if the context of the Raft server has been lost.
// Returns true if the context has been lost, false otherwise.
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.role == role && term == rf.currentTerm)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries is the RPC handler for the AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// always set reply term to this server's current term
	reply.Term = rf.currentTerm
	reply.Success = false
	// check if the term is lower, if so, become follower
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, Lower term %d < %d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// reset election timeout after successful AppendEntries
	rf.resetElectionTimeoutLocked()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// startReplication starts replication to all peers.
// It should only be called when the server is a Leader and is to replicate to all peers.
// Logs an error and does not proceed if not currently a Leader.
// Updates the role to Leader and logs the transition.
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		// send append entries RPC to peer, no lock needed here because we are not accessing any shared state
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// check if the request is successful
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "Replicate to peer %d failed", peer)
			return
		}
		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d]", rf.currentTerm, rf.role, term)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// Could only replicate during the given term.
func (rf *Raft) replicationTickerLocked(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}

// startElection starts an election for the given term.
// It should only be called when the server is a Candidate and is to become the Leader.
// Logs an error and does not proceed if not currently a Candidate.
// Updates the role to Leader and logs the transition.
func (rf *Raft) startElection(term int) {
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		// send request vote RPC to peer, no lock needed here because we are not accessing any shared state
		ok := rf.sendRequestVote(peer, args, reply)
		// handle reply
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from peer %d failed", peer)
			return
		}
		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// check the context, make sure we are still a candidate
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, aborting request vote to peer %d", peer)
			return
		}
		// check the vote
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked(term)
				go rf.replicationTickerLocked(term)
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// startElection is called asynchronously via goroutine, so you cannot predict exactly when it will execute.
	// Therefore, context checks must be performed at execution time to ensure expectations are still valid.
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, aborting RequestVote", rf.role)
		return
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		go askVoteFromPeer(peer, args)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeout() {
			rf.becomeCandidateLocked(rf.currentTerm)
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower // initially a follower
	rf.currentTerm = 0 // initially term is 0
	rf.votedFor = -1   // initially no one has voted
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
