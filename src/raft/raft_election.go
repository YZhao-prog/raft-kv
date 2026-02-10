package raft

import (
	"math/rand"
	"time"
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
