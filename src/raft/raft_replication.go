package raft

import "time"


type LogEntry struct {
	CommandValid bool // true if the command is valid
	Command      interface{} // the command to be executed in the state machine
	Term int // the term of the command
}

// AppendEntriesArgs defines the arguments for the AppendEntries RPC call in Raft.
// Used by the leader to replicate log entries (§5.3) and as a heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // leader's id
	PrevLogIndex int        // the index of the last log entry that the leader has already replicated
	PrevLogTerm  int        // the term of the last log entry that the leader has already replicated
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// AppendEntriesReply defines the reply for an AppendEntries RPC in Raft.
type AppendEntriesReply struct {
	Term    int  // responder's current term, for leader to update itself
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
	// return if prevLog not matched
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, follower's log is shorter than leader's prevLogIndex, len(log)=%d < P`%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	// return if prevLogTerm not matched
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, follower's prevLogTerm %d != leader's prevLogTerm %d", args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	// append new entries to the follower's log
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, AppendEntries, follower accepted [%d,%d] entries", args.LeaderId, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries))
	// TODO: handle leader commit index(PartB)
	// reset election timeout after successful AppendEntries
	rf.resetElectionTimeoutLocked()
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
		// handle failed reply, go back one term
		if !reply.Success {
			idx, term := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && term == rf.log[idx].Term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Replicate to peer %d failed, backtrack to %d", peer, idx + 1)
			return
		}
		// update nextIndex and matchIndex if successful
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// TODO: update leader's commit index(PartB)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d]", rf.currentTerm, rf.role, term)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// update leader's nextIndex and matchIndex to the last log entry
			rf.nextIndex[peer] = len(rf.log)
			rf.matchIndex[peer] = len(rf.log) - 1
			continue
		}
		// get the previous log index and term to send to the peer
		prevIndex := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIndex].Term
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm: prevTerm,
			Entries: rf.log[prevIndex + 1:],
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