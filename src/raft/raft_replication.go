package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	CommandValid bool        // true if the command is valid
	Command      interface{} // the command to be executed in the state machine
	Term         int         // the term of the command
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
	Term    int // responder's current term, for leader to update itself
	Success bool
}

// AppendEntries is the RPC handler for the AppendEntries RPC.
// receives AppendEntriesArgs from leader and returns AppendEntriesReply to leader.
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
	// Reset election timeout whenever we receive valid AppendEntries (term >= ours), even on reject
	// legal leader can always reset election timeout
	defer func() {
		if args.Term >= rf.currentTerm {
			rf.resetElectionTimeoutLocked()
		}
	}()
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
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, AppendEntries, follower accepted [%d,%d] entries", args.LeaderId, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
	// handle leader commit index(PartB)
	// if the leader's commit index is greater than the follower's commit index, update and apply the new commit index
	if args.LeaderCommit > rf.commitIndex {
		oldCommit := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.log)-1 { // avoid commit index out of range
			rf.commitIndex = len(rf.log) - 1
		}
		LOG(rf.me, rf.currentTerm, DApply, "Update commit index from %d to %d by leader's commit index %d", oldCommit, rf.commitIndex, args.LeaderCommit)
		rf.applyCond.Signal() // signal the apply loop to apply the new commit index
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// getMajorityMatchedIndexLocked returns the largest log index that has been
// replicated (matchIndex) on a majority of peers.
// For example, in a 5-node cluster, if matchIndex = [1, 3, 5, 2, 4],
// after sorting: [1, 2, 3, 4, 5], then (len(tmpIndex) - 1) / 2 = 2, majorityIndex = 2, tmpIndex[2] = 3,
// which means at least 3 nodes have matchIndex >= 3, so index less than or equal to 3 can be committed.
func (rf *Raft) getMajorityMatchedIndexLocked() int {
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex) // copy the matchIndex array to avoid modifying the original array
	sort.Ints(tmpIndex)
	majorityIndex := (len(tmpIndex) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majorityIndex=%d, majorityMatchedIndex=%d", tmpIndex, majorityIndex, tmpIndex[majorityIndex])
	return tmpIndex[majorityIndex]
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
		// check context, make sure we are still a leader
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}
		// handle failed reply, go back one term
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			idx, termVal := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && termVal == rf.log[idx].Term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			// avoid out-of-order reply moving nextIndex forward
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			LOG(rf.me, rf.currentTerm, DLog, "Replicate to peer %d failed, backtrack to %d", peer, rf.nextIndex[peer])
			return
		}
		// update nextIndex and matchIndex if successful
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// update leader's commit index(PartB)
		// Rule from Figure 8: only commit current-term entries replicated to majority
		majorityMatched := rf.getMajorityMatchedIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			oldCommit := rf.commitIndex
			rf.commitIndex = majorityMatched
			LOG(rf.me, rf.currentTerm, DApply, "Update commit index from %d to %d by majority matched index %d", oldCommit, rf.commitIndex, majorityMatched)
			rf.applyCond.Signal() // signal the apply loop to apply the new commit index
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check context, make sure we are still a leader in this term
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Context lost, T%d:Leader->T%d:%s", term, rf.currentTerm, rf.role)
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
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIndex+1:],
			LeaderCommit: rf.commitIndex,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// Could only replicate during the given term.
func (rf *Raft) replicationTickerLocked(term int) {
	// Send immediate heartbeat when becoming leader so partitioned nodes step down quickly
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}
