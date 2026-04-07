package raft

import (
	"fmt"
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

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, (%d, %d], LeaderCommit=%d", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

// AppendEntriesReply defines the reply for an AppendEntries RPC in Raft.
type AppendEntriesReply struct {
	Term          int // responder's current term, for leader to update itself
	Success       bool
	ConflictIndex int // the index of the first log entry that conflicts with the leader's log
	ConflictTerm  int // the term of the first log entry that conflicts with the leader's log
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("Term=%d, Success=%t, ConflictIndex=%d, ConflictTerm=%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

// AppendEntries is the RPC handler for the AppendEntries RPC.
// receives AppendEntriesArgs from leader and returns AppendEntriesReply to leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive AppendEntries, Args=%s", args.LeaderId, args.String())
	reply.Success = false
	// check if the term is lower, if so, become follower
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, Lower term %d < %d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// Reply must carry the latest local term after any term/role transition.
	reply.Term = rf.currentTerm
	// Reset election timeout whenever we receive valid AppendEntries (term >= ours), even on reject
	// legal leader can always reset election timeout
	defer func() {
		if args.Term >= rf.currentTerm {
			rf.resetElectionTimeoutLocked()
			// log the conflict if the follower is not successful
			if !reply.Success {
				LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
				LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower log: %s", args.LeaderId, rf.log.String())
			}
		}
	}()
	// PrevLogIndex is before our compacted prefix: entry is not in tailLog (stale RPC or leader behind our snapshot).
	if args.PrevLogIndex < rf.log.snapLastIndex {
		reply.ConflictIndex = rf.log.snapLastIndex + 1
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, prevLogIndex %d < snapLastIndex %d (stale)", args.LeaderId, args.PrevLogIndex, rf.log.snapLastIndex)
		return
	}
	// return if prevLog not matched (log too short)
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConflictIndex = rf.log.size()
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, follower's log is shorter than leader's prevLogIndex, len(log)=%d < P`%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	// return if prevLogTerm not matched
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.log.firstFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject AppendEntries, follower's prevLogTerm %d != leader's prevLogTerm %d", args.LeaderId, reply.ConflictTerm, args.PrevLogTerm)
		return
	}
	// append new entries to the follower's log
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, AppendEntries, follower accepted [%d,%d] entries", args.LeaderId, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
	// handle leader commit index(PartB)
	// if the leader's commit index is greater than the follower's commit index, update and apply the new commit index
	if args.LeaderCommit > rf.commitIndex {
		oldCommit := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.log.size()-1 {
			rf.commitIndex = rf.log.size() - 1
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AppendEntries Reply=%v", peer, reply.String())
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
		if !reply.Success {
			// Log fallback: use follower's ConflictIndex/ConflictTerm to backtrack nextIndex in one
			// step instead of decrementing index-by-index (§5.3).
			// Case 1 — ConflictTerm == InvalidTerm: follower log too short (PrevLogIndex >= len).
			//   Follower set ConflictIndex = len(log). Set nextIndex = ConflictIndex so next
			//   PrevLogIndex = nextIndex-1 aligns with follower's last entry (or before).
			// Case 2 — ConflictTerm set: term mismatch at PrevLogIndex; follower set
			//   ConflictIndex = first index of that term in its log. If leader has that term,
			//   set nextIndex to leader's first index of ConflictTerm; else use ConflictIndex.
			prevIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstLogIndex := rf.log.firstFor(reply.ConflictTerm)
				if firstLogIndex != InvalidIndex {
					rf.nextIndex[peer] = firstLogIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			// Late reply must not move nextIndex forward.
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			// Log next append anchor; index may lie only in snapshot ( < snapLastIndex ) or past tail — skip at().
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIndex && nextPrevIndex < rf.log.size() {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not match at Prev = [%d]T%d, try next Prev = [%d]T%d", peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader's log = %s", peer, rf.log.String())
			return
		}
		// update nextIndex and matchIndex if successful
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// update leader's commit index(PartB)
		// Rule from Figure 8: only commit current-term entries replicated to majority
		majorityMatched := rf.getMajorityMatchedIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
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
			rf.nextIndex[peer] = rf.log.size()
			rf.matchIndex[peer] = rf.log.size() - 1
			continue
		}
		// get the previous log index and term to send to the peer
		prevIndex := rf.nextIndex[peer] - 1
		// if the previous log index is less than the snapshot last index, send install snapshot RPC
		if prevIndex < rf.log.snapLastIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIndex,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send InstallSnapshot, Args=%s", peer, args.String())
			go rf.installToPeer(peer, term, args)
			// skip the rest of the loop
			continue
		}
		prevTerm := rf.log.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			// send the tail log entries from the previous log index + 1
			Entries:      rf.log.tail(prevIndex + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send AppendEntries, Args=%s", peer, args.String())
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
