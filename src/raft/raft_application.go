package raft

// applicationTicker delivers state-machine updates to the service via applyCh.
// It wakes on applyCond when either:
//
// Path 1 — log commands (commitIndex advanced): collect entries in (lastApplied, commitIndex],
// send ApplyMsg with CommandValid for each, then lastApplied += len(entries).
//
// Path 2 — snapshot (InstallSnapshot set snapPending): the compacted prefix is not in log as
// individual entries; send one ApplyMsg with SnapshotValid and the bytes from rf.log, then set
// lastApplied to snapLastIndex, optionally bump commitIndex, and clear snapPending.
//
// Sends are done without holding rf.mu so slow applies do not block Raft RPCs.
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		snapPendingApply := rf.snapPending
		// Path 1: replay committed log entries still present in tailLog.
		if !snapPendingApply {
			if rf.lastApplied < rf.log.snapLastIndex {
					rf.lastApplied = rf.log.snapLastIndex
			}
			// make sure that the rf.log have all the entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		// Path 1: push each committed command to the application.
		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + i + 1,
				}
			}
		} else {
			// Path 2: install leader snapshot into the application (state machine image, not log replay).
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIndex,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		// Update lastApplied (and commitIndex on snapshot path) under the lock.
		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Applied [%d, %d] entries to the state machine", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Applied snapshot for [0,%d]", rf.log.snapLastIndex)
			rf.lastApplied = rf.log.snapLastIndex
			// if the last applied index is greater than the commit index, update the commit index
			if rf.lastApplied > rf.commitIndex {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
