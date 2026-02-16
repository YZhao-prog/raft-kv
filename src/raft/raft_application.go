package raft

// applicationTicker is a long-running goroutine responsible for
// applying committed but not-yet-applied log entries to the state machine.
// It waits for a signal (via rf.applyCond) that commitIndex has advanced.
// Upon waking up, it checks which entries between lastApplied+1 and commitIndex
// should be applied, sends them to the service through applyCh, and
// advances lastApplied accordingly. This ensures exactly-once semantics
// for each committed entry in Raft.
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		// Wait until there are committed entries to apply.
		rf.mu.Lock()
		rf.applyCond.Wait()

		// Gather new entries to apply: (lastApplied, commitIndex]
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()

		// Apply entries outside the lock (lastApplied won't be changed by others).
		// Applying entries to the state machine may block; if we hold the lock while doing so,
		// other Raft operations could be delayed. Therefore, we release the lock before applying,
		// ensuring the lock is not held for long periods during (potentially slow) application.
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i + 1,
			}
		}

		// After applying, update lastApplied.
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Applied [%d, %d] entries to the state machine", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}