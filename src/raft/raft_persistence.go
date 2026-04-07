package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("currentTerm: %d, votedFor: %d, log: [0:%d]", rf.currentTerm, rf.votedFor, rf.log.size()-1)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// persistLocked saves persistent state; caller must hold rf.mu.
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	w := new(bytes.Buffer) // create a new buffer to store the encoded data
	e := labgob.NewEncoder(w) // create a new encoder to encode the data
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persist(e) // encode the log
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot) // save the encoded data bytes array and the snapshot to the persister
	LOG(rf.me, rf.currentTerm, DPersist, "persist to disk: %s", rf.persistString())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	r := bytes.NewBuffer(data) // create a new buffer to store the encoded data
	d := labgob.NewDecoder(r) // create a new decoder to decode the data create a new decoder to decode the data
	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode currentTerm error")
		return
	}
	rf.currentTerm = currentTerm
	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode votedFor error")
		return
	}
	rf.votedFor = votedFor
	// decode the log
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode log error")
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot() // read the snapshot from the persister
	// the readSnapshot function reset the commit index and last applied index to zero
	// so we need to update the commit index and last applied index if the last included index is greater than the commit index
	if rf.log.snapLastIndex > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIndex
		rf.lastApplied = rf.log.snapLastIndex
	}
	LOG(rf.me, rf.currentTerm, DPersist, "read from disk: %s", rf.persistString())
}