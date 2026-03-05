package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("currentTerm: %d, votedFor: %d, log: [0:%d]", rf.currentTerm, rf.votedFor, len(rf.log)-1)
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
	e.Encode(rf.currentTerm) // encode the currentTerm field
	e.Encode(rf.votedFor) // encode the votedFor field
	e.Encode(rf.log) // encode the log field
	raftstate := w.Bytes() // get the encoded data bytes array
	rf.persister.Save(raftstate, nil) // save the encoded data bytes array to the persister
	LOG(rf.me, rf.currentTerm, DPersist, "persist to disk: %s", rf.persistString())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	var currentTerm int
	var votedFor int
	var log []LogEntry
	r := bytes.NewBuffer(data) // create a new buffer to store the encoded data
	d := labgob.NewDecoder(r) // create a new decoder to decode the data
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode currentTerm error")
		return
	}
	rf.currentTerm = currentTerm
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode votedFor error")
		return
	}
	rf.votedFor = votedFor
	if err := d.Decode(&log); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "read from disk: decode log error")
		return
	}
	rf.log = log
	LOG(rf.me, rf.currentTerm, DPersist, "read from disk: %s", rf.persistString())
}