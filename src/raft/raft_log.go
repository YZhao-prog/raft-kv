package raft

import (
	"course/labgob"
	"fmt"
)

// RaftLog is the compacted log: snapshot covers [1, snapLastIndex], tailLog holds the rest.
type RaftLog struct {
	snapLastIndex int        // last log index included in the snapshot
	snapLastTerm  int        // term of that entry (for PrevLog matching)
	snapshot      []byte     // state machine snapshot for indices [1, snapLastIndex]
	tailLog       []LogEntry // tailLog[0]=dummy(snapLastIndex); tailLog[1:]=[snapLastIndex+1, snapLastIndex+2, ...]
}

// NewLog builds a RaftLog from a snapshot and the following in-memory entries.
// snapLastIndex/snapLastTerm: last index (and its term) included in the snapshot.
// entries: log entries at logical indices snapLastIndex+1, snapLastIndex+2, ... (may be nil/empty).
// A dummy entry at tailLog[0] represents logical index snapLastIndex so that PrevLogIndex/PrevLogTerm are easy to compute.
func NewLog(snapLastIndex int, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}
	// tailLog[0] = dummy for logical index snapLastIndex (only .Term is used as PrevLogTerm)
	rl.tailLog = append(rl.tailLog, LogEntry{
		CommandValid: false,
		Command:      nil,
		Term:         snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

// readPersist restores RaftLog from labgob decoder.
// Decode order: snapLastIndex, snapLastTerm, tailLog.
// Snapshot bytes not decoded here.
// all the func below should be called with rf.mu held
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIndex = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

// persist saves RaftLog to labgob encoder.
func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// size returns one past the last valid logical index (so last valid index is size()-1). Indices are 0-based.
// Snapshot holds indices [1, snapLastIndex]; tailLog[1:] holds indices [snapLastIndex+1, size()-1].
// Example: [snapshot: 1 2 3 4 | tailLog: 4(d), 5, 6, 7]
//
//	snapshot: log entries 1–4 (snapLastIndex = 4)
//	tailLog[0]: 4 (dummy, same as last snapshot entry)
//	tailLog[1]: 5; tailLog[2]: 6; tailLog[3]: 7 (last entry)
//	size() = 8; valid logical indexes: [1, 7]
//	idx(4)=0 (dummy), idx(5)=1, idx(7)=3
func (rl *RaftLog) size() int {
	return rl.snapLastIndex + len(rl.tailLog)
}

// idx converts logical log index to tailLog array index. Panics if logicalIdx is not in [snapLastIndex, size()-1].
// tailLog[0] = dummy(snapLastIndex), tailLog[k] = logical index snapLastIndex+k.
func (rl *RaftLog) idx(logicalIdx int) int {
	if logicalIdx < rl.snapLastIndex || logicalIdx >= rl.size() {
		panic(fmt.Sprintf("logical index %d out of range [%d, %d]", logicalIdx, rl.snapLastIndex, rl.size()-1))
	}
	return logicalIdx - rl.snapLastIndex
}

// at returns the log entry at the given logical index. logicalIdx must be in [snapLastIndex, size()-1].
func (rl *RaftLog) at(logicalIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicalIdx)]
}

// last returns the last log index and term.
func (rl *RaftLog) last() (index int, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIndex + i, rl.tailLog[i].Term
}

// String returns a string summarizing the log grouped by term (e.g. "[1,4]T1,[5,7]T2").
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.tailLog[0].Term
	prevStart := rl.snapLastIndex
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf("[%d,%d]T%d,", prevStart, rl.snapLastIndex+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = rl.snapLastIndex + i
		}
	}
	terms += fmt.Sprintf("[%d,%d]T%d", prevStart, rl.size()-1, prevTerm)
	return terms
}

// firstFor returns the first logical log index with the given term, or InvalidIndex if not found.
func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return rl.snapLastIndex + idx
		}
		if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// tail returns the tail log entries from the given logical index (inclusive) to the end.
// startIdx is a logical index; use size() for the upper bound, not len(tailLog).
func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}
 
// doSnapshot is used when the application has applied through index and produced snapshot bytes.
// It moves the compaction boundary to index: entries [1,index] are represented only by snapshot;
// snapLastTerm is taken from the log entry at index. The in-memory tail keeps a dummy at index
// plus any existing entries after index so replication can continue without resending the compacted prefix.
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot
	newLog := make([]LogEntry, 0, len(rl.tailLog)-idx)
	newLog = append(newLog, LogEntry{
		CommandValid: false,
		Command:      nil,
		Term:         rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// installSnapshot replaces the log prefix on a follower after an InstallSnapshot RPC from the leader.
// index and term are LastIncludedIndex and LastIncludedTerm; snapshot holds the leader's state-machine image.
// The old tail is discarded: it may conflict with the leader or lie in a range the leader no longer has as entries.
// tailLog is reset to a single dummy at index so AppendEntries can append from index+1 onward.
func (rl *RaftLog) installSnapshot(index int, term int, snapshot []byte) {
	rl.snapLastIndex = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		CommandValid: false,
		Command: nil,
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}

// appendEntry appends a new entry to the log.
func (rl *RaftLog) append(entry LogEntry) {
	rl.tailLog = append(rl.tailLog, entry)
}

// appendFrom appends a new entry to the log from the given logical previous index.
func (rl *RaftLog) appendFrom(logicalPrevIdx int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicalPrevIdx)+1], entries...)
}
