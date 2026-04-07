package raft

import "fmt"

// Snapshot is called by the application when it has applied through index and created a snapshot.
// The application passes the snapshot bytes; Raft truncates the log to that index and persists.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

// InstallSnapshotArgs defines the arguments for the InstallSnapshot RPC call in Raft.
// Used by the leader to install a snapshot to a follower.
type InstallSnapshotArgs struct {
	Term         int // leader's term
	LeaderId     int // leader's id
	Snapshot     []byte // snapshot data
	LastIncludedIndex int // the last included index
	LastIncludedTerm int // the last included term
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Term=%d, LeaderId=%d, LastIncludedIndex=%d, LastIncludedTerm=%d", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term        int  // responder's current term, for follower to update itself
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("Term=%d", reply.Term)
}

// InstallSnapshot is the RPC handler invoked when a follower receives an InstallSnapshot RPC from a leader.
// It installs a snapshot to a follower and updates the follower's state.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Acquire lock for concurrent safety
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive InstallSnapshot, Args=%s", args.LeaderId, args.String())
	// if leader's term is lower, reject the request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Reject InstallSnapshot, Lower term %d < %d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	// if leader's term is higher, become follower
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// Reply must carry the latest local term after any term/role transition.
	reply.Term = rf.currentTerm
	// Skip if our compacted prefix already reaches (or passes) what the leader is sending.
	if rf.log.snapLastIndex >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Skip InstallSnapshot, local snapLastIndex=%d >= leader LastIncludedIndex=%d", args.LeaderId, rf.log.snapLastIndex, args.LastIncludedIndex)
		return
	}
	// install the snapshot
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	// set snapPending to true and signal the apply loop to apply the new snapshot
	rf.snapPending = true
	rf.applyCond.Signal()
}

// leader sends an InstallSnapshot RPC to a follower.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// leader installs a snapshot to a follower.
func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	// send install snapshot RPC to peer, no lock needed here because we are not accessing any shared state
	ok := rf.sendInstallSnapshot(peer, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check if the request is successful
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "Install snapshot to peer %d failed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, InstallSnapshot Reply=%v", peer, reply.String())
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
	// update matchIndex and nextIndex
	// if the follower's matchIndex is less than the last included index, update the matchIndex and nextIndex
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	// note: don't need to update commitIndex here because the snapshot is already committed
}