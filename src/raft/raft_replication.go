package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	// leader’s commitIndex for updating follower's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())
	reply.Success = false
	reply.Term = rf.currentTerm

	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	} else {
		rf.becomeFollowerLocked(args.Term)
	}

	// as a follower, we must reset election timer whether we accpet the log or not
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.log.String())
		}
	}()

	// Reply false if log doesn’t match
	// if follower's log is too short, set XIndex = len(log)
	if args.PrevLogIndex >= rf.log.size() {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		reply.ConflictIndex = rf.log.size()
		reply.ConflictTerm = InvalidTerm
		return
	}
	// if prevLog's term doesn't match leader's
	// set XTerm = follower's prev term
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.log.firstIndexOf(reply.ConflictTerm)
		return
	}

	// Append any new entries not already in the log
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= rf.log.size() {
			rf.commitIndex = rf.log.size() - 1
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) getMajorMatchIndexLocked() int {
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndex))
	majorIdx := (len(tmpIndex) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndex, majorIdx, tmpIndex[majorIdx])
	return tmpIndex[majorIdx]
}

func (rf *Raft) startReplication(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log.at(prevIdx).Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, %v", peer, args.String())

		// replicate to peer
		go func(peer int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
				return
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

			// align the term
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// check the context
			if rf.contextLostLocked(Leader, term) {
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
				return
			}

			// probe the lower index if the prev log not match
			if !reply.Success {
				oldNext := rf.nextIndex[peer]
				if reply.ConflictTerm == InvalidIndex {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					leaderFirstIndex := rf.log.firstIndexOf(reply.ConflictTerm)
					if leaderFirstIndex != InvalidIndex {
						rf.nextIndex[peer] = leaderFirstIndex + 1
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				}
				// avoid the out-of-order reply move the nextIndex forward
				if rf.nextIndex[peer] > oldNext {
					rf.nextIndex[peer] = oldNext
				}
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, rf.nextIndex[peer]-1, rf.log.at(rf.nextIndex[peer]-1).Term)
				LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.log.String())
				return
			}

			// update the match/next index if log is replicated
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// update commitIndex
			majorMatchIndex := rf.getMajorMatchIndexLocked()
			// leader can only commit the log in the current term!
			if majorMatchIndex > rf.commitIndex && rf.log.at(majorMatchIndex).Term == rf.currentTerm {
				LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorMatchIndex)
				rf.commitIndex = majorMatchIndex
				rf.applyCond.Signal()
			}
		}(peer, args)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			return
		}

		time.Sleep(replicationInterval)
	}
}
