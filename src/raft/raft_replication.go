package raft

import "time"

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
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply.Success = false
	reply.Term = rf.currentTerm

	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	} else {
		rf.becomeFollowerLocked(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)

	rf.resetElectionTimerLocked()
	reply.Success = true
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
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
		}

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

			// align the term
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// probe the lower index if the prev log not match
			if !reply.Success {
				idx := rf.nextIndex[peer] - 1
				term := rf.log[idx].Term
				// go back for a term
				for idx > 0 && rf.log[idx].Term == term {
					idx--
				}
				rf.nextIndex[peer] = idx + 1
				LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
				return
			}

			// update the match/next index if log is replicated
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// update commitIndex
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
