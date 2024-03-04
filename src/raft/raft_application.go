package raft

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// wait for commitIndex to update, and lock released
		rf.applyCond.Wait()

		idx := rf.lastApplied + 1
		entries := make([]LogEntry, 0)
		for i := idx; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log.at(i))
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: idx + i,
			}
		}

		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
