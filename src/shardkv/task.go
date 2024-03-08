package shardkv

import "time"

func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Op)
			var opReply *OpReply
			if op.Type != OpGet && kv.dupRequest(op.ClientId, op.SeqId) {
				opReply = kv.dupTable[op.ClientId].Reply
			} else {
				opReply = kv.applyToStateMachine(op)
				if op.Type != OpGet {
					kv.dupTable[op.ClientId] = LastOpInfo{
						SeqId: op.SeqId,
						Reply: opReply,
					}
				}
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				ch := kv.getNotifyChannel(msg.CommandIndex)
				ch <- opReply
			}

			if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
				kv.makeSnapshot(msg.CommandIndex)
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.currConfig = kv.mck.Query(-1)
		kv.mu.Unlock()

		time.Sleep(FetchConfigInterval)
	}
}
