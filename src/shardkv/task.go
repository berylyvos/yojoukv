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

			var opReply *OpReply
			cmd := msg.Command.(RaftCommand)
			if cmd.Type == ClientOp {
				op := cmd.Data.(Op)
				if op.Type != OpGet && kv.dupRequest(op.ClientId, op.SeqId) {
					opReply = kv.dupTable[op.ClientId].Reply
				} else {
					opReply = kv.applyToStateMachine(op, key2shard(op.Key))
					if op.Type != OpGet {
						kv.dupTable[op.ClientId] = LastOpInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}
			} else if cmd.Type == ConfigChange {
				opReply = kv.handleConfigChange(cmd)
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
		newConfig := kv.mck.Query(kv.currConfig.Num + 1)
		kv.mu.Unlock()

		kv.ConfigCmd(RaftCommand{
			Type: ConfigChange,
			Data: newConfig,
		}, &OpReply{})

		time.Sleep(FetchConfigInterval)
	}
}
