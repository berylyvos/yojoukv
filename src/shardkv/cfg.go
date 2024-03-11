package shardkv

import (
	"course/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCmd(cmd RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Value = res.Value
		reply.Err = res.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			// join shard
			if kv.currConfig.Shards[i] != kv.gid && kv.gid == newConfig.Shards[i] {
				// check if the shard is from a valid group
				if gid := kv.currConfig.Shards[i]; gid != 0 {
					kv.shards[i].Status = ShardMoveIn
				}
			}
			// leave shard
			if kv.currConfig.Shards[i] == kv.gid && kv.gid != newConfig.Shards[i] {
				// check if the shard is gonna join a valid group
				if gid := newConfig.Shards[i]; gid != 0 {
					kv.shards[i].Status = ShardMoveOut
				}
			}
		}
		// update config
		kv.prevConfig = kv.currConfig
		kv.currConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardMigration(shardOpReply *ShardOpReply) *OpReply {
	if shardOpReply.ConfigNum == kv.currConfig.Num {
		// copy shard data
		for shardId, shardData := range shardOpReply.ShardData {
			shard := kv.shards[shardId]
			if shard.Status == ShardMoveIn {
				shard.copyFrom(shardData)
				shard.Status = ShardHangOn
			} else {
				break
			}
		}
		// copy duptale
		for clientId, lastOpInfo := range shardOpReply.DupTable {
			myLastOpInfo, ok := kv.dupTable[clientId]
			if !ok || myLastOpInfo.SeqId < lastOpInfo.SeqId {
				kv.dupTable[clientId] = lastOpInfo
			}
		}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardMeta *ShardOpArgs) *OpReply {
	if shardMeta.ConfigNum == kv.currConfig.Num {
		for _, shardId := range shardMeta.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == ShardHangOn {
				shard.Status = ShardNormal
			} else if shard.Status == ShardMoveOut {
				kv.shards[shardId] = NewInMemSM()
			} else {
				break
			}
		}
	}
	return &OpReply{Err: OK}
}
