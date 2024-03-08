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

func (kv *ShardKV) handleConfigChange(cmd RaftCommand) *OpReply {
	switch cmd.Type {
	case ConfigChange:
		newConfig := cmd.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	default:
		panic("unknown config change type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currConfig.Shards[i] != kv.gid && kv.gid == newConfig.Shards[i] {
				// join shard
			}
			if kv.currConfig.Shards[i] == kv.gid && kv.gid != newConfig.Shards[i] {
				// leave shard
			}
		}
		// update config
		kv.currConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}
