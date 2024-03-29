package shardkv

import (
	"course/shardctrler"
	"sync"
	"time"
)

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
			switch cmd.Type {
			case ClientOp:
				op := cmd.Data.(Op)
				opReply = kv.applyClientOperation(op)
			case ConfigChange:
				newConfig := cmd.Data.(shardctrler.Config)
				opReply = kv.applyNewConfig(newConfig)
			case ShardMigrate:
				shardData := cmd.Data.(ShardOpReply)
				opReply = kv.applyShardMigration(&shardData)
			case ShardGC:
				shardMeta := cmd.Data.(ShardOpArgs)
				opReply = kv.applyShardGC(&shardMeta)
			default:
				panic("unknown config change type")
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

func (kv *ShardKV) applyClientOperation(op Op) *OpReply {
	if kv.matchGroup(op.Key) {
		var opReply *OpReply
		if op.Type != OpGet && kv.dupRequest(op.ClientId, op.SeqId) {
			opReply = kv.dupTable[op.ClientId].Reply
		} else {
			opReply = kv.applyToStateMachine(op, key2shard(op.Key))
			if op.Type != OpGet {
				kv.dupTable[op.ClientId] = LastOpInfo{op.SeqId, opReply}
			}
		}
		return opReply
	}
	return &OpReply{Err: ErrWrongGroup}
}

func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			needFetch := true
			kv.mu.Lock()
			for _, shard := range kv.shards {
				if shard.Status != ShardNormal {
					needFetch = false
					break
				}
			}
			newConfigNum := kv.currConfig.Num + 1
			kv.mu.Unlock()

			if needFetch {
				newConfig := kv.mck.Query(newConfigNum)
				if newConfig.Num == newConfigNum {
					kv.ConfigCmd(RaftCommand{ConfigChange, newConfig}, &OpReply{})
				}
			}
		}
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrateTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidShardIds := kv.getShardIdsByStatus(ShardMoveIn)

			var wg sync.WaitGroup
			for gid, shardIds := range gidShardIds {
				wg.Add(1)
				go func(servers []string, shardIds []int, configNum int) {
					defer wg.Done()
					fetchShardArgs := ShardOpArgs{configNum, shardIds}
					for _, server := range servers {
						var fetchShardReply ShardOpReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.FetchShardData", &fetchShardArgs, &fetchShardReply)
						if ok && fetchShardReply.Err == OK {
							kv.ConfigCmd(RaftCommand{ShardMigrate, fetchShardReply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], shardIds, kv.currConfig.Num)
			}

			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardMigrateInterval)
	}
}

func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidShardIds := kv.getShardIdsByStatus(ShardHangOn)
			for gid, shardIds := range gidShardIds {
				go func(servers []string, shardIds []int, configNum int) {
					deleteShardArgs := ShardOpArgs{configNum, shardIds}
					for _, server := range servers {
						var deleteShardReply ShardOpReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardData", &deleteShardArgs, &deleteShardReply)
						if ok && deleteShardReply.Err == OK {
							kv.ConfigCmd(RaftCommand{ShardGC, deleteShardArgs}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], shardIds, kv.currConfig.Num)
			}
			kv.mu.Unlock()
		}
		time.Sleep(ShardGCInterval)
	}
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	gidShardIds := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			if gid := kv.prevConfig.Shards[i]; gid != 0 {
				if _, ok := gidShardIds[gid]; !ok {
					gidShardIds[gid] = make([]int, 0)
				}
				gidShardIds[gid] = append(gidShardIds[gid], i)
			}
		}
	}
	return gidShardIds
}

func (kv *ShardKV) FetchShardData(args *ShardOpArgs, reply *ShardOpReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copy()
	}

	reply.DupTable = make(map[int64]LastOpInfo)
	for clientId, op := range kv.dupTable {
		reply.DupTable[clientId] = op.copy()
	}

	reply.Err = OK
	reply.ConfigNum = args.ConfigNum
}

func (kv *ShardKV) DeleteShardData(args *ShardOpArgs, reply *ShardOpReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	kv.ConfigCmd(RaftCommand{ShardGC, *args}, &opReply)

	reply.Err = opReply.Err
}
