package kvraft

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied  int
	stateMachine *InMemSM
	notifyChans  map[int]chan *OpReply
	dupTable     map[int64]LastOpInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, Type: OpGet})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
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
		kv.removeNotifyChan(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDupRequest(clientId, seqId int64) bool {
	lastOpInfo, ok := kv.dupTable[clientId]
	return ok && lastOpInfo.SeqId >= seqId
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.isDupRequest(args.ClinetId, args.SeqId) {
		opReply := kv.dupTable[args.ClinetId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		Type:     getOpType(args.Op),
		ClinetId: args.ClinetId,
		SeqId:    args.SeqId,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewInMemSM()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.dupTable = make(map[int64]LastOpInfo)

	go kv.applyTask()
	return kv
}

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				var opReply *OpReply
				if op.Type != OpGet && kv.isDupRequest(op.ClinetId, op.SeqId) {
					opReply = kv.dupTable[op.ClinetId].Reply
				} else {
					opReply = kv.applyToStateMachine(op)
					if op.Type != OpGet {
						kv.dupTable[op.ClinetId] = LastOpInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					ch := kv.getNotifyChan(msg.CommandIndex)
					ch <- opReply
				}

				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.Type {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *KVServer) getNotifyChan(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeNotifyChan(index int) {
	delete(kv.notifyChans, index)
}
