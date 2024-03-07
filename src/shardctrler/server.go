package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead         int32
	lastApplied  int
	stateMachine *CtrlerSM
	notifyChans  map[int]chan *OpReply
	dupTable     map[int64]LastOpInfo

	configs []Config // indexed by config num
}

type Op struct {
	Servers  map[int][]string // for Join
	GIDs     []int            // for Leave
	Shard    int              // for Move
	GID      int              // for Move
	Num      int              // for Query
	Type     OpType
	ClientId int64
	SeqId    int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	opReply := &OpReply{}
	sc.handle(Op{
		Type:     OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	opReply := &OpReply{}
	sc.handle(Op{
		Type:     OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	opReply := &OpReply{}
	sc.handle(Op{
		Type:     OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	opReply := &OpReply{}
	sc.handle(Op{
		Type: OpQuery,
		Num:  args.Num,
	}, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Config = opReply.Config
}

func (sc *ShardCtrler) handle(op Op, reply *OpReply) {
	reply.WrongLeader = false

	sc.mu.Lock()
	if op.Type != OpQuery && sc.isDupRequest(op.ClientId, op.SeqId) {
		opReply := sc.dupTable[op.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case res := <-ch:
		reply.Config = res.Config
		reply.Err = res.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerSM()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.dupTable = make(map[int64]LastOpInfo)

	go sc.applyTask()
	return sc
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				var opReply *OpReply
				if op.Type != OpQuery && sc.isDupRequest(op.ClientId, op.SeqId) {
					opReply = sc.dupTable[op.ClientId].Reply
				} else {
					opReply = sc.applyToStateMachine(op)
					if op.Type != OpQuery {
						sc.dupTable[op.ClientId] = LastOpInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				if _, isLeader := sc.rf.GetState(); isLeader {
					ch := sc.getNotifyChannel(msg.CommandIndex)
					ch <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	var err Err
	var cfg Config
	switch op.Type {
	case OpQuery:
		cfg, err = sc.stateMachine.Query(op.Num)
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	}
	return &OpReply{Config: cfg, Err: err}
}

func (sc *ShardCtrler) isDupRequest(clientId, seqId int64) bool {
	lastOpInfo, ok := sc.dupTable[clientId]
	return ok && lastOpInfo.SeqId >= seqId
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}
