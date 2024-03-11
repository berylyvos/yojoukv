package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	ClientRequestTimeout = 500 * time.Millisecond
	FetchConfigInterval  = 100 * time.Millisecond
	ShardMigrateInterval = 50 * time.Millisecond
	ShardGCInterval      = 50 * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key      string
	Value    string
	Type     OpType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Value string
	Err   Err
}

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

func getOpType(v string) OpType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

type LastOpInfo struct {
	SeqId int64
	Reply *OpReply
}

func (op *LastOpInfo) copy() LastOpInfo {
	return LastOpInfo{
		SeqId: op.SeqId,
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
	}
}

type RaftCommandType uint8

const (
	ClientOp RaftCommandType = iota
	ConfigChange
	ShardMigrate
	ShardGC
)

type RaftCommand struct {
	Type RaftCommandType
	Data interface{}
}

type ShardStatus uint8

const (
	ShardNormal ShardStatus = iota
	ShardMoveIn
	ShardMoveOut
	ShardHangOn
)

type ShardOpArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOpReply struct {
	Err       Err
	ConfigNum int
	ShardData map[int]map[string]string
	DupTable  map[int64]LastOpInfo
}
