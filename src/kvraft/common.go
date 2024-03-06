package kvraft

import (
	"fmt"
	"log"
	"time"
)

const (
	ClientRequestTimeout = 500 * time.Millisecond
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClinetId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

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
	ClinetId int64
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
