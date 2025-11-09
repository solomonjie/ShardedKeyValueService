package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]KVPair
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.store = make(map[string]KVPair)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Server Get: Key=%s", args.Key)

	if pair, ok := kv.store[args.Key]; ok {
		reply.Value = pair.Value
		reply.Version = pair.Version
		reply.Err = rpc.OK
		DPrintf("Server Get: Key=%s found. Value=%s, Version=%d", args.Key, pair.Value, pair.Version)
	} else {
		reply.Err = rpc.ErrNoKey
		DPrintf("Server Get: Key=%s not found", args.Key)
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Server Put: Key=%s, Value=%s, ReqVersion=%d", args.Key, args.Value, args.Version)
	if pair, ok := kv.store[args.Key]; ok {
		if args.Version == pair.Version {
			pair.Value = args.Value
			pair.Version++
			kv.store[args.Key] = pair
			reply.Err = rpc.OK
			DPrintf("Server Put: Key=%s updated. New Version=%d", args.Key, pair.Version)
		} else {
			reply.Err = rpc.ErrVersion
			DPrintf("Server Put: Key=%s version mismatch. ReqVersion=%d, CurrentVersion=%d", args.Key, args.Version, pair.Version)
		}
	} else {
		if args.Version == 0 {
			kv.store[args.Key] = KVPair{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			DPrintf("Server Put: Key=%s created. Version=1", args.Key)
		} else {
			reply.Err = rpc.ErrNoKey
			DPrintf("Server Put: Key=%s not found and ReqVersion is not 0", args.Key)
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
