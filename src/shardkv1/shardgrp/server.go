package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVStore struct {
	Value   string
	Version rpc.Tversion
}

type ShardState int

const (
	ShardOwned   ShardState = iota // 0: Shard 归属当前 Group，正常读写
	ShardFrozen                    // 1. Shard 归属当前 Group，但已冻结，只读，准备迁出 (只接受 Get)
	ShardDeleted                   // 2: Shard 数据已删除 (不接受 Get/Put)
)

type ShardKVStore struct {
	ShardID    shardcfg.Tshid
	ShardState ShardState
	ShardDB    map[string]KVStore
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu           sync.Mutex
	configNum    shardcfg.Tnum
	ShardStorage map[shardcfg.Tshid]ShardKVStore
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := req.(type) {
	case rpc.GetArgs:
		shardId := shardcfg.Key2Shard(op.Key)

		shardStorage, ok := kv.ShardStorage[shardId]
		if !ok || shardStorage.ShardState == ShardDeleted {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		if item, ok := shardStorage.ShardDB[op.Key]; ok {
			return rpc.GetReply{Value: item.Value, Version: item.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:
		shardId := shardcfg.Key2Shard(op.Key)

		shardStorage, ok := kv.ShardStorage[shardId]
		if !ok || shardStorage.ShardState == ShardDeleted {
			//return rpc.PutReply{Err: rpc.ErrWrongGroup}
			// This version will build shardID
			kv.BuildShard(shardId)
		}
		shardStorage = kv.ShardStorage[shardId]

		item, ok := shardStorage.ShardDB[op.Key]

		if ok && item.Version != op.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		newVersion := op.Version + 1
		shardStorage.ShardDB[op.Key] = KVStore{
			Value:   op.Value,
			Version: newVersion,
		}
		return rpc.PutReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) BuildShard(shardID shardcfg.Tshid) {
	_, ok := kv.ShardStorage[shardID]

	if ok {
		return
	}

	shardStorage := ShardKVStore{
		ShardID:    shardID,
		ShardState: ShardOwned,
		ShardDB:    make(map[string]KVStore),
	}

	kv.ShardStorage[shardID] = shardStorage
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.configNum); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.ShardStorage); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var nextConfigNum shardcfg.Tnum
	var nextDb map[shardcfg.Tshid]ShardKVStore

	if err := d.Decode(&nextConfigNum); err != nil {
		panic(err)
	}
	if err := d.Decode(&nextDb); err != nil {
		panic(err)
	}

	kv.configNum = nextConfigNum
	kv.ShardStorage = nextDb
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	rep, ok := res.(rpc.GetReply)

	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = rep
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	// 从 any 类型转换为 rpc.PutReply
	rep, ok := res.(rpc.PutReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader // 通知客户端重试找 Leader
		return
	}

	*reply = rep
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(KVStore{})
	labgob.Register(ShardKVStore{})

	kv := &KVServer{
		gid:          gid,
		me:           me,
		configNum:    0,
		ShardStorage: make(map[shardcfg.Tshid]ShardKVStore),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
