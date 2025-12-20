package shardgrp

import (
	"bytes"
	"log"
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

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu           sync.Mutex
	configNum    shardcfg.Tnum
	shardsState  map[shardcfg.Tshid]ShardState
	ShardStorage map[shardcfg.Tshid]map[string]KVStore
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := req.(type) {
	case rpc.GetArgs:
		shardId := shardcfg.Key2Shard(op.Key)
		//log.Printf("Receive %T Request: %v", req, req)
		shardState, ok := kv.shardsState[shardId]
		if !ok || (shardState != ShardOwned && shardState != ShardFrozen) {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		shardData := kv.ShardStorage[shardId]
		if item, ok := shardData[op.Key]; ok {
			return rpc.GetReply{Value: item.Value, Version: item.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:
		shardId := shardcfg.Key2Shard(op.Key)
		shardState, ok := kv.shardsState[shardId]
		if !ok || shardState != ShardOwned {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		shardData := kv.ShardStorage[shardId]
		item, ok := shardData[op.Key]

		if ok && item.Version != op.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		newVersion := op.Version + 1
		shardData[op.Key] = KVStore{
			Value:   op.Value,
			Version: newVersion,
		}
		return rpc.PutReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:

		return kv.applyDelete(op)
	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(op)
	case shardrpc.InstallShardArgs:
		return kv.applyInstall(op)
	default:
		return nil
	}
}

func (kv *KVServer) applyInstall(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	if args.Num < kv.configNum {
		return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	}

	kv.configNum = args.Num
	//kv.BuildShard(args.Shard)
	shardStorage := kv.deserializeShardData(args.State)
	kv.ShardStorage[args.Shard] = shardStorage
	kv.shardsState[args.Shard] = ShardOwned

	return shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) applyFreeze(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	if args.Num < kv.configNum {
		return shardrpc.FreezeShardReply{Num: kv.configNum, Err: rpc.ErrWrongGroup}
	}

	shardState, ok := kv.shardsState[args.Shard]

	if !ok || shardState == ShardDeleted {
		// 不拥有此分片，无法冻结
		return shardrpc.FreezeShardReply{Num: kv.configNum, Err: rpc.ErrWrongGroup}
	}

	// 收集并序列化数据
	state := kv.serializeShardData(args.Shard)

	// 状态切换：只有当请求的 Num 大于当前配置时才更新状态
	if args.Num > kv.configNum {
		kv.shardsState[args.Shard] = ShardFrozen
	}

	return shardrpc.FreezeShardReply{State: state, Num: args.Num, Err: rpc.OK}
}

func (kv *KVServer) applyDelete(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	if args.Num < kv.configNum {
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	}

	shardState, ok := kv.shardsState[args.Shard]

	if ok && shardState == ShardOwned {
		// 拒绝删除当前 Owned 的分片
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	}

	// 幂等性：如果已经是 Deleted，则允许重复操作。
	if ok && shardState == ShardDeleted {
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}

	// 执行删除
	delete(kv.ShardStorage, args.Shard)
	kv.shardsState[args.Shard] = ShardDeleted

	return shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) serializeShardData(shardId shardcfg.Tshid) []byte {
	shardData, ok := kv.ShardStorage[shardId]
	if !ok {
		// 如果没有数据，返回空 map 的序列化
		shardData = make(map[string]KVStore)
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(shardData); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) deserializeShardData(data []byte) map[string]KVStore {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ShardStorage map[string]KVStore
	if err := d.Decode(&ShardStorage); err != nil {
		log.Printf("Deserialize failed: %v", err)
		return make(map[string]KVStore)
	}
	return ShardStorage
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.configNum); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.shardsState); err != nil {
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
	var nextShardsState map[shardcfg.Tshid]ShardState
	var nextShardsData map[shardcfg.Tshid]map[string]KVStore

	if err := d.Decode(&nextConfigNum); err != nil {
		panic(err)
	}
	if err := d.Decode(&nextShardsState); err != nil {
		panic(err)
	}
	if err := d.Decode(&nextShardsData); err != nil {
		panic(err)
	}

	kv.configNum = nextConfigNum
	kv.shardsState = nextShardsState
	kv.ShardStorage = nextShardsData
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	//log.Printf("Get: %v", args)
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
	//log.Printf("Put: %v", args)
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
	err, res := kv.rsm.Submit(*args)
	//log.Printf("FreezeShard: %v", args)
	if err == rpc.ErrWrongGroup {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep, ok := res.(shardrpc.FreezeShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(*args)
	//log.Printf("InstallShard: %v", args)

	if err == rpc.ErrWrongGroup {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep, ok := res.(shardrpc.InstallShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	//log.Printf("DeleteShard: %v", args)
	err, res := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep, ok := res.(shardrpc.DeleteShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
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
	labgob.Register(ShardState(0))
	labgob.Register(map[string]KVStore{})
	labgob.Register(make(map[shardcfg.Tshid]map[string]KVStore))

	kv := &KVServer{
		gid:          gid,
		me:           me,
		configNum:    0,
		shardsState:  make(map[shardcfg.Tshid]ShardState),
		ShardStorage: make(map[shardcfg.Tshid]map[string]KVStore),
	}

	for i := 0; i < shardcfg.NShards; i++ {
		shardID := shardcfg.Tshid(i)
		if gid == shardcfg.Gid1 {
			// 默认第一个 Group 拥有所有分片
			kv.shardsState[shardID] = ShardOwned
			kv.ShardStorage[shardID] = make(map[string]KVStore)
		} else {
			// 其他 Group 默认不拥有任何分片
			kv.shardsState[shardID] = ShardDeleted
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
