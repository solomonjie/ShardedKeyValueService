package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVStore struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu sync.Mutex
	db map[string]KVStore
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := req.(type) {
	case rpc.GetArgs:
		if item, ok := kv.db[op.Key]; ok {
			return rpc.GetReply{Value: item.Value, Version: item.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:
		item, ok := kv.db[op.Key]
		if ok && item.Version != op.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		newVersion := op.Version + 1
		kv.db[op.Key] = KVStore{
			Value:   op.Value,
			Version: newVersion,
		}

		return rpc.PutReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// 1. 加锁：保护共享的数据库状态
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 2. 序列化数据库状态
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 编码 kv.db (map[string]KVStore)
	if err := e.Encode(kv.db); err != nil {
		// 在实际生产环境中，这里应该进行更详细的错误处理和日志记录
		panic(err)
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// 1. 检查快照数据：如果 data 为空，则无需恢复（或使用空状态）
	if data == nil || len(data) < 1 {
		return
	}

	// 2. 加锁：保护共享的数据库状态
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 3. 反序列化
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// 注意：这里我们使用一个新的 map 来接收反序列化的数据，然后替换旧的 kv.db
	var nextDb map[string]KVStore
	if err := d.Decode(&nextDb); err != nil {
		// 在实际生产环境中，这里应该进行更详细的错误处理和日志记录
		panic(err)
	}

	// 4. 替换当前数据库状态
	kv.db = nextDb
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,
		db: make(map[string]KVStore),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
