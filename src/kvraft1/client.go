package kvraft

import (
	// 引入 rand 和 time

	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// 追踪 Leader 索引
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}

	// 初始化为随机server， 减少高并发时对同一个server造成 消息阻塞，第二增大获取到server的速度
	rand.NewSource(time.Now().UnixNano())
	ck.leaderId = rand.Intn(len(ck.servers))
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		server := ck.leaderId

		// 发送 RPC
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				return reply.Value, reply.Version, reply.Err
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				continue
			}
		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	for {
		reply := rpc.PutReply{}
		server := ck.leaderId

		// 发送 RPC
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				return reply.Err
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				continue
			}
		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(time.Millisecond)
	}
}
