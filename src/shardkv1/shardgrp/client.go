package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	rand.NewSource(time.Now().UnixNano())
	ck.leaderId = rand.Intn(len(ck.servers))
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
		//log.Printf("Submit Request %T %v Reply: %v", args, args, reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				return reply.Value, reply.Version, reply.Err
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				continue
			}

			// 客户端收到 ErrWrongGroup，让上层 ShardKV Client 处理（查询新配置）
			if reply.Err == rpc.ErrWrongGroup {
				return "", 0, reply.Err
			}
		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	isFirstTry := true
	for {
		reply := rpc.GetReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)
		//log.Printf("Submit Request %T %v Reply: %v", args, args, reply)

		if ok {
			if reply.Err == rpc.OK {
				return reply.Err
			}

			if reply.Err == rpc.ErrVersion {
				if isFirstTry {
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				isFirstTry = false
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}

			ck.leaderId = (server + 1) % len(ck.servers)
			isFirstTry = false

		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
			isFirstTry = false
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		reply := shardrpc.FreezeShardReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
		//log.Printf("Submit Request %T %v Reply: %v", args, args, reply.Err)

		if ok {
			if reply.Err == rpc.OK {
				return reply.State, rpc.OK
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// 收到其他错误，返回给 ShardCtrler
			return nil, reply.Err
		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}

	for {
		reply := shardrpc.InstallShardReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)
		//log.Printf("Submit Request %T Num %v - Shard %v Reply: %v", args, args.Num, args.Shard, reply)

		if ok {
			if reply.Err == rpc.OK {
				// 成功安装
				return rpc.OK
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// 收到其他错误，返回给 ShardCtrler
			return reply.Err
		} else {
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		reply := shardrpc.DeleteShardReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
		//log.Printf("Submit Request %T %v Reply: %v", args, args, reply)

		if ok {
			if reply.Err == rpc.OK {
				// 成功删除
				return rpc.OK
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.leaderId = (server + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// 收到其他错误，返回给 ShardCtrler
			return reply.Err
		} else {
			// RPC 失败 (网络错误)，切换 Leader
			ck.leaderId = (server + 1) % len(ck.servers)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
