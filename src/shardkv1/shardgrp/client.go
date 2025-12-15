package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	isFirstTry := true
	for {
		reply := rpc.GetReply{}
		server := ck.leaderId

		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)

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
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
