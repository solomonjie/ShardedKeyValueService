package shardkv

//
// client code to talk to a sharded key/value service.
//
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	// 缓存最新的配置
	config    shardcfg.ShardConfig
	grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	// 查询初始配置：sck.Query() 返回 *shardcfg.ShardConfig
	ck.config = *ck.sck.Query()
	return ck
}

// Get a key from a shardgrp. You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		grpClerk := ck.GetClerk(key)
		// 2. 调用 shardgrp Clerk 的 Get 方法
		value, version, err := grpClerk.Get(key)

		if err == rpc.OK || err == rpc.ErrNoKey {
			return value, version, err
		} else if err == rpc.ErrWrongGroup || err == rpc.ErrWrongLeader {
			// 配置可能已变更或 Leader 已切换，重新查询配置并重试
			time.Sleep(10 * time.Millisecond)
			ck.config = *ck.sck.Query()
			continue
		} else {
			return value, version, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		grpClerk := ck.GetClerk(key)
		// 2. 调用 shardgrp Clerk 的 Put 方法
		err := grpClerk.Put(key, value, version)

		if err == rpc.OK || err == rpc.ErrVersion || err == rpc.ErrMaybe {
			return err
		} else if err == rpc.ErrWrongGroup || err == rpc.ErrWrongLeader {
			// 组已变更或 Leader 丢失，重新查询配置并重试
			time.Sleep(10 * time.Millisecond)
			ck.config = *ck.sck.Query()
			continue
		} else {
			return err
		}
	}
}

func (ck *Clerk) GetClerk(key string) *shardgrp.Clerk {
	for {
		// 1. 确定分片和目标组
		shardId := shardcfg.Key2Shard(key)
		// gid 是 tester.Tgid 类型，与 config.Shards 数组中的类型一致
		gid := ck.config.Shards[shardId]

		// 2. 找到服务器列表
		servers, ok := ck.config.Groups[gid]

		if !ok || gid == 0 {
			// 如果 Gid 为 0 (未分配) 或 Gid 不在 Groups map 中，查询配置并重试
			time.Sleep(100 * time.Millisecond)
			ck.config = *ck.sck.Query()
			continue
		}

		// 3. 获取或创建 shardgrp Clerk (使用缓存)
		grpClerk, ok := ck.grpClerks[gid] // gid 是 tester.Tgid，与 map 键类型一致
		if !ok {
			// 第一次访问这个 Gid，创建并缓存 Clerk
			grpClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.grpClerks[gid] = grpClerk
		}

		return grpClerk
	}
}
