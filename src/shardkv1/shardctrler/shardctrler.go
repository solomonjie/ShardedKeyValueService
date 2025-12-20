package shardctrler

import (
	"log"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp" // 引入 shardgrp
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// 内部状态
	configKey string

	// 新增：用于缓存连接到 Shard Group 的客户端 (实现 ChangeConfigTo 所需)
	clerkMu   sync.Mutex
	grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)

	sck.configKey = "latest_config"
	sck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	configString := cfg.String()

	err := sck.IKVClerk.Put(sck.configKey, configString, rpc.Tversion(0))

	if err != rpc.OK {
		log.Fatalf("InitConfig: Put failed with error: %v. Config: %v", err, configString)
	}
}

// Return the current configuration (保留你原有的正确逻辑)
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	configString, _, err := sck.IKVClerk.Get(sck.configKey)

	if err != rpc.OK {
		if err == rpc.ErrNoKey {
			return shardcfg.MakeShardConfig() // 处理未初始化配置的情况
		}

		log.Fatalf("Query: Get failed with error: %v", err)
	}
	return shardcfg.FromString(configString) // 使用了正确的 FromString
}

// getGrpClerk returns or creates a shardgrp.Clerk for the given GID.
func (sck *ShardCtrler) getGrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	sck.clerkMu.Lock()
	defer sck.clerkMu.Unlock()

	// 检查缓存
	if clerk, ok := sck.grpClerks[gid]; ok {
		return clerk
	}

	// 创建新客户端
	clerk := shardgrp.MakeClerk(sck.clnt, servers)
	sck.grpClerks[gid] = clerk
	return clerk
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		// 1. 获取当前配置
		current := sck.Query()

		//nextConfigNum := current.Num + 1
		if new.Num <= current.Num {
			// 配置已完成或有更新的配置。
			return
		}

		//log.Printf("ShardCtrler: Starting migration from config %d to config %d", current.Num, nextConfigNum)

		// 1.2. 找出需要迁移的分片
		shardsToMove := make(map[shardcfg.Tshid]struct {
			SrcGid tester.Tgid
			DstGid tester.Tgid
		})

		for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
			srcGid := current.Shards[s]
			dstGid := new.Shards[s]

			if srcGid != dstGid {
				shardsToMove[s] = struct {
					SrcGid tester.Tgid
					DstGid tester.Tgid
				}{
					SrcGid: srcGid,
					DstGid: dstGid,
				}
			}
		}

		migrationSuccessful := true

		// 2. 第一阶段：Freeze and Install (冻结并安装)

		// 2.1 Freeze & Get Data from Source Groups (从源 Group 冻结并获取数据)
		frozenShardData := make(map[shardcfg.Tshid][]byte)

		for shard, move := range shardsToMove {
			if move.SrcGid != 0 {
				servers, ok := current.Groups[move.SrcGid]
				if !ok {
					// 源 Group 不存在 (已 Leave)，假定数据已在目标 Group 中
					frozenShardData[shard] = []byte{}
					continue
				}

				grpClerk := sck.getGrpClerk(move.SrcGid, servers)

				// 调用 FreezeShard (使用新配置号)
				state, err := grpClerk.FreezeShard(shard, new.Num)

				if err != rpc.OK {
					// 遇到网络错误、WrongLeader 或 ErrWrongGroup
					//log.Printf("ShardCtrler: FreezeShard for shard %d on GID %d failed: %v. Retrying...", shard, move.SrcGid, err)
					migrationSuccessful = false
					break
				}

				frozenShardData[shard] = state
				//log.Printf("ShardCtrler: Shard %d successfully FROZEN on GID %d (data size: %d)", shard, move.SrcGid, len(state))
			}
		}

		if !migrationSuccessful {
			time.Sleep(100 * time.Millisecond)
			continue // 重试整个 ChangeConfigTo
		}

		// 2.2 Install Data to Destination Groups (向目标 Group 安装数据)
		for shard, move := range shardsToMove {
			if move.DstGid != 0 {
				servers, ok := new.Groups[move.DstGid]
				if !ok {
					// 目标 Group 不存在，新配置无效
					//log.Fatalf("ShardCtrler.ChangeConfigTo: DstGid %d not found in new config %d", move.DstGid, new.Num)
					continue
				}

				grpClerk := sck.getGrpClerk(move.DstGid, servers)

				state, ok := frozenShardData[shard]
				if !ok && move.SrcGid != 0 {
					//log.Fatalf("ShardCtrler: Internal error, missing frozen data for shard %d", shard)
				}
				if !ok {
					state = []byte{} // Gid 0 -> 目标 Gid 的情况
				}

				// 调用 InstallShard (使用新配置号)
				err := grpClerk.InstallShard(shard, state, new.Num)

				if err != rpc.OK {
					// 遇到网络错误、WrongLeader 或 ErrWrongGroup
					//log.Printf("ShardCtrler: InstallShard for shard %d on GID %d failed: %v. Retrying...", shard, move.DstGid, err)
					migrationSuccessful = false
					break
				}
				//log.Printf("ShardCtrler: Shard %d successfully INSTALLED on GID %d", shard, move.DstGid)
			}
		}

		if !migrationSuccessful {
			time.Sleep(100 * time.Millisecond)
			continue // 重试整个 ChangeConfigTo
		}

		// 3. 发布新配置 (原子性 Put)

		configToPublish := *new // 拷贝新的配置
		//configToPublish.Num = nextConfigNum

		configString := configToPublish.String()

		// 使用当前配置号作为版本号 (current.Num)，原子性地写入新配置。
		err := sck.IKVClerk.Put(sck.configKey, configString, rpc.Tversion(current.Num))

		if err != rpc.OK {
			// Put 失败 (ErrVersion 或网络错误)，返回循环开头重新 Query
			log.Printf("ShardCtrler: Failed to publish config %d (version %d), retrying: %v", new.Num, current.Num, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		//log.Printf("ShardCtrler: Config %d SUCCESSFULLY PUBLISHED", nextConfigNum)

		// 4. 清理阶段：从源 Group 中删除 Shard 数据
		// 即使失败也不影响配置的提交，可以忽略错误。
		for shard, move := range shardsToMove {
			if move.SrcGid != 0 {
				servers, ok := current.Groups[move.SrcGid]
				if !ok {
					continue
				}

				grpClerk := sck.getGrpClerk(move.SrcGid, servers)

				// 调用 DeleteShard (使用新的配置号进行清理)
				err := grpClerk.DeleteShard(shard, new.Num)
				if err != rpc.OK && err != rpc.ErrWrongGroup {
					//log.Printf("ShardCtrler: Cleanup DeleteShard for shard %d on GID %d failed: %v (Ignored)", shard, move.SrcGid, err)
				}
			}
		}

		// 配置变更完成
		//log.Printf("ShardCtrler: Configuration change to %d fully completed.", nextConfigNum)
		return
	}
}
