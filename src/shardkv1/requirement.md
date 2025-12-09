###我们需要构建一个分片键值存储服务（Sharded Key/Value Service），它将键空间分割成多个分片（Shards），并由多个 Raft 复制的键值服务组（Shard Groups，简称 shardgrp）并行地处理这些分片。核心挑战在于在维护系统线性化（Linearizability）的同时，处理**配置（Configuration）**的动态变更。
核心组件及其职责
整个系统包含以下几个核心组件：
组件名称	        英文名称	     主要职责
键值存储客户端	     Clerk	        实现 Get 和 Put 操作，处理客户端请求。根据键获取分片信息，将请求路由到正确的 shardgrp。	KVSrv (获取配置), ShardGrp (发送 Get/Put)
配置服务	         KVSrv 	        存储和管理系统配置。配置定义了分片到 shardgrp 的映射关系。	Clerk (提供配置), Controller (接收配置更新)
分片服务组	         ShardGrp	    实际存储键值对，并通过 Raft 复制来保证数据一致性。每个组负责存储配置分配给它的几个分片。	Clerk (接收 Get/Put), Controller (接收分片迁移 RPC)
系统管理员/控制器    Controller	    管理系统的动态配置，包括添加/移除 shardgrp，以及调整分片到 shardgrp 的映射。实现核心方法 ChangeConfigTo.KVSrv (更新配置), ShardGrp (发送分片迁移 RPC)

###组件间的交互流程
##客户端读写操作 (Get/Put) 流程
这是最频繁的交互。目标是确保客户端请求被路由到当前拥有该键所在分片的 shardgrp。
    1. Clerk：客户端调用 Clerk 的 Get(key) 或 Put(key, value) 方法。
    2. 获取配置：Clerk 首先向 KVSrv 发起请求，获取最新的系统配置（Config）。
    3. 确定分片：Clerk 使用键（key）和配置中的哈希函数（例如，key % NShards，其中 NShards=10）来确定该键属于哪个分片 ID。
    4. 查找服务组：Clerk 根据配置中的分片 ID -> shardgrp ID 映射，找到负责该分片的 shardgrp。
    5. 发送请求：Clerk 向目标 shardgrp 发送实际的 Get/Put RPC。
    6. ShardGrp 处理：ShardGrp 接收请求，通过 Raft 提交操作，执行读写操作，并返回结果。

##配置变更操作 (ChangeConfigTo) 流程这是系统动态性所在的核心流程，由 Controller 发起。目标是安全、原子地将系统从旧配置（Config $N$）迁移到新配置（Config $N+1$），确保在迁移过程中线性化不被破坏。
    1. Controller 发起：系统管理员（或测试器）通过 Controller 调用 ChangeConfigTo(NewConfig)。
    2. 锁定旧分片：Controller 确定哪些分片需要从旧的 shardgrp 移动到新的 shardgrp。
    3. 分片迁移 RPCs：对于需要迁移的分片：
        a. Controller 向旧的 shardgrp 发送 FreezeShard 或类似 RPC，要求其停止处理对这些分片的客户端请求。
        b. Controller 向旧的 shardgrp 发送 InstallShard 或类似 RPC，要求其将分片数据发送给新的 shardgrp。
        c. 新的 shardgrp 接收并存储分片数据。
        d. Controller 向旧的 shardgrp 发送 DeleteShard 或类似 RPC，要求其删除已迁移的分片数据。
    4. 原子更新配置：所有分片都成功迁移（或确定不再由旧组服务，而开始由新组服务）后，Controller 向 KVSrv 发送 RPC，原子性地将配置从 Config $N$ 更新为 Config $N+1$。
    5. 线性化保证：通过在 Config $N$ 和 Config $N+1$ 之间引入一个过渡状态（例如，分片仅在 Config $N+1$ 生效后才对外服务），确保在任何时间点，每个分片只由一个 shardgrp 服务。

##配置服务交互 (KVSrv 与 Controller / Clerk)
KVSrv 充当了配置信息的单一事实来源（Single Source of Truth），并且必须通过 Raft 保证自身的高可用性和一致性。
    1. Clerk -> KVSrv：查询当前最新的配置。
    2. Controller -> KVSrv：提交新的配置，通常通过 KVSrv 的 Raft 复制机制来确保配置更新的一致性。

这里需要注意的是
1. 每一个shardgrp 都是一组服务器构成，它们运行的RSM和底层的raft协议
2. 在config 中配置的是类似于 shardgrp ID与一组服务器的IP的关系，而这一个shardgrp中的leader由它们自己选举产生
3. 当Client 尝试与shardgrp进行交互时，向其中一台server发送请求，如果时这台服务器是leader，直接接受日志并复制到follower中，如果不是，则直接拒绝并告诉client正确的leader。

在 Part A 中，我们将专注于实现 shardctrler 和基础的 shardgrp 功能。
1. shardctrler 实现：
   a. 这将是您要实现的组件，它通过与 KVSrv 交互来实现 Controller 的功能。它需要定义配置的结构，并实现 ChangeConfigTo 的逻辑。
   b. 配置结构：需要定义一个包含配置号（ConfigNum）、shardgrp 列表（GID -> 服务器列表）和分片映射（ShardID -> GID）的数据结构。

2. shardgrp 基础实现：
   a. 将您的 Raft 库（Lab 2 的 rsm）集成到 shardgrp 服务器中。
   b. shardgrp 必须能够接收并处理 Get 和 Put 操作（作为 Raft 提交的命令）。
   c. 需要实现 shardgrp 间的分片迁移 RPC（InstallShard, DeleteShard 等），以及配套的 shardgrp Clerk。