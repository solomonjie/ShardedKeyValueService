任务一状态机的实现，下面几个点需要特别理清
1. RSM 是单独的一层中间件
   位置：它夹在“上层应用程序”（如 KV Server）和“底层共识”（Raft）之间。
   作用：
   向下：它负责处理与 Raft 复杂的交互（调用 Start，监听 applyCh，处理快照）。
   向上：它给应用程序提供极其简单的接口 Submit(command)。应用程序不需要关心 Raft 的 Term、Index、心跳等细节，只需要告诉 RSM：“我要执行这个命令”，然后等结果。

2. 机器数目与 Raft 强绑定（1对1）
   物理部署：通常我们将“KV 服务 + RSM 代码 + Raft 代码”编译在同一个二进制程序里运行。
   数量一致：如果有 5 台物理服务器，那么就有：
   5 个 KV 服务实例
   5 个 RSM 实例
   5 个 Raft 实例
   它们在同一个进程的内存空间内，通过函数调用（Function Call）和 Go Channel 交互。

3. 状态机的 Leader 就是 Raft 的 Leader
   应用程序（状态机）本身通常是无状态或对等的。但是，因为只有 Raft 的 Leader 才能成功调用 Start() 处理写请求，所以只有运行在 Raft Leader 节点上的那个应用程序实例，才能真正作为服务   的 Leader 接收客户端请求。
   如果一个节点的 Raft 是 Follower，调用 RSM.Submit -> Raft.Start 会立刻失败（返回 ErrWrongLeader），迫使客户端去寻找真正的 Leader。

4. 状态机之间的交互全靠 Raft 消息
   无直接通信：Server 1 的状态机绝对不会直接发 TCP/RPC 给 Server 2 的状态机说“把 x 加 1”。
   日志即真理：
   Server 1（Leader）收到请求，通过 Raft 达成共识。
   Raft 将操作写入日志，并复制给 Server 2, 3, 4, 5。
   Server 2 的 Raft 通知 Server 2 的 RSM：“收到一个新日志（x+1）”。
   Server 2 的 RSM 执行该逻辑。
   通过这种方式，所有节点虽然不直接对话，但只要按相同的顺序执行 Raft 传过来的相同的日志，大家的状态（内存里的数据）就永远保持一致。

下面是示意图
+-------------------------------------------------------+
|  Client (Clerk)                                       |
+---------------------------+---------------------------+
                            | RPC (Put/Get)
                            v
+-------------------------------------------------------+
|  [Server 节点 (比如 server.go)]                        |
|                                                       |
|  1. 应用程序 (StateMachine)                            |
|     - 实现 DoOp() 真正干活 (比如 map[key]=value)        |
|     - 调用 rsm.Submit() 发送请求                       |
|                                                       |
|  +-------------------------------------------------+  |
|  | 2. RSM 层 (rsm.go)                                 |
|  |    - Submit(): 封装 Op, 调 Raft, 等待结果           |
|  |    - Loop: 读 applyCh, 调 DoOp(), 通知 Submit       |
|  +------------------------+------------------------+  |
|                           |                           |
|  +------------------------v------------------------+  |
|  | 3. Raft 层 (raft.go)                               |
|  |    - Start(), applyCh                              |
|  +-------------------------------------------------+  |
+-------------------------------------------------------+

任务 B 整体工作流程分析
任务 B 要求我们在 rsm 包之上构建一个容错的键值服务 (KVServer)，并设计一个具有重试机制的客户端 (Clerk) 来与它交互。
1. KVServer 的职责 (server.go)
KVServer 是连接客户端和 Raft 状态机的枢纽。
a. RPC 处理器：接收客户端的 Get() 和 Put() RPC 请求。
b. 命令提交：将这些请求封装成 Raft Log Entry，通过 rsm.Submit() 提交给底层的 Raft 组。
c. 状态机执行：实现 DoOp(any) 方法。这是 Raft 提交命令后，rsm.applier 协程调用的方法。它负责将操作应用到 KVServer 内部的键值数据库。
d. 结果返回：在 Submit() 返回成功后，将结果返回给客户端。

2. Clerk 的职责 (client.go)
Clerk 是客户端逻辑的实现者。
a. Leader 查找：它必须能够找到当前的 Raft Leader。
b. 请求重试：如果 RPC 失败、超时，或收到 rpc.ErrWrongLeader，Clerk 必须向其他服务器重试。
c. 线性一致性：确保每个操作（尤其是 Put）只会被执行一次，即使在 Leader 切换时需要重试。

任务 B 第一部分：基础功能（无故障）目标是实现基本的 KV 读写功能，确保在无网络丢包和无服务器宕机的情况下，TestBasic4B 能通过。
关键实现点：
KVServer (RPC)Get/Put 处理器接收 RPC -> 封装为 Op -> 调用 rsm.Submit(op) -> 等待结果并返回。
KVServer (DoOp)状态机应用接收 Op -> 解析请求 (req.Type) -> 读写内部 K/V Map -> 返回结果。
Clerk 简单Leader 查找循环向服务器发送请求，直到找到 Leader 并成功提交。
一致性 读操作处理提示 所有 Get() 操作也必须通过 rsm.Submit() 进入 Raft Log。这保证了 Get 操作只会在 Leader 提交日志后执行，从而避免读取到旧数据（Stale Reads）。
并发安全锁提示 对 KVServer 内部的键值数据库和状态变量（如 counter）访问时，必须使用锁 (sync.Mutex)。

任务 B 第二部分：容错与线性一致性
目标是处理网络分区、服务器宕机和 Leader 切换，并确保 “每个 Put 操作只执行一次” 的**恰好一次（Exactly-Once）**语义。

关键挑战：恰好一次语义（Exactly-Once Semantics）
当 Leader L1 提交了一个 Put(X) 命令到 Raft Log，但在返回结果给 Clerk 之前崩溃了。Clerk 超时后会向新 Leader L2 重试 Put(X)。
问题：如果 L2 简单地将 Put(X) 再次提交，那么 Put(X) 就会被执行两次，破坏了恰好一次语义。
解决思路：去重（Idempotency）。

解决方案：去重机制
Clerk 端：Clerk 必须为每个操作（Put/Get）生成一个全局唯一的 ClientId 和一个递增的 SequenceId（或版本号）。
Op 结构体必须包含 ClientId 和 SequenceId。
KVServer 端（状态机）：每个 KVServer 必须维护一个去重表（例如 map[ClientId]SequenceId 或 map[ClientId]OpResult）。
在 DoOp 中，检查当前操作的 SequenceId 是否已经被执行：
SequenceId <= LastExecutedId：该操作是重试的，直接返回之前缓存的结果，不执行 K/V Map 操作。
SequenceId == LastExecutedId + 1：新的操作，执行 K/V Map 操作，更新 LastExecutedId，并缓存结果。

其他关键挑战：Leader 切换与优化
1. Leader 切换处理 (提示 1)
当 rsm.Submit() 返回 rpc.ErrWrongLeader 时，Clerk 必须立即停止向当前服务器发送请求。
Clerk 必须切换到列表中的下一个服务器进行重试，直到找到新的 Leader。

2. 优化 Leader 发现 (提示 2)
Clerk 应该记录上一次哪个服务器是 Leader (lastLeaderId)。
在发起新的 RPC 时，Clerk 应该首先尝试连接 lastLeaderId。这极大地减少了在稳定状态下寻找 Leader 的延迟。
如果 lastLeaderId 失败，Clerk 再依次尝试其他服务器。

3. ErrMaybe 处理
如果 Clerk 重试 Put 时，收到了新的 Leader 的响应，但不能确定旧 Leader 是否成功执行了原请求（例如，旧 Leader 可能提交成功但返回失败）。
最安全做法：由于引入了去重机制，新 Leader 收到重试请求并将其提交后：
如果原请求已经执行，新 Leader 会返回缓存的结果。
如果原请求未执行，新 Leader 会执行它。
在这个特定任务中，由于我们引入了去重机制，通常不需要像 Lab 2 那样返回 ErrMaybe（因为去重保证了最终状态的正确性）。但是，如果测试要求在无法确知旧 Leader 响应的情况下返回 ErrMaybe，则 Clerk 需要实现相应的逻辑。但在 KV-Raft 结构中，如果命令最终在 Leader L2 上提交，它要么被执行（新），要么被去重（旧），因此 ErrMaybe 更多是 Lab 2 的遗留要求。我们应该优先关注去重。