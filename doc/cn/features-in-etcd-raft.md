# Features in etcd-raft

## Check Quorum

对于网络分区, 如果 `leader` 在少数节点的分区, 只有在接收到比自己大的 `term` 时才会转变为 `follower`, 这种情况下, 旧 `leader` 会一直认为自己当前集群的 `leader`, 接收客户端请求但是不做响应.

`CheckQuorum` 机制下, `leader` 每隔 `election_timeout_` 会检查是否集群中多数节点活跃, 如果不活跃, 主动降级为 `follower`.

通过对每个节点设置一个标签: `recent_active_`, 每次 `leader` 接收到 `follower` 的响应, 就将对应 `follower` 的标签其设为 `true`, 每隔 `election_timeout_` 会将所有 `follower` 的标签设为 `false`.

## Prevote

同样是网络分区, 同样是少数节点的分区, 该分区如果没有 `leader` 的话, 会一直发起选举, 导致该分区的 `term` 不断增大, 如果之后分区恢复, 多数节点分区的 `leader` 接收到少数节点分区的消息, 由于 `term` 比自身大, 会转变为 `follower`, 从而导致多余的一次选举.

`Prevote` 机制避免了这种情况, `follower` 在选举超时后, 转变为 `precandidate`, 此时不会增加本地的 `currentTerm`(但是发送 `kMsgPrevoteReq` 时, 会将 `Message` 中的 `term` 设为 `currentTerm`), 然后发起预选, 预选成功, 再转变为 `candidate`, 这种情况下, 说明自身不在少数节点分区, 就可以自增 `currentTerm` 了.

## Noop

如果选举成功后的一段时间没有新的请求到来, 也就是没有新的 `Entry`, 如果此时本地还有一部分 `Entry` 未 `commit`, 那么对于客户端的请求要过一段时间才能处理.

`candidate` 在选举成功变为 `leader` 后, 立刻构建一条空 `Entry`, 并且广播 `AppendEntry` 请求, 这样便于推动旧 `Entry` 的 `commit`.

## ReadIndex

如果对于读请求, 每次写入 `leader` 的 `log`, 并且达成多数节点共识再 `commit` 然后 `apply` 的话, 性能会下降.

可以考虑直接处理读请求(不需要记录日志, 也不需要复制), 但是如果不加限制的处理, 可能会出现数据不一致:

+ `follower` 与 `leader`, `follower` 与 `follower` 之间状态存在差异, 从不同节点读可能有不同结果.
+ 如果限定只从 `leader` 上读, `leader` 可能因为网络分区成为旧 `leader`(而他自己不知道), 返回的结果可能过时, 在新旧不同 `leader` 上读到的消息也不同.

为了解决以上问题, 需要保证 `linerizability` 语义, 需要一些额外措施:

+ `leader` 必须确保自己是集群 `leader`.
+ `leader` 只能向客户端返回 `applied` 之前的数据.

每个节点都有一个 `read_states_` 数组, 保存了 `ReadState`:

```c++
struct ReadState {
  uint64_t index; // 读请求产生时, 当前 leader 的 commitIndex
  std::string request_ctx; // 客户端生成的唯一 id.
};
```

这个数组会通过 `Ready()` 返回给上层模块最终响应读请求. `index` 指定了上层模块可以最多读取道哪一部分的 `Entry`.

`ReadOnlySafe` 模式下的只读请求处理不会受节点之间时钟差异和网络分区影响.

接收: 如果是 `follower` 接收到 `kMsgReadIndex` 消息, 如果当前有已知 `leader`, 会将其转发给 `leader`, 如果是 `leader` 接收到消息(不管是客户端发送来的还是 `follower` 转发的), 都会在 `StepLeader()` 中做处理.

对于单节点集群, `leader` 直接构建 `ReadState` 放入 `read_states_` 数组, 等待后续处理.

对于多节点集群, `leader` 在当前 `term` 没有 `commit` 过 `Entry` 的情况下, 将 `kMsgReadIndex` 消息放入 `pending_read_index_message_` 中缓存起来, 等到更新 `commitIndex` 时再做处理.

处理时, 首先将该消息构建为 `ReadIndexStatus`, 并放入 `pending_read_index_` 中, `ReadIndexStatus` 由三部分组成:

+ `Message req`: `kMsgReadIndex` 请求.
+ `uint64_t index`: `kMsgReadIndex` 请求到达时, `leader` 的 `commitIndex`.
+ `std::map<uint64_t, bool> acks`: `leader` 广播心跳后, 需要直到是否获取到多数节点应答, `acks` 就记录着每个节点的应答情况.

`kMsgReadIndex` 消息的第一个 `Entry` 的 `data` 存放着消息的 `id`, 通过 `id` 可以从 `pending_read_index_` 中取出特定的 `ReadIndexStatus`. 这个 `id` 也存放在 `read_index_queue_`, 可以将所有 `kMsgReadIndex` 消息按序排列.

更新完 `ReadOnly` 后, 需要广播心跳确保 `leader` 自身不过时, 广播时会将 `id` 作为 `ctx` 一并发送. 等到 `follower` 处理请求, 响应中也携带了 `ctx`, `leader` 处理响应时通过 `ctx` 中的 `id` 更新 `acks`, 等到 `acks` 为集群多数时, 通过 `id`, 遍历 `read_index_queue_`, 找到所有在这 `id` 之前的所有存放在 `pending_read_index` 中的 `ReadIndexStatus`, 构建 `kMsgReadIndex` 对应的响应 `kMsgReadIndexResp`, 如果 `kMsgReadIndex` 是客户端发送给 `leader` 的, `leader` 就将  `kReadIndexStatus` 对应的  `ReadState` 放入本地, 否则发送给 `follower`, `follower` 放入本地.
