# PostgreSQL 版本分阶段实现计划

本文档描述 MarketDataBackend 第一版 PostgreSQL-only 实现路线。

第一版目标不是一次性完成所有存储后端，而是先跑通一条完整链路：

```text
创建 MarketGroup / InputStream
runtime 注册并抢占 lease
消费 Kafka
幂等写入 PostgreSQL
成功后 commit offset
上报 stream_runtime_status
通过 API 查询状态和行情数据
```

## 1. 总体原则

### 1.1 技术选择

第一版建议使用：

```text
Go
PostgreSQL
pgx / pgxpool
golang-migrate 或 goose
手写 SQL
标准库 testing
testcontainers-go 或 docker compose 集成测试
```

暂不引入：

```text
ClickHouse adapter
TimescaleDB 专用 hypertable
sqlc
复杂查询 DSL
图形化管理界面
```

原因：

```text
当前数据模型仍在收敛，手写 SQL 更方便调整
PostgreSQL 可以同时承载 metadata、lease 和行情事实表
先跑通完整链路，再决定是否将高吞吐事实数据迁移到 ClickHouse
```

### 1.2 代码结构目标

建议第一版形成以下目录：

```text
cmd/
  md-control-plane/
  md-stream-runtime/

internal/
  api/
  config/
  db/
  kafka/
  metadata/
  model/
  runtime/
  storage/
```

### 1.3 测试分层

测试按三层组织：

```text
unit tests:
  不依赖外部服务，测试模型、状态转换、SQL 构造辅助逻辑

postgres integration tests:
  启动临时 PostgreSQL，验证 migration、约束、事务和幂等写入

runtime integration tests:
  使用 fake Kafka 或嵌入式测试组件，验证 lease、消费、写入、commit、状态上报的完整行为
```

## 2. M0：项目骨架和基础设施

### 2.1 功能范围

建立最小可运行工程骨架：

```text
cmd/md-control-plane/main.go
cmd/md-stream-runtime/main.go
internal/config
internal/db
internal/model
internal/metadata
internal/storage
```

实现：

```text
配置加载
PostgreSQL DSN 配置
pgxpool 初始化
应用启动和 graceful shutdown 基础逻辑
统一 logger
```

### 2.2 测试范围

```text
配置默认值测试
环境变量覆盖测试
无效 DSN / 缺失配置时的错误测试
db pool 初始化失败路径测试
```

### 2.3 完成标准

```text
go test ./... 通过
两个 main 包可以启动并正常退出
没有业务逻辑，但具备连接 PostgreSQL 的能力
```

## 3. M1：PostgreSQL Migration 和表结构

### 3.1 功能范围

新增 migration 目录：

```text
migrations/
  000001_init_metadata.up.sql
  000001_init_metadata.down.sql
  000002_init_market_data.up.sql
  000002_init_market_data.down.sql
```

实现 metadata 表：

```text
market_groups
group_inputs
runtime_nodes
market_leases
stream_runtime_status
```

实现行情事实表：

```text
trades
klines
orderbook_deltas
orderbook_snapshots
```

PostgreSQL 类型建议：

```text
price / quantity / volume 使用 numeric
时间字段使用 timestamptz
bids / asks 使用 jsonb
状态字段第一版使用 text + CHECK 约束
```

关键唯一约束：

```sql
UNIQUE (exchange, market_type, symbol)
UNIQUE (group_id, stream_key)
UNIQUE (input_id, raw_trade_id) WHERE raw_trade_id IS NOT NULL
UNIQUE (input_id, kafka_partition, kafka_offset)
UNIQUE (input_id, raw_event_id) WHERE raw_event_id IS NOT NULL
UNIQUE (group_id, source, interval, open_time)
UNIQUE (input_id, snapshot_time)
```

### 3.2 测试范围

PostgreSQL 集成测试：

```text
所有 migration up 可以从空库执行成功
所有 migration down 可以回滚
重复执行唯一键数据会失败或触发预期冲突
CHECK 约束可以阻止非法状态
jsonb bids / asks 可以正确写入和读出
```

### 3.3 完成标准

```text
本地可一键创建完整 PostgreSQL schema
migration 测试覆盖所有核心表
核心唯一键和幂等约束已经存在
```

## 4. M2：MetadataStore

### 4.1 功能范围

实现 PostgreSQL MetadataStore：

```go
type MetadataStore interface {
    CreateGroup(ctx context.Context, g MarketGroup) error
    UpdateGroupDesiredStatus(ctx context.Context, groupID string, status string) error
    ListRunnableGroups(ctx context.Context) ([]MarketGroup, error)

    RegisterRuntimeNode(ctx context.Context, node RuntimeNode) error
    HeartbeatRuntimeNode(ctx context.Context, nodeID string, capacity RuntimeCapacity) error

    TryAcquireGroupLease(ctx context.Context, groupID string, nodeID string, ttl time.Duration) (bool, error)
    RenewGroupLease(ctx context.Context, groupID string, nodeID string, ttl time.Duration) (bool, error)
    ReleaseGroupLease(ctx context.Context, groupID string, nodeID string) error

    ListGroupInputs(ctx context.Context, groupID string) ([]GroupInput, error)
    ReportStreamRuntimeStatus(ctx context.Context, status StreamRuntimeStatus) error
}
```

重点实现：

```text
CreateGroup 同时创建 group_inputs
desired_status 更新
runtime node upsert
heartbeat 更新 last_heartbeat_at
lease 抢占
lease 续租
lease 释放
stream_runtime_status upsert
```

lease 语义：

```text
无 lease 时可以抢占
lease 已过期时可以抢占
lease 未过期且 owner 是自己时可以续租
lease 未过期且 owner 是其他 node 时抢占失败
释放 lease 时必须校验 node_id
```

### 4.2 测试范围

单元测试：

```text
状态枚举校验
group_id / input_id 生成规则
stream_key 和 stream_kind / interval 的映射
```

PostgreSQL 集成测试：

```text
CreateGroup 正确写入 market_groups 和 group_inputs
重复创建相同 exchange + market_type + symbol 返回冲突错误
UpdateGroupDesiredStatus 正确更新 updated_at
ListRunnableGroups 只返回 desired_status = running 的 group
RegisterRuntimeNode 可重复调用并更新字段
HeartbeatRuntimeNode 更新心跳和容量
```

lease 并发测试：

```text
两个 node 同时抢同一个 group，最多一个成功
owner 可以续租
非 owner 续租失败
lease 过期后其他 node 可以抢占
非 owner release 不会删除 lease
owner release 可以释放 lease
```

stream status 测试：

```text
第一次 ReportStreamRuntimeStatus 插入
后续 ReportStreamRuntimeStatus 更新
kafka lag / offset / last_error 可以被覆盖
```

### 4.3 完成标准

```text
MetadataStore 具备完整控制面能力
lease 通过并发集成测试
runtime 状态可以被可靠上报和查询
```

## 5. M3：PostgresStorage 幂等写入

### 5.1 功能范围

实现 PostgreSQL MarketDataStorage：

```go
type MarketDataStorage interface {
    WriteTrades(ctx context.Context, rows []Trade) error
    WriteKlines(ctx context.Context, rows []Kline) error
    WriteOrderBookDeltas(ctx context.Context, rows []OrderBookDelta) error
    WriteOrderBookSnapshots(ctx context.Context, rows []OrderBookSnapshot) error

    WriteTradeMetrics(ctx context.Context, rows []TradeMetric) error
    WriteBookMetrics(ctx context.Context, rows []BookMetric) error
    WriteCrossMetrics(ctx context.Context, rows []CrossMetric) error

    Close() error
}
```

第一版先实现事实表写入：

```text
WriteTrades
WriteKlines
WriteOrderBookDeltas
WriteOrderBookSnapshots
```

指标写入可以先保留接口，返回明确的 not implemented，等 M9 实现。

写入策略：

```text
trades:
  INSERT ... ON CONFLICT DO NOTHING

orderbook_deltas:
  INSERT ... ON CONFLICT DO NOTHING

klines:
  INSERT ... ON CONFLICT DO UPDATE
  用于更新未闭合 K 线

orderbook_snapshots:
  INSERT ... ON CONFLICT DO NOTHING
```

### 5.2 测试范围

PostgreSQL 集成测试：

```text
WriteTrades 批量写入成功
重复 raw_trade_id 不产生重复行
重复 kafka offset 不产生重复行
raw_trade_id 为空时仍可通过 kafka offset 幂等

WriteKlines 首次插入成功
未闭合 K 线重复写入会更新 OHLCV
已闭合 K 线重复写入符合预期策略

WriteOrderBookDeltas 批量写入成功
重复 raw_event_id 不产生重复行
重复 kafka offset 不产生重复行
sequence 字段可以为空或按交易所写入

WriteOrderBookSnapshots 写入 bids / asks jsonb
重复 input_id + snapshot_time 不产生重复行
bids 降序、asks 升序的校验在应用层测试覆盖
```

错误路径测试：

```text
非法 numeric 返回错误
非法 jsonb 返回错误
context canceled 时写入中断
空 slice 写入直接返回 nil
```

### 5.3 完成标准

```text
行情事实表支持 at-least-once 消费下的重复写入
所有事实写入都有幂等集成测试
批量写入接口稳定
```

## 6. M4：Control Plane REST API

### 6.1 功能范围

实现管理 API：

```text
POST /groups
GET /groups
GET /groups/{group_id}

POST /groups/{group_id}/pause
POST /groups/{group_id}/resume

POST /groups/{group_id}/inputs
GET /groups/{group_id}/inputs
POST /groups/{group_id}/inputs/{stream_key}/pause
POST /groups/{group_id}/inputs/{stream_key}/resume
```

请求模型使用：

```text
stream_key
stream_kind
interval
kafka_topic
kafka_group_id
```

返回模型合并：

```text
market_groups desired_status
market_leases owner 信息
stream_runtime_status actual_status / lag / offset / last_error
```

### 6.2 测试范围

HTTP handler 测试：

```text
创建 group 成功
创建重复 group 返回 409
非法 market_type 返回 400
非法 stream_kind / interval 返回 400
暂停 / 恢复 group 更新 desired_status
暂停 / 恢复 input 更新 desired_status
查询 group 返回 inputs 和 runtime status
不存在的 group 返回 404
```

PostgreSQL 集成测试：

```text
API 写入的数据能被 MetadataStore 读出
runtime status 存在时 GET /groups/{group_id} 正确聚合状态
无 runtime status 时返回 pending / unknown 状态
```

### 6.3 完成标准

```text
控制面 API 可以创建、暂停、恢复和查询 MarketGroup / InputStream
API 返回能解释 desired_status 和 actual_status 的差异
```

## 7. M5：Runtime Lease Loop

### 7.1 功能范围

实现 md-stream-runtime 的基础调度循环：

```text
启动时 RegisterRuntimeNode
周期性 HeartbeatRuntimeNode
周期性 ListRunnableGroups
对可运行 group TryAcquireGroupLease
抢到 lease 后启动 MarketGroupWorker
MarketGroupWorker 周期性 RenewGroupLease
续租失败后停止 worker
进程退出时 ReleaseGroupLease
```

此阶段 worker 可以先不消费 Kafka，只记录日志并上报 status。

### 7.2 测试范围

单元测试：

```text
worker 状态机：starting / running / stopping / stopped / error
reconcile 计算：哪些 group 需要启动，哪些 group 需要停止
```

PostgreSQL 集成测试：

```text
单 runtime 可以抢到 runnable group
两个 runtime 同时运行时同一个 group 只有一个 owner
owner 心跳和续租正常更新
owner 停止续租后，lease 过期，另一个 runtime 可以接管
desired_status = paused 时 runtime 停止 worker
```

### 7.3 完成标准

```text
多 runtime 实例不会同时处理同一个 MarketGroup
lease 过期接管流程可通过测试复现
不接 Kafka 的情况下，runtime 调度闭环已经可用
```

## 8. M6：Kafka Trade 消费到 PostgreSQL

### 8.1 功能范围

先只支持 trade 输入流。

实现：

```text
Kafka consumer wrapper
手动 commit offset
trade message decode
schema validate
转换为 Trade model
调用 WriteTrades
写入成功后 commit offset
写入失败不 commit
周期性上报 stream_runtime_status
input desired_status = paused 时暂停消费
lease 丢失时停止消费
```

### 8.2 测试范围

单元测试：

```text
trade message decode
trade 字段校验
side 枚举校验
raw_trade_id 缺失时的兜底逻辑
```

runtime 集成测试：

```text
收到 trade 后写入 PostgreSQL
写入成功后 commit offset
写入失败时不 commit offset
重复消费同一 offset 不产生重复 trade
重复 raw_trade_id 不产生重复 trade
暂停 input 后停止消费
恢复 input 后从 committed offset 继续消费
```

状态上报测试：

```text
last_event_time 更新
last_processed_time 更新
committed_offset 更新
kafka_lag 更新
decode error 写入 last_error
```

### 8.3 完成标准

```text
trade 从 Kafka 到 PostgreSQL 的链路跑通
at-least-once 重复消费不会造成重复数据
控制面可以看到 trade stream 的实际状态和 lag
```

## 9. M7：Kline 和 OrderBook Delta 消费

### 9.1 功能范围

在已有 InputWorker 框架上增加：

```text
kline stream
orderbook_delta stream
```

kline：

```text
支持 exchange kline 写入
支持未闭合 K 线更新
支持 is_closed 字段
支持 source = exchange
```

orderbook_delta：

```text
decode bids / asks changes
保留标准化 Kafka 消息中的完整 bids / asks 变化列表，不做 top N 或价格距离截断
保存 raw_event_id
保存 first_update_id / last_update_id / prev_update_id / sequence
执行基础 sequence 连续性校验
写入 orderbook_deltas
```

此阶段可以先不维护完整内存订单簿，只保存 delta 并校验 sequence。

### 9.2 测试范围

kline 测试：

```text
kline decode 成功
未闭合 K 线重复消息更新同一行
闭合 K 线写入后可查询
interval 和 stream_key 匹配校验
```

orderbook delta 测试：

```text
delta decode 成功
bids / asks changes 写入 jsonb
大量档位不会被截断或采样
sequence 连续时通过
sequence 断裂时上报错误状态
重复 raw_event_id 不产生重复行
重复 kafka offset 不产生重复行
```

runtime 测试：

```text
trade / kline / orderbook_delta 三类 input 可以并行运行
暂停某一类 input 不影响其他 input
lease 丢失时三类 input 都停止
```

### 9.3 完成标准

```text
三类核心输入流都能写入 PostgreSQL
订单簿 delta 至少具备连续性校验和错误上报
订单簿 delta 保留全部变化档位
input 级暂停 / 恢复可独立生效
```

## 10. M7.5：批量写入、Delta 小时分区和滚动保留

实现状态：已于 2026-06-07 完成。数据库 migration 版本为 4。

### 10.1 功能范围

在进入完整订单簿状态维护之前，先补齐高频事实流的写入吞吐和生命周期管理。

批量消费与写入：

```text
每个 input 独立维护 batch，避免不同 input 互相阻塞
batch_size 和 flush_interval 按 stream kind 分别配置
达到 batch_size 或 flush_interval 任一条件即 flush
orderbook delta 仍逐条执行 decode 和 sequence 连续性校验
只把通过校验的最高连续消息前缀加入待提交批次
批量数据库事务成功后，commit 该批最高连续 Kafka offset
数据库事务失败时不 commit，重试同一批次
Kafka commit 失败时只重试 commit，不重复构造后续批次
```

建议默认值只作为配置默认值，不作为写死限制：

```text
trade:
  batch_size = 500
  flush_interval = 100ms

kline:
  batch_size = 100
  flush_interval = 500ms

orderbook_delta:
  batch_size = 2000
  flush_interval = 100ms
```

PostgreSQL 写入策略：

```text
orderbook delta 优先使用 pgx CopyFrom 批量写入分区父表
trade 使用批量 INSERT 或 pgx Batch
kline 保留 revision-guarded UPSERT，但在一次事务中提交一批
每个 batch 的事实行和持久写入进度必须位于同一个事务
```

新增持久进度表，例如：

```text
stream_write_progress
  input_id
  kafka_partition
  durable_offset
  last_raw_event_id
  last_update_id
  last_sequence
  updated_at

PRIMARY KEY (input_id, kafka_partition)
```

该表是存储正确性状态，不等同于用于控制面展示的 `stream_runtime_status`。它用于解决以下场景：

```text
数据库批次已经提交
Kafka offset commit 失败或进程退出
消息被重新投递
runtime 读取 durable_offset 后确认该 offset 已经持久化
不重复写事实行，直接重试提交 Kafka offset
```

orderbook delta 分区和保留：

```text
orderbook_deltas 改为 PostgreSQL declarative partitioned table
新增 raw_payload bytea，逐字节保存标准化 Kafka message value
按 ingested_at 进行 RANGE 分区
每个分区覆盖 1 小时
默认保留 72 小时，允许配置
每小时运行 partition maintenance
至少预创建未来 3 小时分区，允许配置
使用 PostgreSQL advisory lock 保证多个 runtime 中只有一个执行分区维护
分区边界统一使用 UTC 和数据库时钟计算
缺少目标分区时写入失败且不 commit Kafka offset
过期分区优先 DETACH PARTITION CONCURRENTLY，再 DROP
禁止用主表大范围 DELETE 作为常规清理方式
```

只删除上界已经早于保留截止时间的完整小时分区，因此配置为 72 小时时，实际保留时长为 72 小时至不足 73 小时。

分区表幂等设计：

```text
PostgreSQL 不支持不包含分区键的跨分区唯一约束
不再依赖 orderbook_deltas 上跨小时的全局唯一索引
Kafka 重放由 stream_write_progress 的 durable_offset 处理
同一 raw_event_id 出现在更高 Kafka offset 时，由 raw_event_id / sequence 校验识别
每个小时分区保留 input_id + partition + offset、raw_event_id、event_time 查询索引
```

生命周期行为：

```text
正常暂停或优雅关闭：
  flush 已校验的待写批次，再提交对应 offset

lease 丢失：
  停止 fetch
  丢弃尚未写入数据库的内存 batch，不 commit
  由新 owner 从 Kafka committed offset 重放

batch 内遇到 sequence gap：
  flush gap 之前的有效连续前缀
  commit 到 gap 前一个 offset
  gap 消息保持未提交并上报错误
```

### 10.2 测试范围

批处理单元测试：

```text
达到 batch_size 触发 flush
达到 flush_interval 触发 flush
每个 input 的 batch 相互隔离
数据库失败时整批不 commit
数据库成功、Kafka commit 失败时不重复写事实批次
暂停时 flush 已验证批次
lease 丢失时未写 batch 不 commit
sequence gap 只提交 gap 前的有效连续前缀
```

PostgreSQL 集成测试：

```text
一批 orderbook delta 通过 CopyFrom 写入成功
事实批次和 stream_write_progress 原子提交
重放 durable_offset 以内消息不产生重复事实
bytea raw_payload 可逐字节读回，全部 bids / asks 档位可读回
消息按 ingested_at 路由到正确小时分区
当前小时和未来小时分区可自动创建
缺少目标分区时写入失败且 offset 不提交
超过 72 小时的完整分区可 detach 和 drop
仍在保留窗口边界内的分区不会误删
清理后主表查询不再返回过期数据
```

性能测试：

```text
使用接近真实档位数量和消息大小的 orderbook delta
记录 rows/s、batch flush latency、WAL 增长和数据库 CPU
确认批量写相对单条写入有明确吞吐提升
确认 partition maintenance 不阻塞持续写入
```

本地开发基准（PostgreSQL 18 Docker，2000 条 delta / batch，3 次采样）约为
22.9 ms / batch，即约 8.7 万 rows/s。该结果只用于确认批量路径生效，不作为生产容量承诺。

### 10.3 完成标准

```text
三类输入流均支持可配置批量写入
高频 orderbook delta 不再逐条往返 PostgreSQL
orderbook delta 按小时分区并自动滚动保留 72 小时
清理通过 detach/drop 分区完成，不制造大规模 dead tuples
数据库提交、持久进度和 Kafka commit 的故障恢复测试通过
```

## 11. M8：完整 OrderBook Snapshot 和基础查询 API

### 11.1 功能范围

实现订单簿快照生成：

```text
消费上游 adapter 提供的完整 orderbook_snapshot 基准输入
orderbook_snapshot 使用独立 Kafka topic 和 consumer group
基准快照到达前缓存后续 delta，但不发布有效本地快照
从完整基准快照初始化 orderbook state
只应用 sequence 连续且晚于基准快照的 delta
MarketGroupWorker 内维护当前 orderbook state
按可配置间隔生成 snapshot，默认建议 1 分钟但不写死
每个 snapshot 保存所有实际存在且数量非零的价格档位
不执行 top N、百分比深度、价格距离或最大档位数截断
bids 按价格降序
asks 按价格升序
写入 orderbook_snapshots
sequence 断裂时废弃当前 state，并等待新的完整基准快照
```

`orderbook_snapshot` 和 `orderbook_delta` 位于不同 topic，不能依赖 Kafka 提供跨 topic 顺序。runtime 必须使用 snapshot sequence 过滤缓存中的旧 delta，并从第一个能与 snapshot 连续衔接的 delta 开始应用。

完整深度使用稀疏价位表示，只保存实际存在的挂单档位，不从价格 0 到最高价格人为补齐空档位。当前 `depth_limit` 字段应通过 migration 删除，或在兼容期间要求内部生成的完整快照将其保持为 NULL。

实现查询 API：

```text
GET /markets/{group_id}/trades
GET /markets/{group_id}/klines
GET /markets/{group_id}/orderbook/snapshots
```

查询约定：

```text
必须传 time range
默认按时间升序或倒序固定
分页使用时间游标 + limit
不做复杂跨市场查询
```

### 11.2 测试范围

orderbook state 测试：

```text
完整基准 snapshot 初始化后可应用 delta
基准 snapshot 到达前不生成有效本地 snapshot
缓存 delta 与基准 sequence 正确衔接
早于或等于基准 sequence 的缓存 delta 被丢弃
无法与基准 sequence 衔接时保持 error 并等待下一份基准 snapshot
bids 始终按价格降序输出
asks 始终按价格升序输出
数量为 0 的档位会删除
远离盘口的档位不会被截断
sequence 断裂时废弃 state、停止生成 snapshot 并上报错误
新的完整基准 snapshot 可以恢复 state
```

snapshot 写入测试：

```text
配置的 snapshot_interval 生效
修改 snapshot_interval 不需要修改代码
重复 snapshot_time 不产生重复行
snapshot bids / asks 可读回
snapshot 包含全部非零 bids / asks 档位
best bid = bids[0]
best ask = asks[0]
```

查询 API 测试：

```text
time range 查询 trades
time range 查询 klines
time range 查询 snapshots
limit 生效
非法时间范围返回 400
不存在 group 返回 404
```

### 11.3 完成标准

```text
可以查询 PostgreSQL 中的 trade / kline / snapshot
snapshot 表保持纯事实，不写 spread / mid / depth 等派生指标
snapshot 生成间隔可配置
snapshot 保存完整稀疏订单簿，不截断远离盘口的挂单
订单簿状态、基准恢复和快照排序有测试保障
```

## 12. M9：派生指标 Worker

### 12.1 功能范围

实现第一批指标表和写入接口：

```text
trade_metrics_1m
book_metrics_1m
cross_metrics_1m，可选
```

第一版优先：

```text
trade_metrics_1m:
  volume
  quote_volume
  trade_count
  vwap
  buy_volume
  sell_volume

book_metrics_1m:
  avg_spread
  min_spread
  max_spread
  avg_depth
  avg_imbalance
```

指标表字段需要包含：

```text
group_id
bucket_time
metric_version
computed_at
```

### 12.2 测试范围

指标计算单元测试：

```text
VWAP 计算
buy / sell volume 计算
spread 计算
depth 计算
imbalance 计算
空 bucket 处理
```

PostgreSQL 集成测试：

```text
指标写入成功
同一 group_id + bucket_time + metric_version 可覆盖
不同 metric_version 可并存或按策略覆盖
```

### 12.3 完成标准

```text
指标由独立 worker 计算
事实表不依赖指标表
指标算法变更后可以通过 metric_version 重算
```

## 13. M10：可靠性、可观测性和部署

### 13.1 功能范围

补齐生产运行能力：

```text
graceful shutdown
HTTP health check
readiness check
Prometheus metrics
结构化日志
dead letter 处理
配置文件示例
Dockerfile
docker compose 本地环境
K8s manifests 草案
```

关键指标：

```text
runtime heartbeat age
owned groups count
input actual_status
kafka lag
processed messages total
write errors total
decode errors total
lease renew failures total
db write latency
```

### 13.2 测试范围

可靠性测试：

```text
收到 SIGTERM 后停止消费新消息
已处理消息写入成功后再 commit
shutdown timeout 后强制退出
db 短暂失败时错误可见
Kafka decode error 不阻塞后续消息，按策略进入 dead letter
```

部署测试：

```text
docker compose 一键启动 PostgreSQL 和服务
health check 正常返回
缺少必要配置时容器启动失败并输出明确错误
```

### 13.3 完成标准

```text
本地可以通过 docker compose 跑完整链路
服务具备基础可观测性
异常路径不会静默失败
```

## 14. 总体验收场景

最终 PostgreSQL MVP 需要通过以下端到端场景：

```text
1. 启动 PostgreSQL
2. 执行 migration
3. 启动 md-control-plane
4. 创建 binance:spot:BTCUSDT
5. 创建 trade / kline_1m / orderbook_delta / orderbook_snapshot inputs
6. 启动两个 md-stream-runtime
7. 确认只有一个 runtime 抢到 BTCUSDT lease
8. 向 Kafka 写入 trade
9. trade 被写入 PostgreSQL
10. offset 在写入成功后 commit
11. 重放同一条 trade 不产生重复行
12. 暂停 trade input 后不再消费 trade
13. 恢复 trade input 后继续消费
14. 批量 orderbook delta 被完整写入小时分区，档位未截断且 raw_payload 字节一致
15. 数据库批次成功后才提交该批最高连续 Kafka offset
16. 超过 72 小时的完整 delta 分区被 detach/drop，保留窗口内数据仍可查询
17. 完整基准 snapshot 到达后可以重建订单簿
18. 按配置的 snapshot_interval 生成完整深度本地 snapshot
19. kill 当前 owner runtime
20. lease 过期后另一个 runtime 接管
21. 查询 API 可以看到数据和 stream_runtime_status
```

## 15. 建议优先级

如果时间有限，严格按这个顺序推进：

```text
M0 项目骨架
M1 migration
M2 MetadataStore + lease
M3 PostgresStorage 幂等写入
M4 Control Plane API
M5 Runtime lease loop
M6 Kafka trade 消费
M7 kline / orderbook_delta
M7.5 批量写入 / delta 小时分区 / 72 小时滚动保留
M8 snapshot / 查询 API
M9 指标 worker
M10 可靠性和部署
```

不要提前做：

```text
多 storage backend 抽象细节
复杂 rebalance
复杂查询 API
完整 exactly-once
跨交易对指标
前端页面
```

第一版真正的成功标准是：

```text
PostgreSQL 版本能稳定跑通控制面、lease 调度、Kafka 消费、幂等写入和状态查询。
```
