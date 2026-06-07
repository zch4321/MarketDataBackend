# 一、PRD：市场数据处理后台

## 1. 产品背景

系统需要处理来自不同市场数据源的标准化行情数据，包括：

1. K 线数据
2. 成交 / 聚合成交数据
3. 订单簿增量数据

原始市场接入、交易所协议适配、字段标准化由外部 adapter / converter 完成。本系统只消费标准化后的 Kafka 数据，不直接连接数据源。

系统需要支持：

1. 动态新增数据源
2. 动态新增交易对
3. 动态暂停 / 恢复某个交易对
4. 动态暂停 / 恢复某类输入
5. 后台多节点部署
6. K8s 弹性部署
7. 处理过程不丢数据

---

## 2. 产品目标

### 2.1 核心目标

构建一个市场数据处理后台，能够：

1. 从 Kafka 消费标准化行情数据
2. 按市场-交易对聚合处理三类数据
3. 计算基础指标
4. 写入可配置的唯一存储后端
5. 支持运行时动态管理数据流
6. 支持多节点部署与故障接管

---

## 3. 非目标

第一版不处理：

1. 原始交易所 WebSocket / REST 接入，这一任务交给kafka前的适配器
2. 交易所字段标准化，这一任务交给kafka前的适配器
3. 下单、交易、账户相关功能
4. 复杂回测系统
5. 完整流式 exactly-once 语义

---

## 4. 核心概念

### 4.1 MarketGroup

一个 MarketGroup 表示一个市场-交易对：

```text
MarketGroup = exchange + market_type + symbol
```

示例：

```text
binance:spot:BTCUSDT
okx:swap:BTC-USDT-SWAP
```

一个 MarketGroup 由一个后端节点独占处理。

---

### 4.2 InputStream

一个 InputStream 表示某个 MarketGroup 下的一类输入数据。

```text
InputStream = MarketGroup + stream_key
```

支持的数据类型：

```text
kline
trade
orderbook_snapshot
orderbook_delta
```

示例：

```text
binance:spot:BTCUSDT:trade
binance:spot:BTCUSDT:kline_1m
binance:spot:BTCUSDT:orderbook_delta
```

其中 `stream_key` 是对外使用的稳定标识，例如 `trade`、`kline_1m`、`orderbook_delta`。

数据库内部建议拆成：

```text
stream_kind = trade / kline / orderbook_delta / orderbook_snapshot
interval    = 1m / 5m / 1h，仅 kline 使用
```

---

## 5. 用户角色

### 5.1 管理员 / 运维

负责：

```text
新增 MarketGroup
启用 / 暂停 / 删除 MarketGroup
启用 / 暂停某类输入
查看 Kafka lag
查看 worker 状态
查看处理错误
```

---

### 5.2 数据消费者

通过 API 或数据库查询：

```text
K 线
成交
订单簿快照
分钟级成交指标
分钟级盘口指标
跨数据源指标
```

---

## 6. 功能需求

## 6.1 MarketGroup 管理

系统需要支持：

```text
创建 MarketGroup
暂停 MarketGroup
恢复 MarketGroup
禁用 MarketGroup
查询 MarketGroup 状态
查询 MarketGroup 所属 runtime 节点
```

示例 API：

```http
POST /groups
POST /groups/{group_id}/pause
POST /groups/{group_id}/resume
DELETE /groups/{group_id}
GET /groups
GET /groups/{group_id}
```

---

## 6.2 InputStream 管理

系统需要支持：

```text
为 MarketGroup 添加 kline 输入
为 MarketGroup 添加 trade 输入
为 MarketGroup 添加 orderbook 输入
暂停某类输入
恢复某类输入
查看某类输入状态
查看某类输入 Kafka lag
```

示例 API：

```http
POST /groups/{group_id}/inputs
POST /groups/{group_id}/inputs/{stream_key}/pause
POST /groups/{group_id}/inputs/{stream_key}/resume
GET /groups/{group_id}/inputs
```

---

## 6.3 Kafka 消费

系统需要支持：

```text
每个 InputStream 对应一个 Kafka topic
每个 InputStream 使用独立 consumer group
手动 commit offset
处理成功后再 commit
暂停后不消费新数据
恢复后从 committed offset 继续消费
```

Topic 命名建议：

```text
md.normalized.{exchange}.{market_type}.{symbol}.{stream_key}
```

示例：

```text
md.normalized.binance.spot.BTCUSDT.trade
md.normalized.binance.spot.BTCUSDT.kline_1m
md.normalized.binance.spot.BTCUSDT.orderbook_delta
md.normalized.binance.spot.BTCUSDT.orderbook_snapshot
```

---

## 6.4 数据处理

系统需要支持三类数据处理。

### K 线

处理：

```text
写入 K 线表
支持更新未闭合 K 线
支持查询 OHLCV
```

---

### 成交

处理：

```text
写入成交表
计算分钟成交量
计算 VWAP
计算买卖成交量 imbalance
计算成交量分布
```

---

### 订单簿

处理：

```text
完整保存标准化 Kafka 消息中的每一条 orderbook diff，不截断、不采样、不聚合档位
从完整基准快照初始化 orderbook state
维护当前 orderbook state
校验 sequence
按可配置间隔生成完整深度订单簿快照，默认建议 1 分钟
计算 spread
计算 mid price
计算 depth
计算 orderbook imbalance
```

完整深度的含义是保存数据源当前订单簿中所有实际存在且数量非零的价格档位，包括远离 best bid / best ask 的挂单。系统不按 top N 或价格距离截断，也不会从价格 0 到最高价格人为补齐不存在的空档位。

---

### 跨数据指标

在同一个 MarketGroupWorker 内支持：

```text
成交价相对 mid price
trade-through
price impact
成交方向与盘口状态对比
```

第一版可选实现。

---

## 6.5 存储后端

系统只连接一个持久化数据库，但数据库类型可配置：

1. PostgreSQL
2. TimescaleDB
3. ClickHouse

通过 Storage Adapter 抽象写入和查询。

配置示例：

```yaml
storage:
  type: clickhouse
  dsn: clickhouse://user:password@clickhouse:9000/market
```

---

## 6.6 多节点调度

系统支持多个 runtime 节点同时运行。

要求：

1. 一个 MarketGroup 同一时间只能被一个节点处理
2. 不同 MarketGroup 可以分布在不同节点
3. 节点宕机后，其他节点可以接管
4. 接管后从 Kafka committed offset 继续消费

调度建立在应用层 lease

---

## 6.7 K8s 部署

系统支持部署到 Kubernetes。

部署组件：

```text
md-control-plane
md-stream-runtime
md-query-api，可选
```

K8s 负责：

```text
Pod 调度
Pod 重启
滚动发布
水平扩容
资源限制
健康检查
```

应用负责：

```text
MarketGroup lease
Kafka offset
graceful shutdown
MarketGroup 接管
```

---

## 7. 非功能需求

### 7.1 可靠性

1. Kafka 消息处理采用 at-least-once
2. 处理成功后手动 commit offset
3. 写入需要尽量幂等
4. Pod 退出时需要 graceful shutdown

---

### 7.2 可扩展性

1. 支持多 runtime 节点
2. 支持按 MarketGroup 水平扩展
3. 支持按权重分配 MarketGroup
4. 支持后续扩展多 Kafka cluster

---

### 7.3 可观测性

系统需要暴露：

1. MarketGroup 状态
2. InputStream 状态
3. Kafka lag
4. 处理延迟
5. 写库延迟
6. 错误数量
7. 订单簿 sequence gap
8. worker restart count

---

### 7.4 性能目标，第一版

1. 单机支持 10 个 MarketGroup
2. Kafka 消费 at-least-once
3. 分钟级指标延迟小于 5 秒
4. 秒级别数据处理延迟（不包含网络延迟）小于500ms

---

# 二、架构设计文档

## 1. 总体架构

```text
                 ┌──────────────────────┐
                 │ Frontend Admin UI    │
                 └───────────┬──────────┘
                             │ REST
                             ▼
                 ┌──────────────────────┐
                 │ md-control-plane     │
                 │ - group management   │
                 │ - input management   │
                 │ - node status        │
                 │ - assignment status  │
                 └───────────┬──────────┘
                             │
                             ▼
                 ┌──────────────────────┐
                 │ Metadata Storage     │
                 │ pg / timescale / ch  │
                 └───────────┬──────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│ runtime node A │  │ runtime node B │  │ runtime node C │
│ GroupWorker BTC│  │ GroupWorker ETH│  │ GroupWorker SOL│
└───────┬────────┘  └───────┬────────┘  └───────┬────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────┐
│ Kafka                                               │
│ md.normalized.binance.spot.BTCUSDT.trade            │
│ md.normalized.binance.spot.BTCUSDT.kline_1m         │
│ md.normalized.binance.spot.BTCUSDT.orderbook_delta  │
│ md.normalized.binance.spot.BTCUSDT.orderbook_snapshot│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│ Market Data Storage                                 │
│ PostgreSQL / TimescaleDB / ClickHouse               │
└─────────────────────────────────────────────────────┘
```

---

## 2. 服务拆分

## 2.1 md-control-plane

职责：

1. 管理 MarketGroup
2. 管理 InputStream
3. 管理 Runtime Node
4. 查看状态
5. 提供 Admin REST API

---

## 2.2 md-stream-runtime

职责：

1. 注册 runtime node
2. 抢占 MarketGroup lease
3. 启动 MarketGroupWorker
4. 消费 Kafka
5. 处理数据
6. 计算指标
7. 写入数据库
8. 上报状态

---

## 2.3 md-query-api，可选

职责：

1. 查询 K 线
2. 查询成交
3. 查询订单簿快照
4. 查询指标

MVP 可以先并入 `md-control-plane`。

---

# 3. Runtime 内部架构

```text
md-stream-runtime
│
├── RuntimeNode
│   ├── heartbeatLoop
│   ├── reconcileLoop
│   └── workerManager
│
├── MarketGroupWorker: binance:spot:BTCUSDT
│   ├── TradeInputWorker
│   ├── KlineInputWorker
│   ├── OrderBookInputWorker
│   ├── group event loop
│   ├── MarketState
│   ├── MetricEngine
│   └── StorageWriter
│
└── MarketGroupWorker: binance:spot:ETHUSDT
    ├── TradeInputWorker
    └── OrderBookInputWorker
```

---

## 4. MarketGroupWorker 设计

一个 MarketGroupWorker 负责一个市场-交易对。

```text
MarketGroupWorker(exchange, market_type, symbol)
```

内部包含：

1. 1 到 4 个 BasicInputWorker 分别处理 K 线、成交、订单簿增量和完整基准快照
2. n 个 IndicatorWorker 可以同时接受多个数据流输入 计算跨数据指标
3. 一个 storage writer
4. 一个状态上报器

---

## 4.1 InputWorker

每个 InputWorker 负责一个 Kafka topic：

```text
TradeInputWorker
KlineInputWorker
OrderBookInputWorker
OrderBookSnapshotInputWorker
```

职责：

1. 消费 Kafka
2. decode 消息
3. schema validate
4. 按 stream kind 的 batch_size / flush_interval 聚合已验证事件
5. 将事实批次和持久写入进度在同一数据库事务中提交
6. 等待 storage ack
7. 成功后 commit 该批最高连续 Kafka offset
---

## 4.2 IndicatorWorker

一个 IndicatorWorker 可以同时接受多个 InputWorker 或原始 Kafka 数据的事件输入，计算跨数据指标：

当前版本只需保留接口

---

## 5. Kafka 设计

## 5.1 Topic 粒度

推荐：

```text
一个 InputStream 一个 topic
```

即：

```text
exchange + market_type + symbol + stream_key
```

示例：

```text
md.normalized.binance.spot.BTCUSDT.trade
md.normalized.binance.spot.BTCUSDT.kline_1m
md.normalized.binance.spot.BTCUSDT.orderbook_delta
md.normalized.binance.spot.BTCUSDT.orderbook_snapshot
```

---

## 5.2 Partition

单个交易对的三个数据都对顺序有强需求，所以每个topic 1 partition

---

## 5.3 Consumer Group

每个 InputStream 使用独立 consumer group.

保障三类数据 offset 独立 暂停某类输入不影响其他输入 节点迁移后可继续消费

```text
md-runtime.{group_id}.{stream_key}
```

示例：

```text
md-runtime.binance.spot.BTCUSDT.trade
md-runtime.binance.spot.BTCUSDT.kline_1m
md-runtime.binance.spot.BTCUSDT.orderbook_delta
md-runtime.binance.spot.BTCUSDT.orderbook_snapshot
```

## 6. Offset 处理

采用 at-least-once

流程：

1. InputWorker 按 partition 顺序拉取 Kafka 消息
2. 逐条 decode + validate；orderbook delta 同时逐条校验 sequence
3. 将连续通过校验的消息加入当前 input 的 batch
4. 达到 batch_size 或 flush_interval 后提交数据库事务
5. 事实数据和 stream_write_progress 在同一事务中落库
6. 数据库成功后 ack
7. InputWorker commit 该批最高连续 Kafka offset

禁止：

```text
enable.auto.commit = true
```

必须手动且整个数据库批次成功后 commit。批次内只能提交最高连续 offset，不能越过 decode error、sequence gap 或写入失败的消息。

---

## 7. 数据库设计

系统只连接一个 storage backend：

```text
PostgreSQL
TimescaleDB
ClickHouse
```

这里的“一个 storage backend”指部署时选择一个 Storage Adapter。领域模型应保持一致，但物理 DDL 需要按数据库特性分别优化，不要求 PostgreSQL、TimescaleDB、ClickHouse 的表结构逐列完全相同。

数据库表分为三层：

1. 控制面元数据：描述系统应该处理什么，以及谁正在处理
2. 行情事实表：保存 trade / kline / orderbook 的原始或标准化事实数据
3. 派生指标表：保存由指标 worker 计算出来的可重算结果

设计原则：

- desired_status 表示用户或控制面的期望
- actual_status 表示 runtime 观测到的实际状态
- lease 表示带过期时间的处理权
- 事实表必须支持 at-least-once 消费下的幂等写入
- 快照表只保存订单簿事实，spread / depth / imbalance 等指标独立计算
- orderbook delta 默认只保留 72 小时，并允许通过配置调整
- PostgreSQL 的 orderbook delta 按 `ingested_at` 小时分区，通过删除过期分区完成滚动清理
- orderbook snapshot 的生成间隔可配置，默认建议值为 1 分钟，不在代码中写死

---

## 7.1 Metadata 表

### market_groups

一个 MarketGroup 表示一个市场-交易对，即 `exchange + market_type + symbol`。

`base_asset` 和 `quote_asset` 是从交易对中拆出的基础资产和计价资产。例如：

```text
BTCUSDT:
  base_asset  = BTC
  quote_asset = USDT
```

它们不是调度所必需，但对按币种查询、统计、风控和后续资产维度分析有用。MVP 可以允许为空。

```sql
CREATE TABLE market_groups
(
    group_id         text PRIMARY KEY,
    exchange         text        NOT NULL,
    market_type      text        NOT NULL,
    symbol           text        NOT NULL,
    base_asset       text,
    quote_asset      text,

    desired_status   text        NOT NULL,

    weight           integer     NOT NULL DEFAULT 1,

    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,

    UNIQUE (exchange, market_type, symbol)
);
```

---

### group_inputs

一个 group input 表示某个 MarketGroup 下的一条输入流。

对外可以继续使用 `stream_key`：

```text
trade
kline_1m
orderbook_delta
```

数据库内部拆成：

```text
stream_kind = trade / kline / orderbook_delta / orderbook_snapshot
interval    = 1m / 5m / 1h，仅 kline 使用
```

这样可以避免后续在数据库里反复解析 `kline_1m` 这种字符串。

```sql
CREATE TABLE group_inputs
(
    input_id       text PRIMARY KEY,
    group_id       text        NOT NULL,

    stream_key     text        NOT NULL,
    stream_kind    text        NOT NULL,
    interval       text,

    enabled        boolean     NOT NULL DEFAULT true,

    kafka_cluster  text        NOT NULL DEFAULT 'default',
    kafka_topic    text        NOT NULL,
    kafka_group_id text        NOT NULL,

    desired_status text        NOT NULL,

    schema_version integer     NOT NULL DEFAULT 1,

    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,

    UNIQUE (group_id, stream_key)
);
```

---

### runtime_nodes

`runtime_nodes` 表示当前有哪些 `md-stream-runtime` 进程存活。即使不使用 K8s，它也可以表示某台机器上的某个 runtime 进程。

它的作用：

```text
记录谁在线
记录每个 runtime 的容量
记录最后心跳时间
辅助控制面判断节点是否健康
辅助 lease 故障接管
```

```sql
CREATE TABLE runtime_nodes
(
    node_id           text PRIMARY KEY,
    hostname          text        NOT NULL,
    pod_name          text,
    status            text        NOT NULL,

    max_groups        integer     NOT NULL,
    current_groups    integer     NOT NULL,
    max_weight        integer     NOT NULL,
    current_weight    integer     NOT NULL,

    last_heartbeat_at timestamptz NOT NULL,
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL
);
```

---

### market_leases

`market_leases` 是应用层的带过期时间的锁。

一个 MarketGroup 同一时间只能被一个 runtime node 处理。持有 lease 的 node 负责该 MarketGroup 下所有 enabled inputs。node 正常运行时持续续租；node 挂掉后停止续租，lease 到期后其他 node 可以接管。

```sql
CREATE TABLE market_leases
(
    group_id         text        PRIMARY KEY,
    node_id          text        NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    version          bigint      NOT NULL DEFAULT 0,

    acquired_at      timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);
```

实现要求：

```text
抢占 lease 必须是 compare-and-swap 语义
续租时必须校验 node_id 仍是 owner
lease_expires_at 过期后，其他 node 才能抢占
```

---

### stream_runtime_status

`stream_runtime_status` 是运行时观测状态，不是用户配置。

`group_inputs.desired_status` 表示“希望这条流运行或暂停”；`stream_runtime_status.actual_status` 表示“runtime 实际看到的状态”。两者分开后，控制面可以清楚回答：

```text
这条流是否真的在消费
当前 Kafka lag 多大
最后处理到哪条 offset
最后一条 event_time 是多少
最近一次错误是什么
```

```sql
CREATE TABLE stream_runtime_status
(
    input_id              text PRIMARY KEY,
    group_id              text        NOT NULL,
    stream_key            text        NOT NULL,

    node_id               text,
    actual_status         text        NOT NULL,

    kafka_partition       integer,
    kafka_lag             bigint,
    committed_offset      bigint,
    high_watermark_offset bigint,

    last_event_time       timestamptz,
    last_processed_time   timestamptz,
    last_error            text,

    updated_at            timestamptz NOT NULL
);
```

---

## 7.2 行情事实表

建议至少包含：

```text
trades
klines
orderbook_deltas
orderbook_snapshots
```

事实表需要保留来源信息：

```text
group_id
input_id
event_time
exchange_time
local_receive_time
kafka_topic
kafka_partition
kafka_offset
schema_version
ingested_at
```

原因是 Kafka 消费语义是 at-least-once。进程可能在“写入数据库成功、commit offset 失败”之间退出，接管节点会重复消费同一条消息。因此事实表必须具备幂等写入能力。

---

### trades

逻辑字段：

```text
group_id
input_id
event_time
exchange_time
local_receive_time
trade_id
raw_trade_id
price
quantity
side
is_aggregated
kafka_topic
kafka_partition
kafka_offset
ingested_at
```

字段含义：

```text
trade_id      = 系统标准化后的成交 ID
raw_trade_id  = 交易所原始成交 ID
```

幂等键建议：

```text
优先：input_id + raw_trade_id，前提是 raw_trade_id 稳定且不为空
兜底：input_id + kafka_partition + kafka_offset
```

因此 `raw_trade_id` 可以加，而且建议加。但不要只依赖它，因为不是所有交易所、所有数据源都能保证成交原始 ID 完整稳定。

---

### klines

逻辑字段：

```text
group_id
input_id
source
interval
open_time
close_time
open
high
low
close
volume
quote_volume
trade_count
is_closed
revision
kafka_topic
kafka_partition
kafka_offset
updated_at
```

字段含义：

```text
source   = exchange / computed
revision = 未闭合 K 线被更新时递增或覆盖用的版本
```

唯一键建议：

```text
group_id + source + interval + open_time
```

如果只保存交易所推送的 K 线，也可以使用：

```text
input_id + open_time
```

---

### orderbook_deltas

订单簿增量的关键不是单纯保存一个 `raw_depth_diff_id`，而是保存能校验连续性的 sequence 信息。不同交易所命名不同，例如 Binance 常见 `U / u / pu`，OKX 常见 `seqId / prevSeqId`。

逻辑字段：

```text
group_id
input_id
event_time
exchange_time
local_receive_time
raw_event_id
first_update_id
last_update_id
prev_update_id
sequence
bids
asks
raw_payload
kafka_topic
kafka_partition
kafka_offset
ingested_at
```

字段含义：

```text
raw_event_id     = 交易所原始事件 ID，raw_depth_diff_id 可以映射到这里
first_update_id  = 本条 delta 覆盖的第一个更新序号
last_update_id   = 本条 delta 覆盖的最后一个更新序号
prev_update_id   = 上一条更新序号，用于连续性校验
sequence         = 某些交易所提供的统一递增序号
bids / asks      = 本条 delta 中全部变化档位，不允许按深度、数量或价格距离截断
raw_payload      = bytea，逐字节保存从标准化 Kafka topic 收到的完整 message value
```

`raw_payload` 使用 `bytea` 而不是 `jsonb`，以保留原始字段顺序、数值文本形式和消息字节。这里的“原始完整数据”指本系统收到的标准化 Kafka 消息。交易所协议适配发生在上游 adapter，因此除非 adapter 主动把交易所原始 frame 放入消息，本系统无法恢复标准化之前的交易所报文。

幂等键建议：

```text
Kafka at-least-once 重放：
  使用独立的 stream_write_progress 表记录每个 input + partition
  已经随事实数据一起提交的最高连续 Kafka offset

交易所事件重复：
  优先使用 raw_event_id
  其次使用 last_update_id / sequence 连续性判断
```

PostgreSQL 物理存储约定：

```text
分区键：
  ingested_at

分区粒度：
  1 小时

默认保留：
  72 小时，可配置

清理方式：
  每小时预创建未来分区
  DETACH 已完整过期的小时分区
  DROP 已 detach 的分区
  不对主表执行大范围滚动 DELETE
```

小时分区只能删除已经完整过期的分区，因此实际保留时间可能比配置值多不到 1 小时。若目标配置为 72 小时，则数据实际保留范围为 72 至不足 73 小时。

由于 PostgreSQL 分区表不能在不包含分区键的情况下提供跨分区唯一约束，Kafka 重放幂等不依赖 `orderbook_deltas` 上的全局唯一索引。事实批次和 `stream_write_progress` 必须在同一数据库事务中提交；事务成功后才能 commit Kafka offset。

---

### orderbook_snapshots

订单簿快照只保存事实，不默认保存派生指标。快照生成间隔通过配置指定，默认建议值为 1 分钟，但可以按部署需要调大或调小。

系统必须先取得完整基准快照，再应用 sequence 连续的增量消息。由于本系统不直接连接交易所，完整基准快照必须由上游 adapter 通过标准化的 `orderbook_snapshot` 输入提供。基准快照到达前可以暂存后续 delta，但不能生成被标记为有效的本地快照；sequence 断裂后必须废弃当前状态并等待新的完整基准快照。

逻辑字段：

```text
snapshot_id
group_id
input_id
snapshot_time
sequence
bids
asks
created_at
```

存储约定：

```text
bids 按价格降序
asks 按价格升序
bids[0] 即 best bid
asks[0] 即 best ask
每个快照保存所有实际存在且数量非零的 bids / asks 档位
不允许 top N、百分比深度、价格距离或最大档位数截断
```

当前模型中的 `depth_limit` 字段不再用于内部生成的订单簿快照；后续 migration 应删除该字段，或在兼容迁移期间要求完整快照的 `depth_limit` 为 NULL。

因此默认不需要在 snapshot 表里额外保存：

```text
best_bid
best_ask
mid_price
spread
depth
imbalance
```

这些值都可以由快照计算出来。如果未来查询路径大量需要 top-of-book，并且解析完整 bids / asks 成为性能瓶颈，可以再冗余 `best_bid_price`、`best_ask_price` 作为性能优化，但它们不是逻辑必需字段。

唯一键建议：

```text
group_id + snapshot_time
```

如果 snapshot 来自某条明确输入流，也可以使用：

```text
input_id + snapshot_time
```

---

## 7.3 派生指标表

派生指标由独立 IndicatorWorker 计算，不写入订单簿快照主表。

建议保留：

```text
trade_metrics_1m
book_metrics_1m
cross_metrics_1m
```

这些表保存的是可重算结果：

```text
成交量
VWAP
买卖成交量 imbalance
spread
mid price
depth
orderbook imbalance
trade-through
price impact
```

设计原则：

```text
指标表按 bucket_time 查询
指标表需要记录 metric_version
指标算法变更后允许重算并覆盖同一 bucket
指标表不要反向影响事实表
```

---

## 7.4 不同 Storage Backend 的物理实现建议

### PostgreSQL / TimescaleDB

```text
metadata 表使用普通关系表
lease 抢占使用事务和条件 UPDATE 实现 compare-and-swap
行情事实表使用分区表或 TimescaleDB hypertable
金额和数量使用 numeric，不使用 float
bids / asks 可先用 jsonb 保存
trade / delta 幂等写入使用 ON CONFLICT
```

### ClickHouse

```text
事实表使用 MergeTree 系列引擎
ORDER BY 优先包含 group_id / input_id 和时间字段
trades 可按 toYYYYMM(event_time) 分区
klines 可使用 ReplacingMergeTree 支持未闭合 K 线更新
bids / asks 可使用 Array / Nested / JSON 类型
metadata 和 lease 如果也放在 ClickHouse，需要 Metadata Adapter 明确实现条件更新和 owner 校验
```

如果未来允许同时使用两个数据库，推荐：

```text
PostgreSQL 管理 metadata / lease
ClickHouse 存储高吞吐行情事实和指标
```

---

# 8. Storage Adapter

代码层统一接口：

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

Metadata 统一接口：

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

实现：

```text
PostgresStorage
TimescaleStorage
ClickHouseStorage

PostgresMetadataStore
TimescaleMetadataStore
ClickHouseMetadataStore
```

---

# 9. Lease 调度设计

## 9.1 基本原则

```text
lease 按 MarketGroup 抢占
同一个 MarketGroup 同一时间只能有一个 owner
owner 节点负责该 MarketGroup 下所有 enabled inputs
```

---

## 9.2 抢占流程

```text
1. runtime 节点启动
2. 注册 node
3. 定期扫描 desired_status = running 的 MarketGroup
4. 如果 group 无 owner 或 lease 过期，则尝试抢占
5. 抢占成功后启动 MarketGroupWorker
6. worker 运行期间定期续租
7. 续租失败则停止 worker
```

---

## 9.3 节点故障接管

```text
runtime Pod 挂掉
  ↓
不再续租
  ↓
lease_expires_at 过期
  ↓
其他节点抢占该 MarketGroup
  ↓
使用相同 Kafka group id 继续消费
```

---

# 10. K8s 部署设计

## 10.1 组件

```text
md-control-plane Deployment
md-stream-runtime Deployment
md-query-api Deployment，可选
ConfigMap
Secret
Service
Ingress
Prometheus ServiceMonitor，可选
```

---

## 10.2 md-stream-runtime Deployment 示例

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: md-stream-runtime
spec:
  replicas: 3
  selector:
    matchLabels:
      app: md-stream-runtime
  template:
    metadata:
      labels:
        app: md-stream-runtime
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: md-stream-runtime
          image: your-registry/market-data-backend:latest
          args: [ "stream-runtime" ]
          ports:
            - name: health
              containerPort: 8081
            - name: metrics
              containerPort: 9090
          envFrom:
            - configMapRef:
                name: md-backend-config
          env:
            - name: STORAGE_DSN
              valueFrom:
                secretKeyRef:
                  name: md-backend-secret
                  key: STORAGE_DSN

            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name

            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace

            - name: RUNTIME_NODE_ID
              value: "$(POD_NAMESPACE)/$(POD_NAME)"

          readinessProbe:
            httpGet:
              path: /readyz
              port: 8081

          livenessProbe:
            httpGet:
              path: /healthz
              port: 8081

          lifecycle:
            preStop:
              httpGet:
                path: /shutdown
                port: 8081

          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
```

---

# 11. Graceful Shutdown

Pod 退出流程：

```text
1. 收到 SIGTERM
2. runtime 进入 draining
3. 不再抢占新的 MarketGroup
4. 通知所有 GroupWorker 停止拉取新消息
5. 处理 eventCh 中已读取消息
6. flush 指标和 buffer
7. commit 已处理 offset
8. release group lease
9. 退出进程
```

---

# 12. REST API 草案

## 12.1 创建 MarketGroup

```http
POST /groups
```

```json
{
  "exchange": "binance",
  "market_type": "spot",
  "symbol": "BTCUSDT",
  "desired_status": "running",
  "inputs": [
    {
      "stream_key": "trade",
      "stream_kind": "trade",
      "kafka_topic": "md.normalized.binance.spot.BTCUSDT.trade"
    },
    {
      "stream_key": "kline_1m",
      "stream_kind": "kline",
      "interval": "1m",
      "kafka_topic": "md.normalized.binance.spot.BTCUSDT.kline_1m"
    },
    {
      "stream_key": "orderbook_delta",
      "stream_kind": "orderbook_delta",
      "kafka_topic": "md.normalized.binance.spot.BTCUSDT.orderbook_delta"
    },
    {
      "stream_key": "orderbook_snapshot",
      "stream_kind": "orderbook_snapshot",
      "kafka_topic": "md.normalized.binance.spot.BTCUSDT.orderbook_snapshot"
    }
  ]
}
```

---

## 12.2 暂停 MarketGroup

```http
POST /groups/{group_id}/pause
```

---

## 12.3 恢复 MarketGroup

```http
POST /groups/{group_id}/resume
```

---

## 12.4 暂停某类输入

```http
POST /groups/{group_id}/inputs/{stream_key}/pause
```

---

## 12.5 查询状态

```http
GET /groups/{group_id}
```

返回：

```json
{
  "group_id": "binance:spot:BTCUSDT",
  "desired_status": "running",
  "actual_status": "running",
  "lease_owner_node_id": "prod/md-stream-runtime-abc123",
  "inputs": [
    {
      "stream_key": "trade",
      "stream_kind": "trade",
      "status": "running",
      "kafka_lag": 120,
      "last_event_time": "2026-06-05T10:00:00Z"
    },
    {
      "stream_key": "orderbook_delta",
      "stream_kind": "orderbook_delta",
      "status": "running",
      "kafka_lag": 880,
      "last_event_time": "2026-06-05T10:00:01Z"
    }
  ]
}
```

---

# 13. 代码结构建议

```text
cmd/
  md-control-plane/
    main.go
  md-stream-runtime/
    main.go
  md-query-api/
    main.go

internal/
  api/
    routes.go
    handlers.go

  group/
    model.go
    manager.go
    worker.go
    input_worker.go
    state.go
    lifecycle.go

  kafka/
    consumer.go
    admin.go
    client_registry.go

  processor/
    trade.go
    kline.go
    orderbook.go
    metrics.go

  storage/
    storage.go
    postgres.go
    timescale.go
    clickhouse.go

  metadata/
    store.go
    postgres.go
    timescale.go
    clickhouse.go

  config/
    config.go

  observability/
    metrics.go
    logging.go
```

---

# 14. 配置示例

```yaml
server:
  mode: stream-runtime
  http_addr: ":8081"
  metrics_addr: ":9090"

storage:
  type: clickhouse
  dsn: "${STORAGE_DSN}"

kafka_clusters:
  default:
    brokers:
      - kafka-0.kafka:9092
      - kafka-1.kafka:9092
      - kafka-2.kafka:9092

runtime:
  node_id: "${RUNTIME_NODE_ID}"
  max_groups: 100
  max_weight: 500
  lease_ttl_seconds: 30
  reconcile_interval_seconds: 5
  worker_shutdown_timeout_seconds: 20

processing:
  manual_commit: true
  dead_letter_enabled: true
  dead_letter_topic: "md.dead_letter"

  batch:
    trade:
      size: 500
      flush_interval: 100ms
    kline:
      size: 100
      flush_interval: 500ms
    orderbook_delta:
      size: 2000
      flush_interval: 100ms

  orderbook:
    snapshot_interval: 1m
    delta_retention: 72h
    delta_partition_interval: 1h
    partition_maintenance_interval: 1h
    partition_precreate_horizon: 3h
```

---

# 15. MVP 范围

## 第一版必须做

```text
1. Go 实现 md-control-plane
2. Go 实现 md-stream-runtime
3. MarketGroup / InputStream 管理
4. Kafka 消费
5. 每个 MarketGroup 一个 GroupWorker
6. 每个 InputStream 一个 Kafka topic
7. 每个 InputStream 独立 consumer group
8. 手动 commit offset
9. Storage Adapter 至少支持一种数据库
10. K8s Deployment 部署
11. Graceful shutdown
12. 基础监控
```

---

## 第一版建议优先支持的存储

建议二选一：

```text
ClickHouse：如果你更关心高吞吐和历史分析
PostgreSQL：如果你更关心开发简单和元数据一致性
```

TimescaleDB 可以作为 PostgreSQL 之后的增强实现。

---

## 第一版可暂缓

```text
1. 多 Kafka cluster
2. 主动 MarketGroup rebalance
3. HPA 基于 Kafka lag 自动扩容
4. exactly-once
5. 复杂 watermark
6. 每个交易对独立 Pod
7. 图形化前端
```

---

# 16. 关键技术决策

| 决策点             | 选择                                |
|-----------------|-----------------------------------|
| 后端语言            | Go                                |
| 部署方式            | K8s Deployment                    |
| 核心调度单位          | MarketGroup                       |
| 数据输入单位          | InputStream                       |
| Kafka topic 粒度  | MarketGroup + stream_key          |
| Kafka partition | 第一版每 topic 1 partition            |
| Offset 语义       | 手动 commit，at-least-once           |
| 多节点分配           | 应用层 lease                         |
| 存储              | pg / timescaledb / clickhouse 三选一 |
| 状态模型            | GroupWorker actor 模型              |
| K8s 粒度          | 一个 Pod 多个 GroupWorker             |
| 失败接管            | lease 过期后其他 Pod 接管                |

---

# 17. 最终系统摘要

这个系统最终形态是：

```text
一个可动态调度的市场数据处理后台。

Kafka 按输入流拆：
  market + symbol + stream_key

Runtime 按交易对聚合：
  一个 MarketGroupWorker 处理一个交易对下的 kline / trade / orderbook

多节点按 MarketGroup 分摊：
  一个 MarketGroup 同一时间只属于一个 runtime node

K8s 负责部署和 Pod 生命周期：
  应用内部负责 lease、offset 和 graceful shutdown

数据库可配置：
  PostgreSQL / TimescaleDB / ClickHouse 三选一
```

一句话版本：

> 这是一个基于 Go + Kafka + 可插拔 Storage + K8s 的 MarketGroup Actor 系统，用应用层 lease 将不同市场-交易对分配到不同
> runtime 节点，每个节点内部用 goroutine 处理多个交易对，保证动态扩展、暂停恢复和故障接管。
