# Kafka 输入消息格式

> 适用版本：M10（2026-06-07）

所有消息由上游适配器写入，系统消费后解码、校验、持久化到 PostgreSQL。未知字段会被拒绝并进入 dead-letter 路径。

---

## 目录

1. [通用约定](#通用约定)
2. [Trade](#1-trade)
3. [Kline](#2-kline)
4. [OrderBook Delta](#3-orderbook-delta)
5. [OrderBook Snapshot](#4-orderbook-snapshot-引用快照)
6. [Topic 命名](#topic-命名)
7. [常见错误](#常见错误)

---

## 通用约定

### 时间字段（flexTime）

所有 `event_time`、`exchange_time`、`open_time`、`close_time` 等时间字段接受以下任意一种格式：

| 格式 | 示例 |
|---|---|
| RFC3339 | `"2026-06-07T10:00:00Z"` |
| RFC3339Nano | `"2026-06-07T10:00:00.123456789Z"` |
| epoch 毫秒数字 | `1780790400000` |
| epoch 毫秒字符串 | `"1780790400000"` |
| 小数 epoch | `1780790400000.5`（截断到毫秒） |

始终归一化为 UTC。`null` 或缺省表示字段未设置。

### 数字字段（flexString / flexInt64）

- **flexString**（price、quantity、id 等）：JSON 字符串或数字均可。`100` 和 `"100"` 等价。
- **flexInt64**（sequence、revision、trade_count、update_id 等）：JSON 整数、整数字符串均可。`null` 或缺省表示 nil。

### 未知字段拦截

所有解码器启用 `json.Decoder.DisallowUnknownFields()`。消息中包含 schema 未定义的字段会被拒绝并写入 `stream_poison_records`（dead-letter 表），offset 仍会提交以继续消费。

### Offset 与持久化

- 事实写入与 `stream_write_progress` 在同一数据库事务中提交。
- Dead-letter 消息：先写入 `stream_poison_records`，成功后 commit Kafka offset；写入失败则 offset 不提交。
- Sequence gap：offset 不提交，worker 退出 → 等待引用快照 → 重启后跳过 gap 消息。

---

## 1. Trade

**Topic 模式**：`md.<exchange>.<market_type>.<symbol>.trade`

### Go 结构体

```go
type tradeMessage struct {
    EventTime        flexTime   `json:"event_time"`          // 必填
    ExchangeTime     *flexTime  `json:"exchange_time"`       // 可选
    LocalReceiveTime *flexTime  `json:"local_receive_time"`  // 可选
    TradeID          flexString `json:"trade_id"`             // 可选
    RawTradeID       flexString `json:"raw_trade_id"`         // 可选
    Price            flexString `json:"price"`                // 必填，正数
    Quantity         flexString `json:"quantity"`             // 必填，正数
    Side             string     `json:"side"`                 // 必填
    IsAggregated     bool       `json:"is_aggregated"`        // 可选
}
```

### 完整示例

```json
{
  "event_time":         "2026-06-07T10:00:00Z",
  "exchange_time":      "2026-06-07T10:00:00.001Z",
  "local_receive_time": 1780790400000,
  "trade_id":           "12345678",
  "raw_trade_id":       "trade-001",
  "price":              "50000.00",
  "quantity":           "1.5",
  "side":               "buy",
  "is_aggregated":      false
}
```

### 最小示例

```json
{"event_time":"2026-06-07T10:00:00Z","raw_trade_id":"trade-1","price":"50000.00","quantity":"1.5","side":"buy"}
```

### 校验规则

| 字段 | 规则 |
|---|---|
| `event_time` | 必须存在 |
| `price` | 非空、正数十进制（`big.Float` > 0） |
| `quantity` | 非空、正数十进制 |
| `side` | 归一化为 `buy`/`sell`。`buy`/`b`/`bid` → `buy`；`sell`/`s`/`ask` → `sell` |

---

## 2. Kline

**Topic 模式**：`md.<exchange>.<market_type>.<symbol>.kline`

### Go 结构体

```go
type klineMessage struct {
    Source      string     `json:"source"`       // 可选，默认 "exchange"
    Interval    string     `json:"interval"`     // 必填，须匹配 Input 配置
    OpenTime    flexTime   `json:"open_time"`    // 必填
    CloseTime   flexTime   `json:"close_time"`   // 必填，须 > open_time
    Open        flexString `json:"open"`         // 必填，正数
    High        flexString `json:"high"`         // 必填，正数，>= low
    Low         flexString `json:"low"`          // 必填，正数
    Close       flexString `json:"close"`        // 必填，正数
    Volume      flexString `json:"volume"`       // 必填，非负数
    QuoteVolume flexString `json:"quote_volume"` // 可选，非负数
    TradeCount  flexInt64  `json:"trade_count"`  // 必填，非负整数
    IsClosed    bool       `json:"is_closed"`    // 必填
    Revision    flexInt64  `json:"revision"`     // 可选，>= 0
}
```

### 完整示例

```json
{
  "source":       "exchange",
  "interval":     "1m",
  "open_time":    "2026-06-07T10:00:00Z",
  "close_time":   "2026-06-07T10:01:00Z",
  "open":         "100.0",
  "high":         "200.0",
  "low":          "50.0",
  "close":        "150.0",
  "volume":       "1000.0",
  "quote_volume": "150000.0",
  "trade_count":  42,
  "is_closed":    true,
  "revision":     1
}
```

### 最小示例

```json
{"source":"exchange","interval":"1m","open_time":1780790400000,"close_time":1780790460000,"open":"100.0","high":"200.0","low":"50.0","close":"150.0","volume":"1000.0","quote_volume":"150000.0","trade_count":42,"is_closed":true,"revision":1}
```

### 终态保护

`is_closed=true` 的 Kline 只能被更高 revision **且同样是 closed** 的消息覆盖。不允许用更高 revision 的未闭合消息"重新打开"已闭合的 Kline。

```sql
-- UPSERT 条件（两处，WriteKlines + writeKlinesTx）
WHERE (klines.is_closed = false AND EXCLUDED.revision >= klines.revision)
   OR (klines.is_closed = true
       AND EXCLUDED.revision > klines.revision
       AND EXCLUDED.is_closed = true)
```

### 校验规则

| 字段 | 规则 |
|---|---|
| `interval` | 必须匹配 Input 定义的 interval |
| `close_time` | 必须 > `open_time` |
| `open`, `high`, `low`, `close` | 正数，`high >= low`，`open`/`close` 在 `[low, high]` 内 |
| `volume` | 非负数 |
| `quote_volume` | 非空时必须为非负数 |
| `trade_count` | >= 0 |
| `revision` | >= 0 |

---

## 3. OrderBook Delta

**Topic 模式**：`md.<exchange>.<market_type>.<symbol>.orderbook_delta`

### Go 结构体

```go
type orderBookDeltaMessage struct {
    EventTime        flexTime    `json:"event_time"`          // 必填
    ExchangeTime     *flexTime   `json:"exchange_time"`       // 可选
    LocalReceiveTime *flexTime   `json:"local_receive_time"`  // 可选
    RawEventID       flexString  `json:"raw_event_id"`        // 可选
    FirstUpdateID    *flexInt64  `json:"first_update_id"`     // 可选
    LastUpdateID     *flexInt64  `json:"last_update_id"`      // 可选
    PrevUpdateID     *flexInt64  `json:"prev_update_id"`      // 可选
    Sequence         *flexInt64  `json:"sequence"`            // 可选，优先于 update_id
    Bids             priceLevels `json:"bids"`                 // 必填（与 asks 至少一个非空）
    Asks             priceLevels `json:"asks"`                 // 必填（与 bids 至少一个非空）
}
```

### 完整示例

```json
{
  "event_time":       "2026-06-07T10:00:00Z",
  "exchange_time":    "2026-06-07T10:00:00.001Z",
  "raw_event_id":     "delta-1000",
  "sequence":         1000,
  "last_update_id":   1000,
  "prev_update_id":   999,
  "first_update_id":  1000,
  "bids": [
    ["50001.0", "2.0"],
    {"price": "100.0", "quantity": "0"}
  ],
  "asks": [
    {"price": "50005.0", "quantity": "1.0"},
    {"price": "50010.0", "quantity": "3.0"}
  ]
}
```

### 最小示例

```json
{"event_time":"2026-06-07T10:00:00Z","raw_event_id":"delta-1000","sequence":1000,"last_update_id":1000,"prev_update_id":999,"first_update_id":1000,"bids":[["50001.0","2.0"],["100.0","0"],["100.0","0.0"]],"asks":[{"price":"50005.0","quantity":"1.0"},{"price":"50010.0","quantity":"3.0"}]}
```

### 档位格式

Bids/asks 支持两种表示，可在同一消息中混用：

**对象形式**：
```json
{"price": "100.0", "quantity": "2.0"}
```

**紧凑二元数组形式**：
```json
["100.0", "2.0"]
```

`quantity = "0"` 或 `"0.0"` 表示移除该价位。

### 连续性校验

按以下优先级依次检查 delta 是否连续：

| 优先级 | 条件 | 通过条件 |
|---|---|---|
| 1（最高） | `delta.Sequence != nil && current.Sequence != nil` | `delta.Sequence == current.Sequence + 1` |
| 2 | `delta.FirstUpdateID != nil && current.LastUpdateID != nil` | `delta.FirstUpdateID == current.LastUpdateID + 1` |
| 3 | `delta.PrevUpdateID != nil && current.LastUpdateID != nil` | `delta.PrevUpdateID == current.LastUpdateID` |
| 4（最低） | 无可用信息 | 接受（best-effort） |

### Gap 恢复流程

```
delta 遇到 sequence gap
        ↓
worker exit (offset 不提交)
        ↓
reconcile 检测到退出，标记 gapExited
        ↓
delta worker 不重启，等待引用快照
        ↓
引用快照到达 → orderBook 恢复 ready
        ↓
reconcile 重启 delta worker，携带 snapshot 的 sequence/lastUpdateID
        ↓
worker 用 snapshot 值覆盖 DB 中旧的 progress
        ↓
gap 消息因不连续被跳过（commit，不阻止后续消费）
        ↓
正常消费恢复
```

### 校验规则

| 字段 | 规则 |
|---|---|
| `event_time` | 必填 |
| `first_update_id`, `last_update_id`, `prev_update_id`, `sequence` | 有值时必须 >= 0 |
| `first_update_id` | <= `last_update_id`（两者均存在时） |
| `bids` + `asks` | 至少一个非空 |
| 档位 `price` | 正数 |
| 档位 `quantity` | 非负数 |

---

## 4. OrderBook Snapshot（引用快照）

**Topic 模式**：`md.<exchange>.<market_type>.<symbol>.orderbook_snapshot`

由上游适配器定时推送完整的点阵快照。被已建立的 delta 流覆盖的市场可以不推送，依赖本地快照生成。

### Go 结构体

```go
type orderBookSnapshotMessage struct {
    EventTime        flexTime    `json:"event_time"`          // 必填
    ExchangeTime     *flexTime   `json:"exchange_time"`       // 可选
    LocalReceiveTime *flexTime   `json:"local_receive_time"`  // 可选
    Sequence         *flexInt64  `json:"sequence"`            // 可选
    Bids             priceLevels `json:"bids"`                 // 必填（与 asks 至少一个非空）
    Asks             priceLevels `json:"asks"`                 // 必填（与 bids 至少一个非空）
}
```

### 完整示例

```json
{
  "event_time":    "2026-06-07T10:00:00Z",
  "sequence":      1000,
  "bids": [
    {"price": "50000.0", "quantity": "10.0"}
  ],
  "asks": [
    {"price": "49990.0", "quantity": "5.0"}
  ]
}
```

### 最小示例

```json
{"event_time":"2026-06-07T10:00:00Z","sequence":1000,"bids":[{"price":"50000.0","quantity":"10.0"}],"asks":[{"price":"49990.0","quantity":"5.0"}]}
```

### 档位校验

引用快照的 bids/asks **必须按价格排序**，否则快照被拒绝且 offset 不提交：

- Bids：**降序**（最高买价在前）
- Asks：**升序**（最低卖价在前）

排序比较使用 `big.Float`，256-bit 精度。

### Apply 流程

```
snapshotInputWorker 收到消息
        ↓
解码 + 校验（排序、正负）
        ↓
发送到 worker.snapshotChan
        ↓
snapshotReceiveLoop 接收
        ↓
applySnapshot():
  1. 在锁外校验排序（不污染当前状态）
  2. 替换 bids/asks map
  3. 重置 sequence / lastUpdateID
  4. 设置 ready = true
  5. drainPending() 立即衔接已缓存的 delta
        ↓
close(result.done) → snapshotInputWorker commit offset
```

排序校验失败时 `result.done` **不被关闭**，snapshotInputWorker **不 commit** offset。

---

## Topic 命名

系统通过 API 为每个 MarketGroup 配置 Topic，名称不限。推荐约定：

```
md.<exchange>.<market_type>.<symbol>.<stream_kind>
```

| 组件 | 说明 |
|---|---|
| `exchange` | 交易所标识，如 `binance`、`okx` |
| `market_type` | `spot`、`future`、`option`（不限制） |
| `symbol` | 交易对，如 `BTCUSDT` |
| `stream_kind` | `trade`、`kline`、`orderbook_delta`、`orderbook_snapshot` |

### Kafka Consumer Group

每个 input 使用独立的 consumer group，group ID 为：

```
md.<group_id>.<stream_key>
```

例如：`md.binance:spot:BTCUSDT.trade`

---

## 常见错误

| 错误信息 | 原因 |
|---|---|
| `decode trade: json: unknown field "xxx"` | 消息包含未定义的字段 |
| `decode trade: multiple JSON values` | 一个消息中包含多个 JSON 对象 |
| `trade: missing event_time` | `event_time` 字段缺失或为 null |
| `trade: invalid price "0"` | price 必须为正数 |
| `trade: invalid side "xxx"` | side 不是 buy/b/sell/s/bid/ask 之一 |
| `kline: close_time must be after open_time` | close_time ≤ open_time |
| `kline: high must be greater than or equal to low` | high < low |
| `orderbook delta: first_update_id must not exceed last_update_id` | first > last |
| `orderbook delta: bids and asks cannot both be empty` | delta 必须至少有 bids 或 asks 之一 |
| `orderbook delta sequence gap: expected sequence N, got M` | delta 不连续，等待引用快照 |
| `orderbook snapshot: snapshot ordering: bids[0].price < bids[1].price` | 引用快照 bids 未按降序排列 |
| `orderbook state: pending delta buffer full (10000)` | 引用快照长期缺失，pending 达上限 |

所有 decode/validate 错误（非 gap 类）会写入 `stream_poison_records` 表，offset 提交后继续消费。

---

## 5. HTTP API（控制面）

系统通过 `md-control-plane` 进程暴露 RESTful HTTP API，支持市场组（MarketGroup）的生命周期管理和已持久化的行情数据查询。以下文档供 Provider 适配器／上游系统对接使用。

### 5.1 端口分离

| 端口 | 默认 | 用途 |
|---|---|---|
| `http_addr` | `:8080` | 业务 API（组管理 + 行情查询） |
| `metrics_addr` | `:9090` | 健康检查 + 就绪检查 + Prometheus 指标 |

### 5.2 通用约定

- **Content-Type**: `application/json`
- **请求 body** 使用 `json.Decoder.DisallowUnknownFields()`，未知字段返回 400。
- **路径参数**: 使用 Go 1.22+ ServeMux 路径通配符 `{group_id}`、`{stream_key}`。
- **`group_id`** 格式: `<exchange>:<market_type>:<symbol>`，如 `binance:spot:BTCUSDT`。
- **`stream_key`** 格式:

| stream_kind | stream_key |
|---|---|
| `trade` | `trade` |
| `kline` | `kline_<interval>`，如 `kline_1m` |
| `orderbook_delta` | `orderbook_delta` |
| `orderbook_snapshot` | `orderbook_snapshot` |

- **`desired_status`** 枚举: `running` / `paused` / `disabled`
- **`actual_status`**（runtime 报告）枚举: `pending` / `starting` / `running` / `paused` / `error` / `stopped`
- **`market_type`** 枚举: `spot` / `margin` / `futures` / `swap` / `option`

---

#### 5.2.1 Group Management

##### POST /groups — 创建市场组

**请求 body：**

```json
{
  "exchange":       "binance",
  "market_type":    "spot",
  "symbol":         "BTCUSDT",
  "base_asset":     "BTC",
  "quote_asset":    "USDT",
  "weight":         1,
  "desired_status": "running",
  "inputs": [
    {
      "stream_kind":    "trade",
      "kafka_topic":    "md.binance.spot.BTCUSDT.trade",
      "kafka_group_id": "my-adapter-group",
      "kafka_cluster":  "default"
    }
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `exchange` | string | 是 | 交易所标识 |
| `market_type` | string | 是 | 须为 `spot`/`margin`/`futures`/`swap`/`option` |
| `symbol` | string | 是 | 交易对，如 `BTCUSDT` |
| `base_asset` | string | 否 | 基础资产 |
| `quote_asset` | string | 否 | 计价资产 |
| `weight` | int | 否 | 负载权重，默认 1 |
| `desired_status` | string | 否 | 默认 `running` |
| `inputs` | array | 否 | 初始 input 列表，每个元素见 input 结构 |

**响应** `201 Created`：返回完整的 groupResponse（见 GET /groups/{group_id}）。

---

##### GET /groups — 列出所有市场组

**响应** `200 OK`：

```json
[
  {
    "group_id":       "binance:spot:BTCUSDT",
    "exchange":       "binance",
    "market_type":    "spot",
    "symbol":         "BTCUSDT",
    "base_asset":     "BTC",
    "quote_asset":    "USDT",
    "desired_status": "running",
    "weight":         1,
    "lease": {
      "node_id":          "runtime-pod-1",
      "lease_expires_at": "2026-06-08T12:00:30Z",
      "version":          3,
      "active":           true
    },
    "inputs": [
      {
        "input_id":       "binance:spot:BTCUSDT:trade",
        "stream_key":     "trade",
        "stream_kind":    "trade",
        "kafka_topic":    "md.binance.spot.BTCUSDT.trade",
        "kafka_group_id": "md-runtime.binance:spot:BTCUSDT.trade",
        "desired_status": "running",
        "runtime": {
          "actual_status":    "running",
          "node_id":          "runtime-pod-1",
          "kafka_lag":        42,
          "committed_offset": 5099,
          "last_error":       "",
          "updated_at":       "2026-06-08T11:59:30Z"
        }
      }
    ],
    "created_at": "2026-06-07T10:00:00Z",
    "updated_at": "2026-06-08T11:50:00Z"
  }
]
```

**groupResponse 字段说明：**

| 字段 | 类型 | 说明 |
|---|---|---|
| `group_id` | string | `<exchange>:<market_type>:<symbol>` |
| `exchange` | string | |
| `market_type` | string | |
| `symbol` | string | |
| `base_asset` | string | 可选 |
| `quote_asset` | string | 可选 |
| `desired_status` | string | 意图状态 |
| `weight` | int | |
| `lease` | object | 当前租约，组未被认领时为 `null` |
| `lease.node_id` | string | 持有该组的 runtime 节点 |
| `lease.lease_expires_at` | RFC3339 | 租约到期时间 |
| `lease.version` | int64 | 租约版本号 |
| `lease.active` | bool | 当前是否有效（未过期） |
| `inputs` | array | 该组下的所有 input |
| `inputs[].input_id` | string | `<group_id>:<stream_key>` |
| `inputs[].stream_key` | string | |
| `inputs[].stream_kind` | string | |
| `inputs[].interval` | string | kline 专用 |
| `inputs[].kafka_topic` | string | |
| `inputs[].kafka_group_id` | string | |
| `inputs[].desired_status` | string | |
| `inputs[].runtime` | object | 运行时状态，未报告时为 `{"actual_status": "pending"}` |
| `inputs[].runtime.actual_status` | string | `pending`/`starting`/`running`/`paused`/`error`/`stopped` |
| `inputs[].runtime.node_id` | string | |
| `inputs[].runtime.kafka_lag` | int64 | |
| `inputs[].runtime.committed_offset` | int64 | |
| `inputs[].runtime.last_error` | string | |
| `inputs[].runtime.updated_at` | RFC3339 | |
| `created_at` | RFC3339 | |
| `updated_at` | RFC3339 | |

---

##### GET /groups/{group_id} — 获取单个市场组

**路径参数：**

| 参数 | 说明 |
|---|---|
| `group_id` | `binance:spot:BTCUSDT` |

**响应** `200 OK`：返回单个 groupResponse（同上）。

**错误**：`404 Not Found` — 组不存在。

---

##### POST /groups/{group_id}/pause — 暂停组

将 desired_status 设为 `paused`。组内所有 input 的 runtime 节点将停止消费。

**响应** `200 OK`：返回该组的 groupResponse。

---

##### POST /groups/{group_id}/resume — 恢复组

将 desired_status 设为 `running`。

**响应** `200 OK`：返回该组的 groupResponse。

---

##### POST /groups/{group_id}/disable — 禁用组

将 desired_status 设为 `disabled`。runtime 释放租约后不再认领该组。

**响应** `200 OK`：返回该组的 groupResponse。

---

##### DELETE /groups/{group_id} — 删除组

**响应** `200 OK`：

```json
{"group_id": "binance:spot:BTCUSDT", "status": "deleted"}
```

**错误**：`404 Not Found` — 组不存在。

---

#### 5.2.2 Input Management

##### POST /groups/{group_id}/inputs — 添加 Input

**请求 body：**

```json
{
  "stream_kind":    "kline",
  "interval":       "1m",
  "kafka_topic":    "md.binance.spot.BTCUSDT.kline",
  "kafka_group_id": "my-adapter-group",
  "kafka_cluster":  "default",
  "desired_status": "running"
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `stream_key` | string | 互斥① | 直接指定 stream_key |
| `stream_kind` | string | 互斥① | `trade`/`kline`/`orderbook_delta`/`orderbook_snapshot` |
| `interval` | string | ② | kline 必填，如 `1m`/`5m`/`15m`/`30m`/`1h`/`4h`/`1d`/`1w`/`1M` |
| `kafka_topic` | string | 是 | Kafka topic 名称 |
| `kafka_group_id` | string | 否 | 默认 `md-runtime.<group_id>.<stream_key>` |
| `kafka_cluster` | string | 否 | 集群标识，默认 `default` |
| `desired_status` | string | 否 | 默认 `running` |

> ① `stream_key` 和 `stream_kind` 必须填其一。两者同时提供时须保持一致。
> ② `stream_kind=kline` 时 `interval` 必填；其他 kind 不允许带 interval。

**响应** `201 Created`：返回单条 inputResponse（结构见 GET /groups/{group_id}/inputs）。

**错误**：

| 状态码 | 场景 |
|---|---|
| 400 | 缺少必填字段、stream_key 不合法、interval 冲突 |
| 404 | group_id 不存在 |
| 409 | stream_key 已存在 |

---

##### GET /groups/{group_id}/inputs — 列出 Input

**响应** `200 OK`：

```json
[
  {
    "input_id":       "binance:spot:BTCUSDT:trade",
    "stream_key":     "trade",
    "stream_kind":    "trade",
    "kafka_topic":    "md.binance.spot.BTCUSDT.trade",
    "kafka_group_id": "md-runtime.binance:spot:BTCUSDT.trade",
    "desired_status": "running",
    "runtime": {
      "actual_status":    "running",
      "node_id":          "runtime-pod-1",
      "kafka_lag":        42,
      "committed_offset": 5099,
      "last_error":       "",
      "updated_at":       "2026-06-08T11:59:30Z"
    }
  }
]
```

---

##### POST /groups/{group_id}/inputs/{stream_key}/pause — 暂停 Input

将指定 input 的 desired_status 设为 `paused`。

**路径参数：**

| 参数 | 说明 |
|---|---|
| `group_id` | 市场组 ID |
| `stream_key` | `trade` / `kline_1m` / `orderbook_delta` / `orderbook_snapshot` |

**响应** `200 OK`：

```json
{"group_id": "binance:spot:BTCUSDT", "stream_key": "trade", "desired_status": "paused"}
```

---

##### POST /groups/{group_id}/inputs/{stream_key}/resume — 恢复 Input

将指定 input 的 desired_status 设为 `running`。

**响应** `200 OK`：

```json
{"group_id": "binance:spot:BTCUSDT", "stream_key": "trade", "desired_status": "running"}
```

---

#### 5.2.3 Market Data Query（M8）

查询端点均返回已持久化的历史行情数据。所有端点需要 `from` 和 `to` 时间范围参数。

**通用查询参数（query string）：**

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `from` | string | 是 | 起始时间，RFC3339，如 `2026-06-07T10:00:00Z` |
| `to` | string | 是 | 结束时间，RFC3339，必须晚于 `from` |
| `limit` | int | 否 | 返回行数上限，默认 200，最大 1000 |
| `cursor` | string | 否 | 复合 keyset 游标，用于翻页 |

**通用响应格式：**

```json
{
  "rows": [...],
  "next_cursor": "cursor_string"
}
```

`next_cursor` 为空字符串表示已无更多数据。

---

##### GET /markets/{group_id}/trades — 查询 Trades

**路径参数：**

| 参数 | 说明 |
|---|---|
| `group_id` | `binance:spot:BTCUSDT` |

**响应行结构：**

```json
{
  "id":                12345,
  "group_id":          "binance:spot:BTCUSDT",
  "input_id":          "binance:spot:BTCUSDT:trade",
  "event_time":        "2026-06-07T10:00:00Z",
  "exchange_time":     "2026-06-07T10:00:00.001Z",
  "local_receive_time":"2026-06-07T10:00:00.002Z",
  "trade_id":          "12345678",
  "raw_trade_id":      "trade-001",
  "price":             "50000.00",
  "quantity":          "1.5",
  "side":              "buy",
  "is_aggregated":     false,
  "kafka_topic":       "md.binance.spot.BTCUSDT.trade",
  "kafka_partition":   0,
  "kafka_offset":      5099,
  "schema_version":    1,
  "ingested_at":       "2026-06-07T10:00:00.003Z"
}
```

---

##### GET /markets/{group_id}/klines — 查询 Klines

**路径参数：**

| 参数 | 说明 |
|---|---|
| `group_id` | `binance:spot:BTCUSDT` |

**响应行结构：**

```json
{
  "group_id":     "binance:spot:BTCUSDT",
  "input_id":     "binance:spot:BTCUSDT:kline_1m",
  "source":       "exchange",
  "interval":     "1m",
  "open_time":    "2026-06-07T10:00:00Z",
  "close_time":   "2026-06-07T10:01:00Z",
  "open":         "100.0",
  "high":         "200.0",
  "low":          "50.0",
  "close":        "150.0",
  "volume":       "1000.0",
  "quote_volume": "150000.0",
  "trade_count":  42,
  "is_closed":    true,
  "revision":     1,
  "kafka_topic":  "md.binance.spot.BTCUSDT.kline",
  "kafka_partition": 0,
  "kafka_offset": 300,
  "updated_at":   "2026-06-07T10:01:00.5Z"
}
```

---

##### GET /markets/{group_id}/orderbook/snapshots — 查询 OrderBook Snapshots

**路径参数：**

| 参数 | 说明 |
|---|---|
| `group_id` | `binance:spot:BTCUSDT` |

**响应行结构：**

```json
{
  "snapshot_id":    1,
  "group_id":       "binance:spot:BTCUSDT",
  "input_id":       "binance:spot:BTCUSDT:orderbook_snapshot",
  "snapshot_time":  "2026-06-07T10:00:00Z",
  "sequence":       1000,
  "bids": [
    {"price": "50000.0", "quantity": "10.0"},
    {"price": "49990.0", "quantity": "5.0"}
  ],
  "asks": [
    {"price": "50005.0", "quantity": "8.0"},
    {"price": "50010.0", "quantity": "3.0"}
  ],
  "created_at":     "2026-06-07T10:00:00.1Z"
}
```

---

#### 5.2.4 Telemetry

以下端点监听在 `metrics_addr`（默认 `:9090`）上。

##### GET /healthz — 存活检查

**响应** `200 OK`：

```json
{"status": "ok"}
```

##### GET /readyz — 就绪检查

检查数据库连通性并确认 schema 版本 >= 6。

**响应** `200 OK`：

```json
{"status": "ready"}
```

**响应** `503 Service Unavailable`（未就绪）：

```json
{"status": "not ready", "error": "schema version 0, want >= 6"}
```

##### GET /metrics — Prometheus 指标

Prometheus 文本格式（`text/plain; version=0.0.4`）。

**预定义指标：**

| 名称 | 类型 | 说明 |
|---|---|---|
| `marketdata_runtime_heartbeat_age_seconds` | gauge | 距离上次心跳的秒数 |
| `marketdata_runtime_owned_groups` | gauge | 当前持有的市场组数量 |
| `marketdata_runtime_processed_messages_total` | counter | 已处理的 Kafka 消息总数 |
| `marketdata_runtime_write_errors_total` | counter | 数据库写入失败次数 |
| `marketdata_runtime_decode_errors_total` | counter | 消息解码/校验失败次数 |
| `marketdata_runtime_lease_renew_failures_total` | counter | 租约续期失败次数 |
| `marketdata_storage_write_latency_ms` | histogram | 存储写入延迟（毫秒） |

所有指标携带静态标签 `service="md-control-plane"` 或 `service="md-stream-runtime"`。

---

#### 5.2.5 通用错误响应

所有错误响应格式统一为：

```json
{"error": "description"}
```

| HTTP 状态码 | 场景 |
|---|---|
| `400 Bad Request` | 请求 body 解析失败、缺少必填字段、字段值不合法 |
| `404 Not Found` | group_id / stream_key 不存在 |
| `409 Conflict` | 重复创建（exchange+market_type+symbol 已存在或 input stream_key 已存在） |
| `500 Internal Server Error` | 服务端内部错误（不泄露详情，仅返回 `"internal error"`） |
