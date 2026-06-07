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
