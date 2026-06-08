# Market Data Backend — REST API

> 适用版本：M10（2026-06-08）
> Base URL：`http://<host>:8080`

## 概述

控制面（md-control-plane）提供两组端点：

| 分组 | 职责 |
|---|---|
| **Group / Input 管理** | 创建、查询、暂停、恢复、删除 market group 及其 input 流 |
| **行情数据查询** | 按时间范围查询已持久化的 trade、kline、orderbook snapshot |

所有请求体为 `application/json`，响应体为 `application/json`。
非法 JSON（未知字段、尾部数据）返回 `400 Bad Request`。
请求体最大 1 MiB。

---

## 枚举值

### market_type

| 值 | 含义 |
|---|---|
| `spot` | 现货 |
| `margin` | 保证金 |
| `futures` | 期货 |
| `swap` | 永续合约 |
| `option` | 期权 |

### stream_kind

| 值 | 含义 | stream_key 格式 |
|---|---|---|
| `trade` | 逐笔成交 | `trade` |
| `kline` | K 线 | `kline_<interval>`，例如 `kline_1m` |
| `orderbook_delta` | 订单簿增量 | `orderbook_delta` |
| `orderbook_snapshot` | 订单簿引用快照 | `orderbook_snapshot` |

### desired_status

| 值 | 含义 |
|---|---|
| `running` | 运行中 |
| `paused` | 已暂停 |
| `disabled` | 已禁用 |

### actual_status（运行时观测状态）

| 值 | 含义 |
|---|---|
| `pending` | 等待分配 |
| `starting` | 启动中 |
| `running` | 运行中 |
| `paused` | 已暂停 |
| `error` | 错误 |
| `stopped` | 已停止 |

### trade side

| 值 | 含义 |
|---|---|
| `buy`、`b`、`bid` | 买方 |
| `sell`、`s`、`ask` | 卖方 |

---

## 标识符约定

| 标识符 | 格式 | 示例 |
|---|---|---|
| `group_id` | `<exchange>:<market_type>:<symbol>` | `binance:spot:BTCUSDT` |
| `input_id` | `<group_id>:<stream_key>` | `binance:spot:BTCUSDT:trade` |
| `stream_key` | `trade` / `orderbook_delta` / `orderbook_snapshot` / `kline_<interval>` | `kline_1m` |

---

## 1. Group 管理

### 1.1 创建 Group

```
POST /groups
```

**请求体**

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `exchange` | string | **是** | 交易所名称 |
| `market_type` | string | **是** | 市场类型，见枚举 |
| `symbol` | string | **是** | 交易对符号 |
| `base_asset` | string | 否 | 基础资产 |
| `quote_asset` | string | 否 | 计价资产 |
| `weight` | int | 否 | 调度权重，默认 0 |
| `desired_status` | string | 否 | 目标状态，见枚举 |
| `inputs` | array | 否 | 初始 input 列表，见 [Input 对象](#input-对象) |

**示例**

```json
{
  "exchange": "binance",
  "market_type": "spot",
  "symbol": "BTCUSDT",
  "base_asset": "BTC",
  "quote_asset": "USDT",
  "desired_status": "running",
  "inputs": [
    {
      "stream_kind": "trade",
      "kafka_topic": "md.binance.spot.btcusdt.trade"
    },
    {
      "stream_kind": "kline",
      "interval": "1m",
      "kafka_topic": "md.binance.spot.btcusdt.kline"
    }
  ]
}
```

**响应** `201 Created`

返回合并后的 group 视图，包含 lease 和 per-input 运行时状态。见 [Group 响应对象](#group-响应对象)。

**错误**

| 状态码 | 说明 |
|---|---|
| `400` | 缺少 exchange / market_type / symbol，或枚举值无效 |
| `409` | group_id 已存在 |

---

### 1.2 列出所有 Group

```
GET /groups
```

**查询参数**：无

**响应** `200 OK`

```json
[
  {
    "group_id": "binance:spot:BTCUSDT",
    "exchange": "binance",
    "market_type": "spot",
    "symbol": "BTCUSDT",
    "desired_status": "running",
    "weight": 0,
    "created_at": "2026-06-08T12:00:00Z",
    "updated_at": "2026-06-08T12:00:00Z"
  }
]
```

> 列表视图不含 `lease` 和 `inputs` 详情，需要用 `GET /groups/{group_id}` 获取。

---

### 1.3 获取单个 Group 详情

```
GET /groups/{group_id}
```

**响应** `200 OK` — 返回 [Group 响应对象](#group-响应对象)

**错误**

| 状态码 | 说明 |
|---|---|
| `404` | group 不存在 |

---

### 1.4 暂停 Group

```
POST /groups/{group_id}/pause
```

将 group 下所有 input 的 `desired_status` 设为 `paused`。

**请求体**：无

**响应** `200 OK` — 返回更新后的 [Group 响应对象](#group-响应对象)

---

### 1.5 恢复 Group

```
POST /groups/{group_id}/resume
```

将 group 下所有 input 的 `desired_status` 设为 `running`。

**请求体**：无

**响应** `200 OK`

---

### 1.6 禁用 Group

```
POST /groups/{group_id}/disable
```

将 group 下所有 input 的 `desired_status` 设为 `disabled`。

**请求体**：无

**响应** `200 OK`

---

### 1.7 删除 Group

```
DELETE /groups/{group_id}
```

**请求体**：无

**响应** `200 OK`

```json
{
  "group_id": "binance:spot:BTCUSDT",
  "status": "deleted"
}
```

---

## 2. Input 管理

### Input 对象

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `stream_key` | string | 否* | 流标识，如 `trade`、`kline_1m` |
| `stream_kind` | string | 否* | 流类型，见枚举 |
| `interval` | string | 否 | K 线周期（仅 `stream_kind=kline` 时需要） |
| `kafka_topic` | string | **是** | Kafka topic 名 |
| `kafka_group_id` | string | 否 | 消费者组 ID，默认自动生成 |
| `kafka_cluster` | string | 否 | Kafka 集群名，默认 `default` |
| `desired_status` | string | 否 | 目标状态 |

> *`stream_key` 和 `stream_kind` 至少提供一个。两者都提供时，推导出的 stream_key 必须与提供的匹配。

### 2.1 为 Group 添加 Input

```
POST /groups/{group_id}/inputs
```

**请求体**：单个 [Input 对象](#input-对象)

**示例**

```json
{
  "stream_kind": "orderbook_delta",
  "kafka_topic": "md.binance.spot.btcusdt.orderbook_delta"
}
```

**响应** `201 Created` — 返回 [Input 响应对象](#input-响应对象)

**错误**

| 状态码 | 说明 |
|---|---|
| `400` | 缺少 kafka_topic 或 stream_key/stream_kind |
| `404` | group 不存在 |

---

### 2.2 列出 Group 的所有 Input

```
GET /groups/{group_id}/inputs
```

**响应** `200 OK`

```json
[
  {
    "input_id": "binance:spot:BTCUSDT:trade",
    "stream_key": "trade",
    "stream_kind": "trade",
    "kafka_topic": "md.binance.spot.btcusdt.trade",
    "kafka_group_id": "md-runtime.binance:spot:BTCUSDT.trade",
    "desired_status": "running",
    "runtime": {
      "actual_status": "running",
      "node_id": "marketdata/md-stream-runtime-5f689b9b7b-nzh7s",
      "kafka_lag": 0,
      "committed_offset": 42,
      "updated_at": "2026-06-08T12:01:00Z"
    }
  }
]
```

---

### 2.3 暂停单个 Input

```
POST /groups/{group_id}/inputs/{stream_key}/pause
```

**示例**

```
POST /groups/binance:spot:BTCUSDT/inputs/trade/pause
```

**请求体**：无

**响应** `200 OK`

```json
{
  "group_id": "binance:spot:BTCUSDT",
  "stream_key": "trade",
  "desired_status": "paused"
}
```

---

### 2.4 恢复单个 Input

```
POST /groups/{group_id}/inputs/{stream_key}/resume
```

**响应** `200 OK`

---

## 3. 行情数据查询

所有查询端点共享相同的查询参数和响应格式，支持游标分页。

### 通用查询参数

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `from` | string (RFC3339) | **是** | 时间范围起点（含） |
| `to` | string (RFC3339) | **是** | 时间范围终点（不含），必须 > from |
| `limit` | int | 否 | 返回行数上限，默认 0（不限制），最大建议 1000 |
| `cursor` | string | 否 | 游标，用于翻页 |

### 通用响应格式

```json
{
  "rows": [...],
  "next_cursor": "eyJvZmZzZXQiOjEwMH0="
}
```

- `rows`：数据行数组
- `next_cursor`：下一页游标，为空表示已到末尾

---

### 3.1 查询 Trades

```
GET /markets/{group_id}/trades
```

**示例**

```
GET /markets/binance:spot:BTCUSDT/trades?from=2026-06-08T00:00:00Z&to=2026-06-09T00:00:00Z&limit=200
```

**响应 `rows` 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | int64 | 自增主键 |
| `group_id` | string | 所属 group |
| `input_id` | string | 所属 input |
| `event_time` | string (RFC3339) | 事件时间 |
| `exchange_time` | string \| null | 交易所时间 |
| `local_receive_time` | string \| null | 本地接收时间 |
| `trade_id` | string | 交易 ID |
| `raw_trade_id` | string | 原始交易 ID |
| `price` | string | 价格（十进制） |
| `quantity` | string | 数量（十进制） |
| `side` | string | `buy` / `sell` |
| `is_aggregated` | bool | 是否为聚合成交 |
| `kafka_topic` | string | 来源 topic |
| `kafka_partition` | int | 来源分区 |
| `kafka_offset` | int64 | 来源 offset |
| `schema_version` | int | schema 版本 |
| `ingested_at` | string (RFC3339) | 入库时间 |

**示例响应**

```json
{
  "rows": [
    {
      "id": 1,
      "group_id": "binance:spot:BTCUSDT",
      "input_id": "binance:spot:BTCUSDT:trade",
      "event_time": "2026-06-08T12:00:00Z",
      "exchange_time": "2026-06-08T12:00:00.001Z",
      "local_receive_time": null,
      "trade_id": "12345",
      "raw_trade_id": "trade-001",
      "price": "50000.00",
      "quantity": "1.5",
      "side": "buy",
      "is_aggregated": false,
      "kafka_topic": "md.binance.spot.btcusdt.trade",
      "kafka_partition": 0,
      "kafka_offset": 42,
      "schema_version": 1,
      "ingested_at": "2026-06-08T12:00:01Z"
    }
  ],
  "next_cursor": ""
}
```

---

### 3.2 查询 Klines

```
GET /markets/{group_id}/klines
```

参数同 [通用查询参数](#通用查询参数)。

**响应 `rows` 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| `group_id` | string | 所属 group |
| `input_id` | string | 所属 input |
| `source` | string | 来源：`exchange` / `computed` |
| `interval` | string | K 线周期，如 `1m` |
| `open_time` | string (RFC3339) | 开盘时间 |
| `close_time` | string (RFC3339) | 收盘时间 |
| `open` | string | 开盘价 |
| `high` | string | 最高价 |
| `low` | string | 最低价 |
| `close` | string | 收盘价 |
| `volume` | string | 成交量 |
| `quote_volume` | string | 成交额 |
| `trade_count` | int64 | 成交笔数 |
| `is_closed` | bool | 是否已闭合 |
| `revision` | int64 | 修订版本号 |
| `kafka_topic` | string | 来源 topic |
| `kafka_partition` | int | 来源分区 |
| `kafka_offset` | int64 | 来源 offset |
| `updated_at` | string (RFC3339) | 更新时间 |

---

### 3.3 查询 OrderBook Snapshots

```
GET /markets/{group_id}/orderbook/snapshots
```

参数同 [通用查询参数](#通用查询参数)。

**响应 `rows` 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| `snapshot_id` | int64 | 快照主键 |
| `group_id` | string | 所属 group |
| `input_id` | string | 所属 input |
| `snapshot_time` | string (RFC3339) | 快照时间 |
| `sequence` | int64 \| null | 序列号 |
| `bids` | array | 买盘档位，价格从高到低 |
| `asks` | array | 卖盘档位，价格从低到高 |
| `created_at` | string (RFC3339) | 创建时间 |

**`bids` / `asks` 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| `price` | string | 价格（十进制） |
| `quantity` | string | 数量（十进制） |

---

## 4. 响应对象参考

### Group 响应对象

```json
{
  "group_id": "binance:spot:BTCUSDT",
  "exchange": "binance",
  "market_type": "spot",
  "symbol": "BTCUSDT",
  "base_asset": "BTC",
  "quote_asset": "USDT",
  "desired_status": "running",
  "weight": 0,
  "lease": {
    "node_id": "marketdata/md-stream-runtime-5f689b9b7b-nzh7s",
    "lease_expires_at": "2026-06-08T12:00:30Z",
    "version": 1,
    "active": true
  },
  "inputs": [
    {
      "input_id": "binance:spot:BTCUSDT:trade",
      "stream_key": "trade",
      "stream_kind": "trade",
      "kafka_topic": "md.binance.spot.btcusdt.trade",
      "kafka_group_id": "md-runtime.binance:spot:BTCUSDT.trade",
      "desired_status": "running",
      "runtime": {
        "actual_status": "running",
        "node_id": "marketdata/md-stream-runtime-5f689b9b7b-nzh7s",
        "kafka_lag": 0,
        "committed_offset": 100,
        "last_error": "",
        "updated_at": "2026-06-08T12:01:00Z"
      }
    }
  ],
  "created_at": "2026-06-08T12:00:00Z",
  "updated_at": "2026-06-08T12:00:00Z"
}
```

### Input 响应对象

| 字段 | 类型 | 说明 |
|---|---|---|
| `input_id` | string | input 标识 |
| `stream_key` | string | 流 key |
| `stream_kind` | string | 流类型 |
| `interval` | string | K 线周期（仅 kline） |
| `kafka_topic` | string | Kafka topic |
| `kafka_group_id` | string | 消费者组 ID |
| `desired_status` | string | 目标状态 |
| `runtime` | object \| null | 运行时观测状态（见下表） |

**`runtime` 子对象**

| 字段 | 类型 | 说明 |
|---|---|---|
| `actual_status` | string | 实际状态，见枚举 |
| `node_id` | string | 负责处理的运行时节点 ID |
| `kafka_lag` | int64 \| null | Kafka 消费延迟 |
| `committed_offset` | int64 \| null | 已提交 offset |
| `last_error` | string | 最近错误信息 |
| `updated_at` | string (RFC3339) | 状态更新时间 |

### 错误响应

```json
{
  "error": "human-readable error message"
}
```

---

## 5. 完整工作流示例

```powershell
# 1. 创建 group + input
curl -X POST http://localhost:8080/groups `
  -H "Content-Type: application/json" `
  -d '{
    "exchange": "binance",
    "market_type": "spot",
    "symbol": "BTCUSDT",
    "desired_status": "running",
    "inputs": [
      {"stream_kind": "trade", "kafka_topic": "md.binance.spot.btcusdt.trade"},
      {"stream_kind": "kline", "interval": "1m", "kafka_topic": "md.binance.spot.btcusdt.kline"}
    ]
  }'

# 2. 查 group 状态（含 lease 和 runtime 信息）
curl http://localhost:8080/groups/binance:spot:BTCUSDT

# 3. 往 Kafka 推数据后，查询 trades
curl "http://localhost:8080/markets/binance:spot:BTCUSDT/trades?from=2026-06-08T00:00:00Z&to=2026-06-09T00:00:00Z&limit=50"

# 4. 查询 klines
curl "http://localhost:8080/markets/binance:spot:BTCUSDT/klines?from=2026-06-08T00:00:00Z&to=2026-06-09T00:00:00Z"

# 5. 暂停 input
curl -X POST http://localhost:8080/groups/binance:spot:BTCUSDT/inputs/trade/pause

# 6. 恢复 input
curl -X POST http://localhost:8080/groups/binance:spot:BTCUSDT/inputs/trade/resume

# 7. 删除 group
curl -X DELETE http://localhost:8080/groups/binance:spot:BTCUSDT
```

---

## 6. 状态码汇总

| 状态码 | 说明 |
|---|---|
| `200` | 请求成功 |
| `201` | 资源创建成功 |
| `400` | 请求体格式错误或校验失败 |
| `404` | group 或 input 不存在 |
| `409` | 资源冲突（如重复创建） |
| `500` | 内部错误 |
