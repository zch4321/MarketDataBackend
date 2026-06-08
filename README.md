# MarketDataBackend

> 行情数据后端 — 接收上游适配器推送的 Kafka 消息，解码校验后持久化到 PostgreSQL，对外提供 REST API 查询 Trade / Kline / OrderBook 行情数据。

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## 架构

```
                        ┌──────────────────────┐
   Provider ── Kafka ──▶│  md-stream-runtime   │──▶ PostgreSQL
                        │  (lease 调度 + 消费)  │    (行情数据)
                        └──────────────────────┘
                                  │
                        ┌─────────▼────────────┐
   API Consumer ── HTTP │   md-control-plane   │──▶ PostgreSQL
                        │  (REST API :8080)    │    (元数据)
                        └──────────────────────┘
```

两个独立二进制，共享一个 PostgreSQL 数据库：
- **md-control-plane** — 管理 market group 元数据，提供行情查询 API
- **md-stream-runtime** — 基于租约的分布式消费者，从 Kafka 拉取消息并批量写入

## 特性

- **四种行情数据类型** — Trade、Kline、OrderBook Delta、OrderBook Snapshot
- **分布式调度** — 基于 PostgreSQL 租约的多节点自动选主与故障转移
- **批量写入** — Trade / Kline / Delta 各自独立的批量缓冲与定时刷盘
- **Dead-letter 机制** — 非法消息不丢失，写入 `stream_poison_records` 表
- **游标分页查询** — trades / klines / snapshots 均支持 `from`/`to`/`limit`/`cursor`
- **单机 + K8s 双模式** — `docker-compose up` 即可本地开发，`kubectl apply -f k8s/` 上集群

## 快速开始

### 方式一：docker-compose（推荐本地开发）

```powershell
# 1. 准备环境变量
cp .env.example .env

# 2. 启动 PG + Kafka
docker compose up -d --wait

# 3. 配置应用
cp config.example.yaml config.yaml

# 4. 运行迁移 + 启动控制面
go run ./cmd/md-control-plane --migrate
go run ./cmd/md-control-plane &

# 5. 启动流式运行时
go run ./cmd/md-stream-runtime &

# 6. 验证
curl http://localhost:8080/groups
```

或者用环境变量运行（跳过 config.yaml）：

```powershell
$env:DATABASE_DSN = "postgres://marketdata:marketdata@127.0.0.1:15432/marketdata?sslmode=disable"
$env:KAFKA_BROKERS = "127.0.0.1:19090"
$env:MODE = "control-plane"
go run ./cmd/md-control-plane --migrate
go run ./cmd/md-control-plane
```

### 方式二：Kubernetes

```powershell
# 构建镜像
docker build --target md-control-plane  -t md-control-plane:latest  .
docker build --target md-stream-runtime -t md-stream-runtime:latest .

# 全量部署（命名空间 + PG + Kafka + 应用）
kubectl apply -f k8s/

# 等待就绪
kubectl -n marketdata wait --for=condition=ready pod --all --timeout=300s

# 转发 API
kubectl -n marketdata port-forward svc/md-control-plane 8080:8080

# 验证
curl http://localhost:8080/groups
```

## 项目结构

```
├── cmd/
│   ├── md-control-plane/    # 控制面入口
│   └── md-stream-runtime/   # 流式运行时入口
├── internal/
│   ├── api/                 # REST API 处理与 DTO
│   ├── config/              # 配置加载（YAML + 环境变量）
│   ├── db/                  # pgx 连接池 + 迁移引擎
│   ├── kafka/               # Kafka Reader 工厂
│   ├── logging/             # slog JSON/text 封装
│   ├── metadata/            # 元数据存储（groups/inputs/leases）
│   ├── model/               # 领域模型与枚举
│   ├── observability/       # /healthz /readyz /metrics
│   ├── runtime/             # 租约调度、消费者 Worker、解码器
│   └── storage/             # 行情数据持久化与查询
├── migrations/              # PostgreSQL 迁移脚本
├── k8s/                     # Kubernetes 部署清单
├── docs/
│   ├── api.md               # REST API 文档
│   ├── kafka-message-format.md  # Kafka 消息格式规范
│   └── prd-design.md        # 产品设计文档
├── docker-compose.yml       # 本地依赖编排
├── Dockerfile               # 多阶段构建
├── config.example.yaml      # 配置示例
└── .env.example             # 环境变量示例
```

## 文档

| 文档 | 说明 |
|---|---|
| [API 文档](docs/api.md) | 完整 REST API 参考（Group/Input CRUD + 行情查询） |
| [Kafka 消息格式](docs/kafka-message-format.md) | Provider 推送消息的 JSON schema |
| [产品设计](docs/prd-design.md) | 系统设计决策与里程碑规划 |
| [config.example.yaml](config.example.yaml) | 完整配置项及默认值 |
| [.env.example](.env.example) | docker-compose 环境变量 |

## 配置

三层优先级（低 → 高）：

```
defaults  <  config.yaml  <  环境变量
```

关键环境变量：

| 变量 | 说明 | 默认值 |
|---|---|---|
| `DATABASE_DSN` | PostgreSQL 连接串 | —（必填） |
| `KAFKA_BROKERS` | Kafka bootstrap 地址，逗号分隔 | `127.0.0.1:19090` |
| `MODE` | `control-plane` / `stream-runtime` | `control-plane` |
| `HTTP_ADDR` | API 监听地址 | `:8080` |
| `METRICS_ADDR` | Health / metrics 监听地址 | `:9090` |
| `RUNTIME_NODE_ID` | 运行时节点标识（留空自动生成） | `""` |
| `RUNTIME_MAX_GROUPS` | 单节点最大 group 数 | `10` |
| `LEASE_TTL` | 租约超时 | `30s` |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` | `info` |

完整配置项见 [config.example.yaml](config.example.yaml)。

## 开发

```powershell
# 运行所有测试
go test ./...

# 格式化
go fmt ./...

# 应用迁移
go run ./cmd/md-control-plane --migrate

# 回滚迁移
go run ./cmd/md-control-plane --migrate-down
```

## 规划

- [x] M0–M10：Trade / Kline / OrderBook 数据采集、持久化、查询
- [ ] **md-data-relay** — 独立数据网关，对外提供 HTTP 推送端点，内部转写 Kafka，解耦 provider 对 Kafka 协议的依赖
- [ ] 行情数据指标计算（VWAP、买卖不平衡度等）
- [ ] gRPC 流式推送

## License

[Apache 2.0](LICENSE)
