# metrics — 统一指标出口组件

全仓库统一的 Prometheus 指标出口：**集中注册表 + `xmt_<component>_<name>` 命名规范 + `/metrics` HTTP Handler + 组件私有指标适配器**。

- 模块路径：`github.com/xm-utils/sunmao/metrics`
- 技术选型：`prometheus/client_golang`（v1.22.0），不引入其他指标链
- 兼容策略：**不删除、不修改** `breaker` / `mq/kafka/client` / `deadletter` / `ratelimit` / `lock` 各自的私有指标实现；接入一律经 `metrics/adapter/*` 薄适配器（拉取式 `prometheus.Collector`），组件本体零依赖本包

## 目录结构

```
metrics/
├── metrics.go                 # 统一注册表、Register、Handler、Exporter、规范构造器
├── naming.go                  # xmt_<component>_<name> 命名规范与校验
├── config.go                  # Config + Init（前缀、Go/进程 collector）
├── adapter/
│   ├── breaker/               # CircuitBreaker(Metrics) / Manager 快照 → xmt_breaker_*
│   ├── kafka/                 # client.ConsumerMetrics 累计+窗口 → xmt_kafka_consumer_*
│   ├── deadletter/            # QueueMetrics.GetStats → xmt_deadletter_*（panic 安全）
│   ├── lock/                  # Statser.Stats 获取/释放/续期计数 → xmt_lock_*（panic 安全）
│   └── ratelimit/             # Local.Snapshots 判定计数+水位 → xmt_ratelimit_*（panic 安全）
└── config.example.yaml
```

## API 参考

### 初始化（可选）

```go
import "github.com/xm-utils/sunmao/metrics"

err := metrics.Init(metrics.Config{
    Namespace:            "xmt", // 留空即 xmt
    EnableProcessCollector: true,
})
```

未调用 `Init` 时即为默认配置（前缀 `xmt`、启用 Go collector），所有 API 可直接使用。`Init` 应在注册任何业务指标之前调用；重复调用会重建注册表。

### 创建规范指标

```go
fails, err := metrics.NewCounterVec("order", "handler_fail_total", "处理失败数", "topic")
depth, err := metrics.NewGaugeVec("order", "queue_depth", "积压深度", "queue")
latency, err := metrics.NewHistogramVec("order", "handle_seconds", "处理耗时", nil, "op")
snap, err := metrics.NewGaugeFunc("order", "workers", "当前 worker 数", func() float64 { return float64(n) })
```

构造即注册进统一注册表；命名非法、label 重复、同名重复注册均返回 `error`，不 panic。Vec 构造器要求至少一个 label，无 label 场景用 `NewGaugeFunc` 或原生 `prometheus.NewXxx` + `metrics.Register`。

### 暴露 /metrics

```go
http.Handle("/metrics", metrics.Handler()) // 标准库
// Gin: r.GET("/metrics", gin.WrapH(metrics.Handler()))
// grpcx：挂到 cmux 的 HTTP 端口，与 gRPC 同端口复用
```

`Handler()` 每次抓取实时解析当前注册表，`Init` 之后无需重建。

### 收录组件采集器

```go
type Exporter interface {
    MetricsCollectors() []prometheus.Collector
}

// 适配器均实现 MetricsCollectors()，两种接入方式等价：
err := metrics.RegisterExporter(breakermetrics.NewFromManager(xmbreaker.GetManager()))
err := metrics.Register(breakermetrics.NewFromManager(xmbreaker.GetManager()))
```

组件（或适配器）实现 `MetricsCollectors()` 即可被 `RegisterExporter` 收录；组件本体无需 import 本包——结构类型天然解耦。

## 组件适配器与指标清单

### breaker（`metrics/adapter/breaker`）

```go
import xmbreaker "github.com/xm-utils/sunmao/breaker"
import breakermetrics "github.com/xm-utils/sunmao/metrics/adapter/breaker"

// 单个熔断器
metrics.Register(breakermetrics.New(cb))
// 或整个管理器（推荐，动态覆盖 manager 下全部熔断器）
metrics.Register(breakermetrics.NewFromManager(xmbreaker.GetManager()))
```

| 指标 | 类型 | label |
|---|---|---|
| `xmt_breaker_requests_total` | counter | `name`, `result=success\|failure\|rejected` |
| `xmt_breaker_request_duration_seconds` | gauge | `name`, `stat=min\|avg\|max`（无成功请求时不产出） |
| `xmt_breaker_request_rate` | gauge | `name`, `kind=success\|failure\|rejection` |
| `xmt_breaker_state_change_total` | counter | `name`, `transition`（如 `closed_to_open`） |
| `xmt_breaker_last_update_timestamp_seconds` | gauge | `name` |

### kafka consumer（`metrics/adapter/kafka`）

```go
import kafkaclient "github.com/xm-utils/sunmao/mq/kafka/client"
import kafkametrics "github.com/xm-utils/sunmao/metrics/adapter/kafka"

metrics.Register(kafkametrics.New(consumer.GetMetrics(), "order-group"))
```

| 指标 | 类型 | label |
|---|---|---|
| `xmt_kafka_consumer_messages_total` / `fetches_total` / `errors_total` | counter | `consumer` |
| `xmt_kafka_consumer_rate` | gauge | `consumer`, `kind=message_avg\|message_recent\|fetch_avg\|fetch_recent` |
| `xmt_kafka_consumer_process_time_seconds` | gauge | `consumer`, `stat=avg` |
| `xmt_kafka_consumer_uptime_seconds` | gauge | `consumer` |
| `xmt_kafka_consumer_window` | gauge | `consumer`, `kind=messages\|fetches\|errors` |
| `xmt_kafka_consumer_window_process_time_seconds` | gauge | `consumer`, `stat=total\|avg` |

### deadletter（`metrics/adapter/deadletter`）

```go
import dlmetrics "github.com/xm-utils/sunmao/metrics/adapter/deadletter"

metrics.Register(dlmetrics.New(queueMetrics)) // *deadletter.QueueMetrics，须业务侧持有引用
```

抓取时实时调用 `GetStats`（含 Redis 队列长度与存储层统计）；存储层/Redis 未初始化引发的 **panic 被兜底为抓取错误**（InvalidMetric），不击穿抓取方。

| 指标 | 类型 | label |
|---|---|---|
| `xmt_deadletter_messages_total` | counter | `queue_key`, `kind=dead\|recovered\|abandoned` |
| `xmt_deadletter_queue_length` | gauge | `queue_key` |
| `xmt_deadletter_db_messages` | gauge | `queue_key`, `status=pending\|processing\|processed\|abandoned` |
| `xmt_deadletter_db_avg_retry_count` | gauge | `queue_key` |
| `xmt_deadletter_last_check_timestamp_seconds` | gauge | `queue_key`（未经巡检器更新时不产出） |

### ratelimit（`metrics/adapter/ratelimit`）

```go
import rlmetrics "github.com/xm-utils/sunmao/metrics/adapter/ratelimit"

metrics.Register(rlmetrics.New(local))            // *ratelimit.Local，拉取时读 local.Snapshots()
metrics.Register(rlmetrics.NewWithProvider(fn))   // 自定义快照源：多后端聚合、自持 SlidingWindow/LeakyBucket 实例
```

抓取时实时读快照（按 Key 的累计判定计数 + 当前令牌水位）；快照源 panic 兜底为抓取错误（InvalidMetric）。计数回退/不恒等（`Rejected > Total`、`Remaining < 0`）的异常快照被夹持，不会产出负值样本。

| 指标 | 类型 | label |
|---|---|---|
| `xmt_ratelimit_requests_total` | counter | `key`, `result=allowed\|rejected` |
| `xmt_ratelimit_rejection_rate` | gauge | `key`（累计拒绝率 0~1；`Total=0` 时不产出） |
| `xmt_ratelimit_tokens_available` | gauge | `key`（当前可用令牌） |
| `xmt_ratelimit_rule_rate` | gauge | `key`（当前生效速率，个/秒） |

> 区间拒绝率（告警用）不在指标侧计算，由 `ratelimit/alert` 直接差分快照得出，见该组件 README。

### lock（`metrics/adapter/lock`）

```go
import xmlock "github.com/xm-utils/sunmao/lock"
import lockmetrics "github.com/xm-utils/sunmao/metrics/adapter/lock"

metrics.Register(lockmetrics.New(ddlMu, recoverMu))   // 若干 lock.Statser（三端实例可混用）
metrics.Register(lockmetrics.NewWithProvider(func() map[string]xmlock.Stats {
    return lockRegistry.Snapshot()                    // 装配层自持锁注册表时用
}))
```

抓取时逐个调实例的 `Stats()`（拉取式，不包装锁本体）；provider 或单个实例 panic 各自兜底为抓取错误，**一个坏实例不会带走其他锁的样本**。负时长（外部 provider 交出脏快照）被夹持为 0，`backend` 为空时回退为 `unknown` 标签。

| 指标 | 类型 | label |
|---|---|---|
| `xmt_lock_operations_total` | counter | `key`, `backend`, `op=acquire\|release\|renew`, `result` |
| `xmt_lock_errors_total` | counter | `key`, `backend`（后端故障与误用，跨三条路径汇总）|
| `xmt_lock_duration_seconds_total` | counter | `key`, `backend`, `phase=wait\|hold` |
| `xmt_lock_contention_ratio` | gauge | `key`, `backend`（累计撞锁比例 0~1；`Attempts=0` 时不产出）|
| `xmt_lock_state` | gauge | `key`, `backend`, `field=holding\|depth\|fence\|closed` |

> `acquire` 的 `result` 取 `acquired/reentered/busy/timeout/lost/canceled`，`release` 取 `full/decreased/rejected`，`renew` 取 `ok/failed`。区间竞争率请对 `xmt_lock_operations_total` 做 `rate()` 推导；`lock` 侧字段口径见该组件 README「观测与指标」。

## 设计说明

1. **拉取式适配器**：适配器不包装、不拦截组件写入路径，仅在抓取瞬间读快照——组件私有 `metrics.go` 保持原语义，独立可引。
2. **零全局副作用**：本包不写 `prometheus.DefaultRegisterer`；需要使用默认注册表的场景请 `metrics.Registry()` 取表自行桥接。
3. **panic 转 error**：`Register` / `RegisterExporter` / 各构造器全部返回 error；`Init` 非法前缀报错并保持原配置。
4. **与 trace 的关系**：metrics 是可观测性三件套的第二块；`/metrics` 端点复用 `grpcx` cmux HTTP 端口或 `httpclient` server 侧，无需新端口。

## 测试

```bash
go test -race ./...        # 根包 + 五个适配器；deadletter/ratelimit/lock 用例可离线运行
```
