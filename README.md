# 榫卯 Sunmao

Go 微服务基础设施构件集，提供数据库访问、缓存、消息队列、服务治理、重试容错、可观测性等开箱即用的通用组件。各组件以独立 Go Module 发布，可按需引入，互不耦合。

## 环境要求

- Go 1.24.2+
- 各组件的外部依赖（MySQL、Redis、Kafka、MongoDB、Consul、Nacos 等）按实际使用场景部署

## 模块总览

| 模块路径 | 说明 | 文档 |
|---------|------|------|
| `gorm/database` | GORM 数据库工具库：泛型 CRUD、链式 Context API、一主多从读写分离（DBResolver）、条件构建器、事务 | [README](gorm/database/README.md) · [QUICKSTART](gorm/database/QUICKSTART.md) |
| `gorm/database/sharding` | 分库分表：取模分片 / 时间范围分片、Snowflake 主键、自动路由 | [README](gorm/database/sharding/README.md) · [QUICKSTART](gorm/database/sharding/QUICKSTART.md) |
| `beego/database` | Beego ORM 数据库工具库（兼容旧项目） | — |
| `redis` | Redis 客户端封装：String / Hash / List / Set / SortedSet / Stream 全数据结构操作 | [README](redis/README.md) |
| `mq/core` | 消息队列统一门面（纯契约，零第三方依赖）：Message / Handler / Producer / Consumer 抽象与多后端选型矩阵 | [README](mq/core/README.md) |
| `mq/redis` | Redis 侧适配器四形态：list 安全队列 / zset 延迟队列 / stream 消费者组 / pubsub 广播；含 `stream/queue` 队列本体（当前仅启用 Stream 基础读写，装饰器 / Pending 巡检 / 指标已注释保留） | [README](mq/redis/README.md) · [QUICKSTART](mq/redis/QUICKSTART.md) |
| `mq/kafka` | Kafka 侧适配器：手动 offset 模式生产 / 消费与额度内重投；含 `client` 客户端本体（Worker Pool、消息去重、指标监控） | [README](mq/kafka/README.md) · [client](mq/kafka/client/README.md) |
| `mq/pipequeue` | pipeline 引擎 × MQ 门面桥接队列：推送→拉取转换、终态钩子回写确认 | [README](mq/pipequeue/README.md) |
| `mongodb` | MongoDB 客户端：连接管理、泛型 CRUD、聚合管道 | [README](mongodb/README.md) · [QUICKSTART](mongodb/QUICKSTART.md) |
| `pipeline` | 泛型消息消费执行引擎：步骤链编排、错误分级、步骤超时、有界停机、同 Key 串行 | [README](pipeline/README.md) · [QUICKSTART](pipeline/QUICKSTART.md) |
| `retry` | 非阻塞重试执行器：多策略（固定 / 指数 / 线性）、批量并发、监控回调 | [README](retry/README.md) · [QUICKSTART](retry/QUICKSTART.md) |
| `breaker` | 熔断器：滑动窗口指标、状态机（Closed / Open / Half-Open）、告警、与 Retry 联动 | [README](breaker/README.md) · [QUICKSTART](breaker/QUICKSTART.md) |
| `ratelimit` | 限流与降级：令牌桶 / 滑动窗口 / 漏桶三算法、本地内存与 Redis(Lua 原子) 双后端、gRPC/HTTP 入口中间件、拒绝率超阈值联动 `breaker` 告警通道 | [README](ratelimit/README.md) · [QUICKSTART](ratelimit/QUICKSTART.md) |
| `lock` | 分布式互斥锁：统一 `Mutex` 契约 + 进程内 / Redis(Lua+PEXPIRE) / Consul(Session+KV) 三后端、租期与看门狗自动续期、goroutine 绑定重入、栅栏 token | [README](lock/README.md) · [QUICKSTART](lock/QUICKSTART.md) |
| `deadletter` | 死信队列：自定义持久化（GORM）、自动恢复重试、实时监控告警 | [README](deadletter/README.md) · [QUICKSTART](deadletter/QUICKSTART.md) |
| `consul` | Consul SDK 封装：服务注册 / 发现、健康检查、KV 配置 | [README](consul/README.md) |
| `nacos` | Nacos SDK 封装：配置管理、服务发现（闭包模式独立客户端） | [README](nacos/README.md) |
| `registry` | 注册中心/配置中心统一抽象：consul / nacos 双适配器，服务注册发现、实例 Watch、配置热更新 | [README](registry/README.md) · [QUICKSTART](registry/QUICKSTART.md) |
| `trace` | 链路追踪与指标计量：OpenTelemetry 统一装配、W3C traceparent 跨进程传播（gRPC metadata / HTTP Header / MQ 消息头）、内置拦截器与中间件 | [README](trace/README.md) · [QUICKSTART](trace/QUICKSTART.md) |
| `metrics` | 统一指标出口：`xmt_<component>_<name>` 命名规范、统一注册表与 `Handler()`（`/metrics`）、五个组件拉取式适配器（breaker / kafka / deadletter / ratelimit / lock） | [README](metrics/README.md) · [QUICKSTART](metrics/QUICKSTART.md) |
| `logging` | 统一日志出口：logrus JSON 结构化、`ctx` 自动附加 traceId、文件轮转、`SetLevel` 调级抽象（配置源监听由装配层实现） | [README](logging/README.md) · [QUICKSTART](logging/QUICKSTART.md) |
| `grpcx` | gRPC 服务工具：Server / Client 快速构建、cmux 端口复用（gRPC + HTTP）、优雅停机 | [README](grpcx/README.md) · [QUICKSTART](grpcx/QUICKSTART.md) |
| `httpclient` | HTTP 客户端：连接池、JSON / 表单 / 文件上传、请求追踪 | [README](httpclient/README.md) · [QUICKSTART](httpclient/QUICKSTART.md) |
| `common` | 通用工具函数：JWT、字符串 / 切片 / 时间 / 网络、RSA 加解密、密码哈希、统一响应体 | — |

## 架构设计

榫卯是**构件集而非框架**：下面的分层只回答「谁该依赖谁」，不要求整套引入。任何一层都可以被业务自研实现替换，组件之间没有隐式装配；全局副作用只集中在少数 `Init`（`trace` / `metrics` / `logging` / `redis` / `gorm/database`），而它们面对非法配置一律返回 error 并保持原状，不 panic。

```
┌──────────────────────── 装配层（业务仓 main.go / wire） ─────────────────────────┐
│  统一 Init 顺序 · 依赖注入 · 优雅停机编排 · 配置源监听与热更新                   │
├───────────────────────────────── 入口 / 出口层 ──────────────────────────────────┤
│  grpcx（gRPC + cmux 同端口 HTTP）    httpclient（连接池 / 追踪 / 熔断联动）      │
├────────────────────────────────── 治理与协同层 ──────────────────────────────────┤
│  ratelimit 入口限流    breaker 熔断降级    retry 非阻塞重试                      │
│  lock 跨实例互斥    pipeline 消费编排    deadletter 死信兜底                     │
├───────────────────────────── 契约层（零第三方 SDK） ─────────────────────────────┤
│  mq/core：Producer / Consumer / Handler      registry：Registry / Discovery      │
│  lock.Mutex      ratelimit.Limiter      metrics.Exporter                         │
├─────────────────────── 适配层（外部 SDK 只出现在这一层） ────────────────────────┤
│  mq/redis  mq/kafka  mq/pipequeue      registry/consul  registry/nacos           │
│  lock/redisstore  lock/consulstore  ratelimit/redisstore  metrics/adapter/*      │
├───────────────────────────────────── 设施层 ─────────────────────────────────────┤
│  redis  consul  nacos  gorm/database(+sharding)  mongodb  common                 │
├───────────────────────────────────── 贯穿线 ─────────────────────────────────────┤
│  trace（W3C 上下文） -> logging（自动附 traceId） -> metrics（/metrics）         │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 分层职责

| 层 | 构件 | 职责 | 硬约束 |
|---|---|---|---|
| 入口 / 出口 | `grpcx`、`httpclient` | 协议承载、端口复用、优雅停机、连接池 | 只依赖 `trace` 与治理件接口，不绑定具体数据后端 |
| 治理与协同 | `ratelimit`、`breaker`、`retry`、`lock`、`pipeline`、`deadletter` | 决策与编排逻辑（放行/熔断/重试/互斥/步骤链/兜底） | 根包零外部 SDK，后端与中间件拆到子包 |
| 契约 | `mq/core`、`registry`、`lock.Mutex`、`ratelimit.Limiter`、`metrics.Exporter` | 统一模型与接口，屏蔽后端差异 | 零第三方依赖，禁止往这一层加 SDK |
| 适配 | `mq/redis`、`mq/kafka`、`mq/pipequeue`、`registry/{consul,nacos}`、`lock/{redisstore,consulstore}`、`ratelimit/redisstore`、`metrics/adapter/*` | 把设施能力接到契约上 | 外部 SDK 只能出现在这一层；被适配组件零改动 |
| 设施 | `redis`、`consul`、`nacos`、`gorm/database`（含 `sharding`）、`beego/database`、`mongodb`、`common` | 连接管理与读写 | 不认识任何上层治理件 |
| 贯穿 | `trace`、`logging`、`metrics` | 链路、日志、指标 | 只被依赖，不反向依赖治理件；组件私有指标的接入一律写在 `metrics/adapter/*` |

### 四条依赖铁律

1. **依赖只向下**：上层可依赖下层，同层不互依赖，下层不得引用上层。唯一的横向读取是 `metrics/adapter/*` 拉取组件快照——它依赖的是组件**已公开的接口**（`Statser`/`Snapshots`/`GetStats`），组件本体零改动、零反向依赖。
2. **契约层零 SDK**：`mq/core`、`registry`、`lock`、`ratelimit`、`metrics` 的根包都不引第三方 SDK。因此换后端、换注册中心、换限流存储都**不需要改调用方一行代码**，只改装配时 import 的那个子包。
3. **换后端 = 换 import**：需要外部系统的组件都按「契约 + N 个后端子包」排布（`lock` 三后端、`ratelimit` 两后端、`mq` 四形态、`registry` 两注册中心），后端各自的 SDK 只进自己的子包，不会顺着依赖树把无关 SDK 拉进业务仓。
4. **联动走适配器，不改被适配方**：跨组件能力（`ratelimit/alert` 产 `breaker.AlertEvent`、`mq/pipequeue` 把 `mq.Consumer` 适配成 `pipeline.Queue`、`lock/consulstore` 复用 `consul` 组件的会话方法集）全部是**新增接口 + 薄适配器**，被适配组件保持原 API 与独立可引。

### 三条运行主线

**A · 同步请求**：入口中间件（`ratelimit/httpmiddleware`|`grpcmiddleware` 挡量）→ `grpcx`/`httpclient` 承接 → 业务读写 `gorm/database`（分片走 `sharding`、缓存走 `redis`）→ 出口调用套 `breaker` + `retry` → `trace` 上下文贯通全链、`logging` 自动带 traceId、耗时落 `metrics`。

**B · 异步消费**：`mq/core` 门面生产 → Kafka 或 Redis 四形态之一投递 → `mq/pipequeue` 把消费者桥成 `pipeline.Queue` → Step 链执行（错误分级 / 步骤超时 / 同 Key 串行）→ Handler 返回值驱动确认（`nil`=Commit、`mq.ErrManualAck`=挂起、其他 error=失败）→ 重投额度耗尽进 `deadletter` 持久化并按策略恢复。换 Kafka↔Redis 时 Step 链零改动。

**C · 跨实例协同与热更新**：定时器多副本各跑一份 → `lock` 抢租约保证单副本执行（`redisstore` 高吞吐 / `consulstore` 强一致）→ `Stats()` 出快照 → `metrics/adapter/lock` 汇入 `/metrics`；运行期参数变更由 `registry/config` 的 `ConfigSource.Watch` 推给装配层，再调 `logging.SetLevel`、`ratelimit.Local.Update`——治理件本身不依赖 `registry`，监听逻辑写在业务装配层。

### 发布与引用形态

- 每个构件独立 `go.mod`，模块路径 `github.com/xm-utils/sunmao/<模块目录>`；`mq` 族是两级路径，顶层 `mq/` 仅为容器目录。
- 发布用轻量 tag `<目录>/v<semver>`（如 `lock/v0.0.1`、`mq/core/v0.0.2`）；Go 侧只打包该子目录内容，构件之间发版互不影响。

## 组件组合策略

### 装配顺序

顺序颠倒通常不会报错，但问题出在「组件装配早于它所依赖的东西」：`logging` 装在 `trace` 之前则日志里没有 traceId，`metrics.Init` 晚于指标注册会丢弃已注册的 collector（它重建注册表）。推荐固定为：

以下别名沿用各组件文档的写法：`xmredis`=`redis`、`xmconsul`=`consul`、`regconsul`=`registry/consul`、`lockmetrics`=`metrics/adapter/lock`、`kafkaclient`=`mq/kafka/client`、`mqpipequeue`=`mq/pipequeue`。

```go
// 1. 链路先行：其余组件都要从 ctx 里取 span
_ = trace.Init(ctx, trace.Config{ServiceName: "order-svc", Exporter: "otlp", Endpoint: "otel:4317", Insecure: true})
// 2. 指标注册表 + 各组件适配器（拉取式，组件本体零改动）
_ = metrics.Init(metrics.Config{Namespace: "xmt"})
_ = metrics.Register(lockmetrics.New(orderSettleMu, couponMu))
// 3. 日志（formatter 依赖 trace 已装配）
_ = logging.Init(logging.Config{Service: "order-svc", JSON: true, OutputFile: "/var/log/order.log"})
// 4. 设施连接
database.InitGorm(&database.MysqlConfig{ /* 一主多从：Master / Slaves */ })
xmredis.InitRedisCache(&xmredis.Config{ /* … */ })
// 5. 治理件构造 + 择一后端
mu, _ := redisstore.New(redisstore.Config{
    Client: rdb,
    Config: lock.Config{Key: "cron:order-settle", TTL: 30 * time.Second},
})
// 6. 注册与入口（cmux：gRPC 与 /metrics、健康检查共用端口）
backend, _ := regconsul.New(&xmconsul.Config{Address: "127.0.0.1:8500"})   // 换 nacos 只改这一行
_ = backend.Register(ctx, &registry.RegisterRequest{ /* Name / Host / Port / HealthCheck */ })
// 7. 停机：先摘流量 → 停消费 → 释放锁 → 关日志
```

### 五个典型配方

**① HTTP API 服务（读多写少 + 自我保护）**

```bash
go get github.com/xm-utils/sunmao/{trace,logging,metrics,ratelimit,httpclient,gorm/database,redis,common}
```

```go
lim, _ := ratelimit.NewLocal(ratelimit.LocalConfig{ /* 默认速率 + 按 Key 覆写 */ })
handler, _ := httpmiddleware.New(httpmiddleware.Config{
    Local: lim,
    KeyFunc: func(r *http.Request) (string, error) { return r.Header.Get("X-Tenant-Id"), nil },
}, mux)                                  // mux 另将 metrics.Handler() 挂到 /metrics
srv := &http.Server{Addr: ":8080", Handler: handler}
```

> 关键点：单实例自保护用 `Local`（多维、零 IO）；多副本要共享份额就换 `ratelimit/redisstore.New`，业务代码不动。出口侧 HTTP 调用要带 span，用 `trace.NewTracedTransport("order-api", nil)` 包装 RoundTripper。鉴权与统一响应体用 `common`（JWT / RSA / `Response`）。限流规则热更新把 `ratelimit/alert.Watcher` 与 `registry/config` 接起来调 `Local.Update`。

**② gRPC 微服务（服务发现 + 优雅停机）**

```go
gs := grpcx.NewGrpcServer("order", ":9000", pb.RegisterOrderServiceServer /* , grpc.StatsHandler(otelgrpc.NewServerHandler()) */)
hs := grpcx.NewHTTPServer("admin", ":9000", func(e *gin.Engine) {
    e.GET("/metrics", gin.WrapH(metrics.Handler()))
})
srv := grpcx.NewServer([]grpcx.IServer{gs, hs})   // 同端口 cmux 复用
if err := srv.Start(); err != nil { /* … */ }
<-quit                                            // SIGTERM
_ = backend.Deregister(ctx, inst)                 // 先摘流量
srv.BeforeShutdown(); srv.Stop()                  // 再跑停机钩子
```

> 关键点：`registry` 只做抽象，consul / nacos 由子包适配，切换注册中心等于改一行 import；入口限流用 `ratelimit/grpcmiddleware.NewUnaryServerInterceptor`（超限 `ResourceExhausted` + `retry-after-ms`）。`IServer` 含未导出方法，业务侧只能通过 `NewGrpcServer`/`NewHTTPServer` 组合，不要自行实现。

**③ MQ 消费者（后端可切换）**

```go
consumer, _ := kafka.NewConsumer(&kafkaclient.ConsumerConfig{ /* 根包强制 AutoCommit=false */ })
queue, _ := mqpipequeue.New(mqpipequeue.Config{
    Consumer: consumer, Group: "order-service", Topic: "orders",
    BufferSize: 1000, ManualAck: true,          // 确认挂到引擎终态钩子，严格 at-least-once
})
_ = queue.Start(ctx)
engine, _ := pipeline.New(pipeline.Config[*mq.Message, *OrderCtx]{
    Queue: queue, Workers: 4,
    Resolve: func(msg *mq.Message) ([]pipeline.Step[*OrderCtx], *OrderCtx, error) {
        return []pipeline.Step[*OrderCtx]{parse, validate, persist}, &OrderCtx{}, nil
    },
    OnSuccess: func(msg *mq.Message, ctx *OrderCtx) { _ = queue.Commit(context.Background(), msg) },
    OnFatal:   func(msg *mq.Message, reason string) { /* 落死信 */ },
})
_ = engine.Start()
defer engine.StopWithTimeout(30 * time.Second)   // 有界停机：drain 完在途消息再退
```

> 关键点：Step 链只认 `pipeline.Queue`，而 `mq/core` 的契约决定了「Kafka ↔ Redis stream/list/zset 换后端不改一行 Step」；`pipequeue` 负责推送→拉取转换、有界缓冲背压与确认回写。重投次数看 `mq.HeaderAttempt`，额度用完交给 `deadletter`（`NewQueueManager` + `NewGormPersistenceStore`）持久化，恢复循环用 `QueueManager.StartRecovery` 并由配方 ④ 的 `lock` 保证多副本只跑一份。指标接 `metrics/adapter/kafka`、`metrics/adapter/deadletter`。

**④ 多副本定时任务 / 启动期一次性动作**

```go
// 反例：每个副本各自 time.NewTicker —— 同一批任务被重复执行
mu, _ := redisstore.New(redisstore.Config{
    Client: rdb,
    Config: lock.Config{Key: "cron:daily-report", TTL: time.Minute},
})
for range ticker.C {
    err := lock.Do(ctx, mu, func() error { return runDailyReport(ctx) })
    if errors.Is(err, lock.ErrBusy) { continue }   // 别的副本在跑，本轮直接跳过
}
```

> 关键点：`lock.Do` 收敛「获取—执行—释放」，获取失败不执行、临界区 panic 先归还锁；业务里如果带着栅栏做写操作，取 `mu.(lock.Fencer).Fence()` 的值随写入一起提交，服务端比对即可拒绝超时后才醒来的旧持有者。启动期并发建表同理（`gorm/database/sharding` 的自动 DDL 用 `consulstore` 锁包住）。

**⑤ 出口调用第三方（重试 + 熔断 + 降级）**

```go
cb := breaker.NewCircuitBreaker("pay-gw", &breaker.CircuitBreakerConfig{ /* 窗口、阈值、HalfOpen */ })
ex := retry.NewRetryExecutor(&retry.Config{ /* 策略：固定 / 指数 / 线性 */ })
res := ex.ExecuteSync(func(ctx context.Context) (any, error) {
    return cb.ExecuteWithContext(ctx, func(ctx context.Context) (any, error) { return callPayGateway(ctx, order) })
})
```

> 关键点：**重试在外、熔断在内**——反过来会让重试次数污染熔断统计，熔断窗口永远打不满。`breaker` 打开时走内置 `FallbackFunc` 降级（读缓存 / 返回兜底），`retry` 只包幂等操作。指标接 `metrics/adapter/breaker`；`retry` 尚无适配器（只有 breaker / deadletter / kafka / lock / ratelimit 五个），要上报重试次数就用 `Executor.SetCallback` 读 `Result.Attempts` / `TotalDuration` 自行埋点。

### 选型对照

**限流后端**

| 场景 | 选择 | 理由 |
|---|---|---|
| 单实例自保护、多维 Key、不想加 IO | `ratelimit.Local` | 纯内存，按 Key 惰性创建，支持 `Update`/`Remove` 热更新与 `Snapshots()` 观测 |
| 多副本合计不超额 | `ratelimit/redisstore` | 单 Hash + Lua 原子回填，`{tag}` 保证 Cluster 同 slot；后端故障交给 `FailOpen` 显式决策 |

**锁后端**

| 场景 | 选择 | 注意 |
|---|---|---|
| 同实例内互斥（不跨进程） | `lock.Local` | 互斥范围仅本实例，多副本必须换服务端后端 |
| 高吞吐、已有 Redis、可容忍极小竞态窗口 | `lock/redisstore` | 三段 Lua + `PEXPIRE` 租约，`Fence()` 取服务端 `INCR` |
| 强一致、需要公平排队与严格交接 | `lock/consulstore` | Session + KV；崩溃交接要等 `TTL + lock_delay`（实测约 15s），`WaitTimeout` 必须按这个量级设 |

**消息形态**

| 需求 | 选择 |
|---|---|
| 高吞吐、分区有序、长时间堆积 | `mq/kafka` |
| 消费者组 + ACK 语义 + 已有 Redis | `mq/redis/stream` |
| 需要「领取即不可见」的安全队列 | `mq/redis/list`（Lua 原子） |
| 延迟投递 / 定时可见 | `mq/redis/zset` |
| 广播通知、允许丢（at-most-once） | `mq/redis/pubsub` |

**韧性三件套的边界**

| 组件 | 管什么 | 不管什么 |
|---|---|---|
| `ratelimit` | 入口流量是否放行（量的问题） | 下游健康与否 |
| `breaker` | 出口调用是否劣化到该断开（质的问题） | 单次调用的重试 |
| `retry` | 单次操作是否可再来一次（可恢复性） | 幂等性由调用方保证 |

三者互不依赖（`ratelimit/alert` 只借 `breaker` 的事件模型），组合与否、怎么组合都在装配层决定，同一入口常见「限流 → 熔断 → 重试」三层叠加。

### 反模式

- **整套引入**：只用 Redis 客户端却 `go get` 整个仓库思维打包——各模块独立，按需引入即可，无关后端的 SDK 不会进依赖树。
- **往契约层加 SDK**：为了省事在 `mq/core`、`registry`、`lock` 根包里 import 后端库，会让所有使用者被迫继承这些依赖。
- **组件之间直接依赖**：比如让 `logging` 反向依赖 `registry/config` 来实现热更新——配置源监听写在装配层，否则模块互依赖、包级环。
- **用全局单例做透传**：治理件应显式构造注入（`New(Config{...})` 返回 error），别在 `init()` 里塞连接与后台 goroutine，测试和优雅停机都会失控。
- **绕过 `errors` 哨兵判失败**：`lock.ErrBusy`/`ErrLost`、`mq.ErrManualAck`、`ratelimit.ErrRateLimited` 都是 `errors.Is` 可判的哨兵，字符串匹配会在包装链上失效。

## 快速引入

各模块独立 `go.mod`，按需 `go get`：

```bash
# 示例：引入 GORM 数据库组件
go get github.com/xm-utils/sunmao/gorm/database

# 示例：引入 Redis 组件
go get github.com/xm-utils/sunmao/redis

# 示例：引入重试执行器
go get github.com/xm-utils/sunmao/retry

# 示例：引入分布式锁（根包零仓外依赖，后端子包按需引入）
go get github.com/xm-utils/sunmao/lock
go get github.com/xm-utils/sunmao/lock/redisstore

# 示例：引入 MQ 门面 + 择一后端适配器
go get github.com/xm-utils/sunmao/mq/core
go get github.com/xm-utils/sunmao/mq/redis
```

> 模块路径统一前缀：`github.com/xm-utils/sunmao/<模块目录>`；`mq` 族为两级路径（`mq/core`、
> `mq/redis`、`mq/kafka`、`mq/pipequeue`），顶层 `mq/` 仅为容器目录、自身不是模块。

## 目录结构

```
sunmao/
├── beego/database/       # Beego ORM 封装
├── breaker/              # 熔断器
├── common/               # 通用工具函数
├── consul/               # Consul 服务治理
├── deadletter/           # 死信队列
├── gorm/database/        # GORM 数据库（含 sharding 子包）
├── grpcx/                # gRPC 工具
├── httpclient/           # HTTP 客户端
├── lock/                 # 分布式互斥锁（含 redisstore / consulstore 子包）
├── logging/              # 统一日志出口（JSON + traceId + 文件轮转）
├── metrics/              # 统一指标出口（含 adapter/* 组件采集器）
├── mongodb/              # MongoDB 客户端
├── mq/                   # 消息队列族（顶层容器，自身无 go.mod）
│   ├── core/             # 统一门面（纯契约，package mq）
│   ├── redis/            # Redis 四形态适配器（含 stream/queue 队列本体）
│   ├── kafka/            # Kafka 适配器（含 client 客户端本体）
│   └── pipequeue/        # MQ × pipeline 桥接队列
├── nacos/                # Nacos 配置/注册中心
├── pipeline/             # 消息消费执行引擎
├── ratelimit/            # 限流与降级（含 redisstore / alert / grpcmiddleware / httpmiddleware 子包）
├── redis/                # Redis 客户端
├── registry/             # 注册中心/配置中心统一抽象（consul/nacos 双适配器）
├── retry/                # 重试执行器
└── trace/                # 链路追踪与指标计量（OpenTelemetry 封装）
```

## 开发

```bash
# 克隆仓库
git clone https://github.com/xm-utils/sunmao.git
cd sunmao

# 进入目标模块开发/测试（各模块独立）
cd gorm/database
go build ./...
go test ./...
go vet ./...
```

多模块同时联调时，在仓根建 `go.work`（**不提交**）`use` 相关模块目录；验证完成后必须回到 `GOWORK=off` 跑一遍 `build/vet/test`，确保该模块脱离工作区仍可独立构建。贡献代码前请先读仓根 `CONTRIBUTING.md`。

## 许可证

MIT