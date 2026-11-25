# ratelimit — 限流与降级组件

主动流量防护：**三种限流算法共用一个 `Limiter` 契约 + 本地内存/Redis 两类后端 + gRPC/HTTP 入口中间件**。对应架构路线图 3.2（P0）。

与 `breaker` 的分工：`breaker` 是"被动逃生"（依赖已劣化才熔断），本组件是"主动防护"——入口流量超标时保护自身，出口调用时约束自我频率。

- 模块路径：`github.com/xm-utils/sunmao/ratelimit`
- 依赖策略：**根包零仓外依赖**（只用标准库）；`google.golang.org/grpc` 只进 `grpcmiddleware` 子包，`go-redis/v8` 只进 `redisstore` 子包，`breaker`（仅取告警事件模型）只进 `alert` 子包，纯本地限流场景不会被拖入重依赖链
- 兼容策略：`grpcx` / `httpclient` / `redis` / `breaker` 组件本体**零改动**，接入一律经本组件子包适配器（指标侧走 `metrics/adapter/ratelimit`，本组件零依赖 `metrics`）

## 目录结构

```
ratelimit/
├── ratelimit.go                   # Limiter 契约、Result、Clock 时间源、三哨兵错误
├── token_bucket.go                # TokenBucket 令牌桶（默认算法，懒回填）
├── sliding_window.go              # SlidingWindow 滑动窗口（精确计数）
├── leaky_bucket.go                # LeakyBucket 漏桶（匀速整形，含 Reserve/Wait）
├── local.go                       # Local 本地内存后端（按 Key 惰性创建，多维限流 + 判定计数快照）
├── config.example.yaml            # 配置模板（装配层同构 struct）
├── redisstore/                    # Redis 后端：Lua 脚本原子扣减，多实例共享份额
├── alert/                         # 拒绝率告警巡检：差分快照超阈值 → breaker 告警通道
├── grpcmiddleware/                # gRPC 一元/流服务端拦截器
└── httpmiddleware/                # net/http Handler 包装（Gin 经 gin.WrapH 装配）
```

## 算法对比与选型

### 一览

| 维度 | 令牌桶 `TokenBucket` | 滑动窗口 `SlidingWindow` | 漏桶 `LeakyBucket` |
|---|---|---|---|
| 核心问题 | 允许多大突发？ | 任意窗口内是否超额？ | 出水速率是否恒定？ |
| 突发容忍 | **高**：桶内有 `Burst` 个令牌即可一次放过 | 低：窗口内配额用尽即拒（窗口内不二次放大） | **最低**：`Queue=1` 时零突发，背靠背必拒 |
| 平均速率上限 | ≤ `Rate` | ≤ `Limit / Window` | ≤ `Rate`（严格） |
| 计数精确性 | 按时间折算浮点令牌，非逐条记录 | **任意长度为 `Window` 的区间内总数严格 ≤ `Limit`**，无固定窗口临界双倍 | 按出水时刻逐个累加，整数级精确 |
| 状态内存 | O(1) | O(`Limit`) 条放行记录（驻留至过期） | O(1) |
| 初始状态 | **满桶**（开局即允许突发） | 空窗（开局额度完整可用） | **空桶**（开局无排队，突增要现漏） |
| 被拒时退避预估 | 缺额 / `Rate` | 最早一条记录滑出窗口的时刻 | 溢出水位数 / `Rate` |
| 额外能力 | `SetParams` 缩容同步截断 | `SetParams` 收紧额度不追溯撤销 | `Reserve` 返回应排队时长、`Wait` 阻塞整形 |
| 典型场景 | API 入口通用限流、网关配额 | 计费/合同式配额（"每 60 秒最多 N 次"）、防刷 | 出口自我限速、保护脆弱下游（DB/三方配额接口）、匀速灌数据 |

三者都是"**额度 + 时间**"模型，但**令牌桶管的是入口能不能进、漏桶管的是出口以多快速度走**——数学上互为对偶（空闲时长在令牌桶里攒令牌、在漏桶里降水位），语义上却不可互换：令牌桶会把攒下的额度一次性放出去形成突发，漏桶永远按固定间隔出水。

### 选型策略

1. **默认用令牌桶**。除非命中下面两条，`TokenBucket` 是安全选择：能容忍突发、内存 O(1)、退避可预估。
2. **需要"任意区间内严格不超额"就用滑动窗口**。配额合同、按量计费、防暴力枚举这类"每一分钟都不能超 N 次"的语义，固定窗口会在边界放大近一倍，令牌桶的浮点折算也不保证逐条精确——只有滑窗保证。代价是内存 O(`Limit`)，`Limit` 上到十万级要重新评估。
3. **限流对象是自己要调用的下游就用漏桶**。此时目标不是"拒掉别人"，而是"约束自己"：`LeakyBucket.Wait(ctx, n)` 直接阻塞到应出水的时刻，把重试/回灌流量摊平成恒定速率，避免把上游打挂后又因 retry 二次放大。
4. **参数换算**（以目标 QPS = 100、可容忍突发 2 秒为例）：
   - 令牌桶：`Rate: 100, Burst: 200`（`Burst = Rate × 可容忍突发秒数`，一般取 1~3 倍）
   - 滑动窗口：`Limit: 100, Window: time.Second`（`Limit ≈ Rate × Window`；窗口越宽判定越平滑、内存驻留越久）
   - 漏桶：`Rate: 100, Queue: 200`（`Queue = Rate × 允许排队秒数`，**最小取 1**；`Queue=1` 即纯匀速）

### 后端选择

| | `Local`（本地内存） | `redisstore`（Redis + Lua） |
|---|---|---|
| 份额范围 | 单进程内共享，**多实例各算各的**（10 副本 × Rate 100 = 实际 1000 QPS） | 全部实例共享同一份，跨进程严格 |
| 每次判定开销 | 纯内存加锁，纳秒级 | 一次 Redis RTT（`EvalSha`），毫秒级 |
| 支持算法 | 令牌桶（`Local` 惰性创建的就是 `TokenBucket`） | 令牌桶 |
| 适用 | 单机服务、按进程配额、热点接口自我保护 | 集群全局配额、租户级总额、需要跨实例一致的 429 |

经验组合：**两层限流**——全局配额用 `redisstore` 挡在入口（防总额超标），本地 `Local` 再设一道 `Rate = 全局 / 副本数 × 1.5` 的兜底（防 Redis 抖动时集群整体失守，配 `FailOpen` 使用）。

## API 参考

### 统一契约

```go
type Limiter interface {
    Allow(ctx context.Context) error                    // 单次判定
    AllowN(ctx context.Context, n int) (bool, error)     // 一次消耗 n 个额度
    Peek() Result                                        // 只读快照，不消耗
    Close() error                                        // 幂等，关闭后判定一律 ErrClosed
}

type Result struct {
    OK         bool          // 是否放行
    RetryAfter time.Duration // 被拒时建议等待（0 表示无法预估或无需等待）
    Limit      float64       // 等效速率（个/秒）
    Remaining  float64       // 判定后剩余额度
}
```

错误语义（三算法与两后端一致，全程不 panic）：

| 情况 | 返回 |
|---|---|
| `Allow` 被限流 | 包装 `ErrRateLimited`，消息含退避时长，如 `ratelimit: rate limited: retry after 100ms` |
| `AllowN` 被限流 | `(false, nil)`——被拒是正常结果而非错误，需要退避时长时另取 `Peek()` |
| `n <= 0` / 配置非法 | 包装 `ErrInvalidConfig` |
| 已 `Close` | `ErrClosed` |
| `ctx` 已取消/超时 | 优先返回 `ctx.Err()`，不做判定 |

判断被限流一律用 `errors.Is(err, ratelimit.ErrRateLimited)`，不要比对字符串。

### 时间源

`Clock` 接口（`SystemClock` / `ClockFunc`）注入所有算法与 Redis 后端。单测里注入假时钟可让窗口滑动、令牌回填完全确定化（仓内并发组件的统一手法）；生产留空即用 `time.Now`。

### TokenBucket 令牌桶

```go
tb, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
    Rate:  100,             // 回填速率，个/秒，必须 > 0
    Burst: 200,             // 桶容量（最大突发），必须 > 0
    Clock: nil,             // 可选
})

err = tb.Allow(ctx)                              // 消耗 1 个令牌
ok, err := tb.AllowN(ctx, 10)                    // 一次消耗 10 个
res := tb.Peek()                                 // 不消耗
err = tb.SetParams(50, 100)                      // 热更新，缩容时当前令牌同步截断
err = tb.Close()
```

初始为**满桶**（开局即允许 `Burst` 规模突发）；判定采用懒回填——按经过时间补令牌并封顶于 `Burst`，无后台协程。时钟回拨时不倒扣令牌，仅重置基准点。

### SlidingWindow 滑动窗口

```go
sw, err := ratelimit.NewSlidingWindow(ratelimit.SlidingWindowConfig{
    Limit:  100,                 // 窗口内最大放行数，必须 > 0
    Window: time.Minute,         // 窗口宽度，必须 > 0
    Clock:  nil,
})

err = sw.Allow(ctx)
ok, err := sw.AllowN(ctx, 5)     // 一次记 1 条、权重 5，不逐令牌展开
err = sw.SetParams(50, time.Minute)
```

每条放行记录驻留至滑出窗口，内存上界 O(`Limit`)。时钟回拨时过期边界随 `now` 前移，在窗记录不会被误删（回拨只让窗口显得"更新"，不会放大配额）。`SetParams` 收紧 `Limit` 后已在窗记录不追溯撤销，仅影响后续判定；此时 `Peek().Remaining` 截为 0（不报负数）。

### LeakyBucket 漏桶

```go
lb, err := ratelimit.NewLeakyBucket(ratelimit.LeakyBucketConfig{
    Rate:  100,          // 出水速率，个/秒，必须 > 0
    Queue: 200,          // 桶容量（允许排队的额度数），必须 >= 1
    Clock: nil,
})

err = lb.Allow(ctx)                          // 非阻塞判定：只看桶容不容得下
ok, err := lb.AllowN(ctx, 10)

delay, err := lb.Reserve(ctx, 10)            // 预定并拿到"本次应排队多久"
err = lb.Wait(ctx, 10)                       // 直接阻塞到出水时刻（ctx 取消即刻返回，额度不回滚）
err = lb.SetParams(50, 100)
```

`Queue=1` 即纯匀速限速（任何快于 `1/Rate` 秒间隔的请求都被拒）；增大 `Queue` 只提高"接纳"能力，不改变出水速率。`Allow` 与 `Reserve` 的差别仅在语义——后者把排队时长明确交给调用方，用于出口自我限速（`time.Sleep(delay)` 或直接 `Wait`）。

`SetParams` 改速率时以**水位个数**为不变量重排出水时刻，避免速率抬高后既有水位被折算成成倍虚增。

### Local 本地内存多维后端

```go
local, err := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 100, Burst: 200})

err = local.Allow(ctx, "api:order:create")          // 按 Key 惰性创建并复用限流器
ok, err := local.AllowN(ctx, "tenant:10086", 10)
limiter, err := local.Limiter("api:order:create")   // 拿到 *TokenBucket 满足 Limiter

err = local.Update("api:order:create", 50, 100)     // 热更新；Key 不存在则按新参数创建
err = local.Remove("api:order:create")              // 规则删除
res := local.Stats("api:order:create")              // 令牌快照；Key 不存在返回零值
snaps := local.Snapshots()                          // 全部已判定 Key 的计数 + 水位快照
err = local.Close()                                 // 关闭全部限流器
```

Key 由使用方拼接，天然支撑接口级（`api:xxx`）、服务级、租户级（`tenant:xxx`）多维组合；空 Key 报 `ErrInvalidConfig`（入口中间件把"空串"用作直通约定，见下）。双检锁惰性创建，同 Key 恒定复用一个限流器。

**判定计数**：`Allow` / `AllowN` 会按 Key 累计 `Total`（判定次数）与 `Rejected`（限流拒绝次数），只记**真实判定**——ctx 取消、后端故障、`n<=0` 等参数错误一律不记账，避免污染拒绝率差分。快照形态：

```go
type KeySnapshot struct {
    Key       string
    Total     int64   // 累计判定次数（进程启动以来）
    Rejected  int64   // 累计被限流拒绝次数
    Remaining float64 // 当前可用令牌数
    Limit     float64 // 当前生效速率（个/秒）
}
```

`Snapshots()` 实时拉取，是 `metrics/adapter/ratelimit` 与 `alert.Watcher` 的唯一数据来源；`Local` 已 `Close` 时返回 `nil`（不报错）。计数为**累计值**，区间速率需差分（`alert` 已代劳）。

> `Local` 惰性创建的是 `TokenBucket`，`Update` 也只接受令牌桶参数；需要按 Key 用滑窗/漏桶时，自行持有 `*SlidingWindow` / `*LeakyBucket`  map（见「已知边界」），观测侧可用 `adapter.NewWithProvider` 自备快照源。

### redisstore Redis 后端

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/xm-utils/sunmao/ratelimit/redisstore"
)

// 客户端由使用方持有（go-redis v8，地址/密码/库号字段与 redis.Config 同形）
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Password: "", DB: 0})

rl, err := redisstore.New(redisstore.Config{
    Client: client,                   // 必填，不可为 nil
    Key:    "api:order:create",        // 实际存储 Key 为 xm_ratelimit:{api:order:create}
    Rate:   100,
    Burst:  200,
    TTL:    10 * time.Minute,         // 空闲状态自动清理，<=0 取默认 10 分钟
})

err = rl.Allow(ctx)                   // 一次 Lua 脚本调用，服务端原子扣减
```

要点：

- **原子性**：读取水位 → 按经过时间回填 → 判定扣减 → 写回 + `PEXPIRE` 全在一段 Lua 脚本内完成，多实例并发不超放；
- **集群安全**：全部状态收敛在单个 Hash Key，并用 `{tag}` 包裹，Redis Cluster 下脚本内所有命令命中同一 slot；
- **脚本回落**：经 `redis.NewScript(...).Run` 乐观走 `EVALSHA`，`NOSCRIPT` 时自动回落 `EVAL`（go-redis v8 未导出 `NoScriptError`，不手写回落判定）；
- **时间由调用方传入**：`now` 经 `ARGV` 入参而非服务端 `TIME`，既规避脚本非确定性限制，也让单测可注入假时钟；
- **退避同批返回**：`Allow` 的 `RetryAfter` 取自脚本返回值，不另发一次 `Peek`，避免跨调用间隙回填导致的误判；
- **定点数**：额度按 `1e9` 放大为整数微令牌传输，规避 Lua 5.1 double 的分数解析歧义；因此 `AllowN` 有单次上限 `9223372036`（约 `2^63/1e9`），超出报 `ErrInvalidConfig` 并提示拆批消耗；
- **后端故障返回 error 而非 panic**：Redis 不可达时 `Allow` 返回包装底层错误的 error，由入口中间件的 `FailOpen` 决定放行还是拒绝（见下）；
- `Close` **仅阻断本实例**后续判定，不动 Redis 中的共享状态（其他副本仍在用）。

`redisstore` 不经过 `redis` 组件的全局 API：该组件的 `client` 未导出（`GetClient` 不存在），
其全局 `Eval` 还存在 KEYS 双前缀缺陷（`redisCache.go` 里 `prefixedKeys` 构建后未使用，实际传的
`associateKeys(keys...)` 已叠过一次前缀），因此本包改为接收注入的 `*redis.Client` 直接执行脚本；
连接池/健康检查仍由使用方按 `redis` 组件的配置形态自行管理。

### grpcmiddleware gRPC 入口拦截器

```go
import (
    "google.golang.org/grpc"
    "github.com/xm-utils/sunmao/ratelimit/grpcmiddleware"
)

// 服务级：全部方法共享一份额额
itc, err := grpcmiddleware.NewUnaryServerInterceptor(grpcmiddleware.Config{Limiter: rl})
server := grpc.NewServer(grpc.UnaryInterceptor(itc))

// 接口级 / 租户级：按 Key 走 Local 后端
itc, err := grpcmiddleware.NewUnaryServerInterceptor(grpcmiddleware.Config{
    Local: local,
    KeyFunc: func(ctx context.Context, fullMethod string) (string, error) {
        md, _ := metadata.FromIncomingContext(ctx)
        if tenant := md.Get("x-tenant-id"); len(tenant) > 0 {
            return "tenant:" + tenant[0], nil
        }
        return fullMethod, nil        // 返回 "" 表示直通不限流（公开端点）
    },
    FailOpen: true,
})
```

- `NewUnaryServerInterceptor` / `NewStreamServerInterceptor` 返回标准 grpc 拦截器类型，与 `grpcx` 现有 `grpc.UnaryInterceptor` ServerOption 直接拼装，`grpcx` 本体零改动；
- `Config` 的 `Limiter` 与 `Local` **互斥二选一**（都不填或都填 → `ErrInvalidConfig`）；`Local` 模式下 `KeyFunc` 缺省用 `fullMethod`；
- 超限 → `codes.ResourceExhausted`，trailer 键 `retry-after-ms`（`grpcmiddleware.TrailerRetryAfterMs`）携带建议等待毫秒数；
- 限流后端自身故障 → 默认 `codes.Internal`（fail-closed），`FailOpen: true` 时放行——限流是防护而非鉴权，后端抖动不应拖垮业务；
- 流拦截器**只在流建立时判定一次**，长流不逐条消息计数（消息级配额请在业务链路内调 `Local.AllowN`）。

### httpmiddleware HTTP 入口中间件

```go
import "github.com/xm-utils/sunmao/ratelimit/httpmiddleware"

// 快捷形态：服务级
handler, err := httpmiddleware.Wrap(rl, mux)

// 完整配置：多维 + 直通 + fail-open
mw, err := httpmiddleware.Middleware(httpmiddleware.Config{
    Local: local,
    KeyFunc: func(r *http.Request) (string, error) {
        if tenant := r.Header.Get("X-Tenant-Id"); tenant != "" {
            return "tenant:" + tenant, nil
        }
        return r.URL.Path, nil        // 返回 "" 表示直通
    },
})
handler = mw(mux)

// Gin / grpcx cmux 的 HTTP 端口（不引入 Gin 依赖，经标准 Handler 桥接）
// engine.Use(gin.WrapH(handler))
```

- 超限 → `429 Too Many Requests` + `Retry-After` 秒头（可预估时向上取整）+ JSON 体 `{"code":429,"message":"too many requests"}`（字段名与 `common` 统一响应体对齐）；
- `Wrap` / `New` / `Middleware` 三种形态：前两者产出 `http.Handler`，`Middleware` 产出 `func(http.Handler) http.Handler` 便于多处复用；
- 后端故障默认 `500`，`FailOpen: true` 放行；
- `KeyFunc` 返回空串 = 直通不查限流器（不会为该请求创建 Key）。

### alert 拒绝率告警联动（breaker 告警通道）

`breaker` 已有的 `AlertManager`（`alert.go`）具备完整告警出口（分级日志 + `AlertCallback`），但其监控对象只能是熔断器实例。本包把限流快照接到该通道：周期差分 `Local.Snapshots()`，某 Key 在巡检间隔内的**区间拒绝率**超阈值即产出 `breaker.AlertEvent`（本体零改动，适配写在本包）。

```go
import "github.com/xm-utils/sunmao/ratelimit/alert"

w, err := alert.New(alert.Config{
    Snapshots:   local.Snapshots,        // 必填，快照源
    Threshold:   0.8,                    // 区间拒绝率阈值，<=0 或 >1 取 0.8
    MinRequests: 10,                      // 区间样本量下限，<=0 取 10
    Interval:    10 * time.Second,       // 巡检间隔，<=0 取 10s
    Cooldown:    5 * time.Minute,        // 同 Key 冷却期，<=0 取 5min
    OnAlert:     func(ev *breaker.AlertEvent) { /* 透传 breaker 告警出口 */ },
    OnPanic:     func(r any) { /* 快照源/回调 panic 通知 */ },
    Clock:       nil,                    // 冷却期时间源，nil 用 SystemClock
})

go w.Start(ctx)   // 阻塞至 ctx 取消或 Close；需要手动节奏可只调 w.Check()
err = w.Close()   // 幂等，等待监控协程退出
```

行为要点（均与 `breaker.AlertManager` 对齐）：

- **首次采样只记基线不告警**：无差分可用；Key 从快照消失会清理其基线，再现时重新记基线；
- **区间而非累计**：告警消息里的判定/拒绝数是两次采样之差，示例 `限流拒绝率 100.00% 超过阈值 80.00%(区间判定 20, 拒绝 20)`；
- **冷却期防风暴**：同 Key 在 `Cooldown` 内不重复告警，不同 Key 互不影响；
- **计数回拨免疫**：使用方重置导致 `Total`/`Rejected` 回退时按新基线处理，本轮不误报；
- **来源标识**：`CircuitName` 形如 `ratelimit:api:order:create`，`Level` 固定 `AlertLevelError`，与同通道内熔断器告警可区分；
- **panic 安全**：快照源 panic 被隔离为本轮跳过，`OnAlert` 回调 panic 转 `OnPanic`，监控协程不崩溃；
- `Start` 需由调用方放入 goroutine（与 `breaker` 的 `StartMonitoring` 内部自启不同），便于纳入统一生命周期与优雅停机；
- 本包只引入 `breaker` 依赖，不影响根包零依赖特性（不 import `alert` 的子包使用方不会被拖入 `breaker`/`logrus`，Go 按包粒度构建）。

### 指标出口接入（metrics 适配器）

埋点在 `metrics/adapter/ratelimit`（拉取式 `prometheus.Collector`，本组件零依赖 `metrics`）：

```go
import rlmetrics "github.com/xm-utils/sunmao/metrics/adapter/ratelimit"

metrics.Register(rlmetrics.New(local))              // 本地后端
metrics.Register(rlmetrics.NewWithProvider(fn))     // 自定义快照源（多后端聚合/自持实例）
```

| 指标 | 类型 | label |
|---|---|---|
| `xmt_ratelimit_requests_total` | counter | `key`, `result=allowed\|rejected` |
| `xmt_ratelimit_rejection_rate` | gauge | `key`（累计拒绝率，区间值请对计数做 `rate()`） |
| `xmt_ratelimit_tokens_available` | gauge | `key` |
| `xmt_ratelimit_rule_rate` | gauge | `key`（当前生效速率） |

仅含发生过真实判定的 Key；`Total=0` 时只产出计数零值，不产出拒绝率/水位（避免零除与误导）。快照源 panic 经 `InvalidMetric` 兜底为抓取错误，不击穿 `/metrics`。

> `redisstore` 无内存快照（状态在 Redis 侧），需其指标请按 Key 调 `Peek()` 自行组装 `NewWithProvider`，或在入口中间件层埋点。

## 配置与热更新

配置模板见 [config.example.yaml](config.example.yaml)（装配层同构 struct 的键位说明与加载示例）。本组件**不内置 YAML 解析与配置源监听**，以保持根包零依赖：

1. 装配层把 yaml 反序列化进同构 struct，映射为各 `Config` 构造限流器；
2. 规则热更新走应用端点：`Local.Update(key, rate, burst)`、`tb.SetParams(...)`；
3. 配置源监听（Consul KV / Nacos）由使用方经 `registry/config.Watch` 装配后调用上述端点——与 `logging` 动态调级同一手法，组件不反向依赖 `registry`。

## 与其他组件联动

| 组件 | 方式 |
|---|---|
| `redis` | 可沿用其配置形态（host/password/dbNum）；因组件 `client` 未导出且全局 `Eval` 有 KEYS 双前缀缺陷，`redisstore` 直接接收使用方持有的 `*redis.Client` 执行脚本，不改组件本体 |
| `grpcx` | 拦截器经标准 `grpc.ServerOption` 拼装，`grpcx` 零改动（ROADMAP 第四阶段若落地 `WithRateLimit` 一行装配，再评估收编本子包入口） |
| `httpclient` | 出口自我限速用 `LeakyBucket.Wait(ctx, n)` 包住调用；`httpclient` 本体零改动 |
| `breaker` | 分工互补：本组件主动拒、`breaker` 被动熔断。**已联动**：`alert` 子包差分拒绝率快照，超阈值产出 `breaker.AlertEvent` 进其告警通道；降级兜底可直接复用 `breaker` 的 `FallbackFunc` 体系 |
| `metrics` | **已接入**：`metrics/adapter/ratelimit` 拉取式适配器读 `Local.Snapshots`，产出 `xmt_ratelimit_*`；本组件零依赖 `metrics`，根包零依赖不变 |
| `registry/config` | 限流规则热更新的配置源，监听后调 `Local.Update` |

## 测试

```bash
cd ratelimit
GOWORK=off go test -race -count=2 ./...
```

- 全部算法/后端用例注入假时钟，窗口滑动与令牌回填完全确定化，不依赖真实时间；
- 并发用例冻结时钟并严格断言放行数量恰等于 `Burst` / `Limit` / `Queue`（不超放也不漏放）；
- `redisstore` 基于 `miniredis` 离线运行，覆盖双 client 共享份额、`NOSCRIPT` 回落、TTL 过期、后端宕机的 error 传播（不 panic）；
- 中间件用例覆盖超限拒绝码、trailer/header、`FailOpen`、直通 Key；
- `alert` 用例覆盖首轮基线不告警、阈值/样本量边界、冷却期、计数回拨与 Key 消失、回调/快照源 panic 隔离、并发 `Check`（`-race`）。

## 已知边界与待办

- **`Local` 只惰性创建令牌桶**：按 Key 用滑窗/漏桶需使用方自持实例（后续可给 `LocalConfig` 加算法选择或 `Factory` 钩子）；
- **Redis 后端只有令牌桶**：滑窗/漏桶尚无 Lua 实现，多实例全局配额需精确计数时暂用"令牌桶 + 合理 Burst"近似；
- **`redisstore` 每次判定一个 RTT**：热点接口建议本地 `Local` 先挡一层，或按 `Key` 分片降低单 Key 争用；
- **时钟回拨防御是"不倒扣"而非"回滚"**：跨实例存在时钟漂移时，各实例独立钳制，不追求绝对一致；
- **计数不跨进程**：`Local` 的判定计数与告警巡检均为单进程视图，多副本需各自部署 `alert.Watcher`（或改用指标侧 `rate()` 做聚合告警）；
- **告警不自动接 `breaker.AlertManager`**：`AlertManager.RegisterBreaker` 只接熔断器实例，本包产出事件由使用方透传至实际出口（webhook / IM / 日志），避免反向侵入熔断器监控循环；
- 待办：规则热更新的配置源监听装配（`registry/config.Watch` → `Local.Update`）、`redisstore` 侧指标快照、滑窗/漏桶的 Redis 后端。

## 许可

MIT
