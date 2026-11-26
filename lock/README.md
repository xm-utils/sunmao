# lock — 分布式互斥锁组件

跨实例协同原语：**一个 `Mutex` 契约 + 进程内 / Redis / Consul 三类后端，租期与看门狗续期、try-lock、阻塞获取、goroutine 绑定重入、ctx 取消感知全部统一**。

立项动因是仓库里已存在、但至今没有原语支撑的多实例竞争：

- `gorm/database/sharding` 启动期自动建表——多实例并发 `CREATE TABLE IF NOT EXISTS` 互相阻塞甚至报 1091/1061（见 `sharding.go:336`、`primary_key.go:60`）；
- `deadletter` 定时恢复——每个副本各自 `time.NewTicker` 跑一轮，同一批死信被重复捞取重投（见 `manager.go:76 StartRecovery`）；
- `mq/redis/stream` pending 巡检——该能力块恢复启用后即面临重复告警。

- 模块路径：`github.com/xm-utils/sunmao/lock`
- 依赖策略：**根包零仓外依赖**（只用标准库）；`go-redis/v8` 只进 `redisstore` 子包，`hashicorp/consul/api` 与 `consul` 组件只进 `consulstore` 子包，纯进程内场景不会被拖入重依赖链
- 兼容策略：`sharding` / `deadletter` 等被保护组件**零改动**，接入一律在调用方装配层包一层 `TryLock`/`Lock`（本组件的验证用例即按此手法复现两个场景，见下文「场景接入示范」）

## 目录结构

```
lock/
├── lock.go                     # Mutex/Fencer/Holder/Refresher 契约、六哨兵错误、Config 与默认值
├── owner.go                    # GoroutineOwner：进程前缀 + goroutine 标识（默认重入语义）
├── acquire.go                  # WaitAcquire/WaitAcquireSignal 轮询等待、Watchdog 续期（两端共用）
├── local.go                    # Local 进程内实现（零依赖，测试与单机部署）
├── do.go                       # Do(ctx, mu, fn)：获取-执行-释放配对
├── stats.go                    # Stats 快照、Statser 契约、Recorder 原子计数
├── example_test.go             # 可运行示例（带 // Output: 断言，go test 即校验）
├── config.example.yaml         # 配置模板（装配层同构 struct）
├── redisstore/                 # Redis 后端：Lua 原子脚本 + PEXPIRE 租期 + 栅栏 token
│   └── integration_test.go     # 真实 Redis/Cluster 用例（//go:build integration）
└── consulstore/                # Consul 后端：Session(TTL 租约) + KV Acquire/Release
    └── integration_test.go     # 真实 Consul 会话与 KV 用例（//go:build integration）
```

## 后端对比与选型

| 维度 | `Local`（进程内） | `redisstore`（Redis + Lua） | `consulstore`（Consul Session + KV） |
|---|---|---|---|
| 互斥范围 | **仅同一实例内**，多副本各拿各的 | 跨进程、跨机器严格互斥 | 跨进程、跨机器严格互斥 |
| 租期 | 无（不存在过期） | Hash `PEXPIRE`，默认 30s | Session TTL，默认 30s |
| 持有者崩溃 | 无所谓（进程没了锁也没了） | 租期到点 Key 消失，他人可抢 | 会话 TTL 到点 Consul 自动释放 KV，他人可抢 |
| 崩溃回收速度 | — | ≤ `TTL` | ≤ `TTL`（会话失效即刻） |
| 自动续期 | 不适用 | 看门狗 `TTL/3` | 看门狗 `TTL/3`（`RenewSession`） |
| 栅栏 token | ✅ `Fence()` 单调递增 | ✅ `Fence()`（服务端 `INCR` 独立计数 Key） | ❌（`consul` 组件未透出 `ModifyIndex`） |
| 释放唤醒等待者 | ✅ 广播唤醒（不等轮询间隔） | ❌ 纯轮询 | ❌ 纯轮询 |
| 单次获取开销 | 纳秒级内存加锁 | 1 次 Redis RTT（`EVALSHA`） | 首次 2 次 RTT（建会话 + Acquire），重入 0 次 |
| ctx 传导 | 全程 | 全程（获取、释放、续期都吃 ctx） | **仅获取等待**：`consul` 组件客户端方法不吃 ctx |
| 典型场景 | 单测、单机部署、同进程多 goroutine 初始化竞争 | 已有 Redis（单机/主从/Cluster/Failover）、需要栅栏 token 防旧持有者写入 | 已用 Consul 做注册中心、想少一套存储 |

### 选型策略

1. **默认用 `redisstore`**。仓库里 Redis 几乎是标配，Lua 脚本一次 RTT 完成「判属主 + 加深度 + 续期」，且独有三端里最完整的栅栏能力（`Fence()`），能配合下游存储拒绝旧持有者的写入。
2. **已有 Consul、不想再占一套 Redis 就用 `consulstore`**。它的租约是 Consul 原生语义（会话失效 → KV 自动释放），运维可在 UI/CLI 里直接看到锁与持有者会话；代价是没有栅栏 token、释放/续期不吃 ctx。
3. **只有进程内并发问题就用 `Local`**。它的互斥范围严格限于同一实例（同 Key 两个实例互不相知，见 `TestLocalInstancesNotShared`），绝不要把 `Local` 当分布式锁用——多副本部署时它不会报错，只会各跑一遍。
4. **TTL 取值**：`TTL ≥ 2 × 最坏临界区耗时`，且必须 > 看门狗间隔（默认 `TTL/3`，允许 2 次续期失败）。DDL、批量捞取这类慢任务给 60s 起；短任务（毫秒级判定）也不要低于 5s，否则 GC 停顿就可能丢锁。
5. **`Lock` 还是 `TryLock`**：串行化必要动作（建表、迁移）用 `Lock`（等待者进去后走「已存在」分支即可）；周期性任务（死信恢复、巡检）用 `TryLock`——抢不到说明别人正在跑，本轮跳过比排队等待更省资源，也不会堆出N倍重复执行。

## 统一契约

```go
type Mutex interface {
    Lock(ctx context.Context) error     // 阻塞获取
    TryLock(ctx context.Context) error  // 非阻塞获取，占用即 ErrBusy
    Unlock(ctx context.Context) error   // 释放一层深度
    Close() error                       // 停看门狗 + 归还仍持有的锁，幂等
}

type Fencer interface { Fence() uint64 }              // 可选：栅栏 token（Local / redisstore）
type Holder interface { Held() bool }                 // 可选：本地持有视图（三端）
type Refresher interface { Refresh(ctx) error }       // 可选：手动续期（两服务端后端）
type Statser interface { Stats() Stats }              // 可选：运行快照（三端，接 metrics 采集器）
```

契约方法**一律不 panic**：后端异常、内部脚本返回值异常、回调 panic 全部 recover 后包成 error。判定一律用 `errors.Is`，不要比对字符串。

拿到契约后最省事的用法是 `Do`——它把「谁加锁谁解锁」收敛成一次调用，错误分支与 panic 都会归还锁：

```go
err := lock.Do(ctx, mu, func() error {
    return ddl(ctx)             // 进不去就报 ErrTimeout/后端错误，fn 不会执行
})
```

| 情形 | `Do` 的行为 |
|---|---|
| 获取失败 | 不执行 `fn`，错误原样返回（仍可 `errors.Is` 分出 `ErrTimeout` / ctx 取消）|
| `fn` 返回错误 | 先释放再返回；释放也失败时 `errors.Join` 合并，**不拿释放错误覆盖业务错误** |
| `fn` panic | 先释放再把 panic 原样抛回——这里 panic 的是调用方自己的临界区，不是锁的故障路径 |
| 同 goroutine 嵌套 `Do` | 重入加深一层，内层出栈后外层仍持有 |

周期任务「抢不到就本轮跳过」不要用 `Do`（它会排队），那种语义写 `TryLock` + `errors.Is(err, ErrBusy)`。

错误语义（三后端一致）：

| 情况 | 返回 |
|---|---|
| `TryLock` 时锁被他人持有 | 包装 `ErrBusy`，消息含 `key=` |
| `Lock` 等待到 `WaitTimeout` 或 ctx 截止 | 包装 `ErrTimeout`；**ctx 自身取消/超时则原样透传 `ctx.Err()`**，两者可区分 |
| 释放/续期自己没持有的锁 | 包装 `ErrNotHeld`（本地判定，不发后端请求） |
| 跨持有者释放（默认按 goroutine 绑定） | 包装 `ErrNotHeld`——绝不把别人的临界区提前打开 |
| 持锁期间租期到期被抢占 / 续期发现属主易主 | 同时包装 `ErrLost` 与 `ErrNotHeld`：临界区**可能已并发执行**，调用方应放弃本轮而非重试补偿 |
| 后端故障（Redis 断开、Consul 不可达、脚本异常） | 透传后端 error（带 `lock/xxx:` 前缀），**绝不返回 nil**（fail-closed） |
| `Close` 后继续使用 | 包装 `ErrClosed` |
| 配置非法（空 `Key`、`TTL < 1ms`、`RenewInterval >= TTL`、`PollInterval < 1ms` 等） | 构造期返回包装 `ErrInvalidConfig` 的 error |

## Config

各后端以内嵌方式复用同一份 `lock.Config`，**换后端不换默认值**：

| 字段 | 默认 | 语义 |
|---|---|---|
| `Key` | 必填 | 锁标识，如 `sharding:ddl:orders_202611`；实际存储键由后端加前缀 |
| `TTL` | 30s | 租期。持有者崩溃后最迟经此时长被抢占；`<=0` 取默认；**下限 1ms**（服务端以毫秒计时，亚毫秒租期会让加锁成为空操作，构造期即拒）|
| `WaitTimeout` | 0（不限） | `Lock` 最长等待，仅受 ctx 之外的约束；超时报 `ErrTimeout` |
| `PollInterval` | 100ms | 竞争重试间隔，内部加 **±25% 抖动**避免多副本同点风暴；下限 1ms |
| `RenewInterval` | `TTL/3` | 看门狗续期间隔；`-1` 关闭自动续期（此后长任务必须自行周期调用 `Refresh`）；`>= TTL` 直接报错 |
| `Owner` | 空 | 持有者标识。留空即默认 `OwnerFunc = GoroutineOwner`；设为固定串则同 Owner 视为同一持有者（不做 goroutine 隔离） |
| `OwnerFunc` | `GoroutineOwner` | 每次操作求值，优先级高于 `Owner` |
| `OnRenewError` | nil | 续期失败回调（看门狗 goroutine 内调用，不可阻塞）；nil 时静默，仅 `Held()` 变 false |

**默认 Owner 为什么是 goroutine 绑定**：`GoroutineOwner()` 返回 `<hostname>-<pid>-<8hex>-g<goid>`——进程前缀防止多进程 goid 串号误判重入，goid 让「同一 goroutine 递归加锁加深深度、跨 goroutine 仍需等待」成立，这正是契约里「可重入（goroutine 绑定）」的含义。推论：默认配置下**必须谁加锁谁在同一 goroutine 里解锁**，跨 goroutine 传递锁实例做解锁会拿到 `ErrNotHeld`；确需跨 goroutine 收尾（如统一的 `Close`）时组件仍会归还租约，不会白占到 TTL。

## API 参考

### Local 进程内锁

```go
mu, err := lock.New(lock.Config{Key: "job:report"})   // 零依赖，TTL 等字段仅参与校验
if err != nil { return err }
defer mu.Close()

if err := mu.TryLock(ctx); err != nil { /* ErrBusy = 已有 goroutine 在做 */ }
defer mu.Unlock(ctx)

mu.Held()   // 本 goroutine 视角是否持有
mu.Depth()  // 重入深度
mu.Fence()  // 单调递增栅栏值，释放后归零
mu.Stats()  // 运行快照（争用、超时、释放计数），见「观测与指标」
```

释放即唤醒等待者（`WaitAcquireSignal` 的广播渠道），因此 `PollInterval` 设得再大也不会让等待方空等；无远端 IO，`Close()` 恒返回 nil。

### redisstore Redis 后端

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/xm-utils/sunmao/lock/redisstore"
)

client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})  // 或由 redis 组件建立
mu, err := redisstore.New(redisstore.Config{
    Client: client,                                 // 接口类型：ClusterClient/Ring/UniversalClient 同可
    Config: lock.Config{Key: "deadletter:recover:order-pay", TTL: 30 * time.Second},
})

err = mu.Lock(ctx)                                 // 阻塞获取
err = mu.TryLock(ctx)                              // 非阻塞；ErrBusy 即本轮跳过
err = mu.Unlock(ctx)                               // ctx 已取消也会归还（内部用 Background 兜底）
err = mu.Refresh(ctx)                              // 手动续期（关闭看门狗时使用）
fence := mu.Fence()                                // 栅栏 token，随每次真正获取单调递增
left, err := mu.RemainingTTL(ctx)                  // 服务端剩余租期，观测/巡检用
depth := mu.Depth()                                // 服务端记录的重入深度的本地视图
st := mu.Stats()                                   // 运行快照（含 renewals/lost 计数）
err = mu.Close()
```

服务端形态：Hash `xm_lock:{<key>}`（字段 `owner` / `depth` / `fence`）+ 计数 Key `xm_lock:{<key>}:fence`。两者同 `{tag}` 同 slot，Cluster 下不会 `CROSSSLOT`。获取/释放/续期各自一段 Lua 脚本，单例复用（`EVALSHA` 优先，`NOSCRIPT` 自动回落 `EVAL`）：

- **获取**：空闲则 `HSET owner/depth=1` + `INCR` 栅栏 + `PEXPIRE`；同 owner 则 `HINCRBY depth 1` + `PEXPIRE`；他人持有返回 busy。全程原子，不存在「判完再写」的窗口。
- **释放**：属主校验通过后深度减一，归零才 `DEL`。Key 不存在（租期已过）报 `ErrNotHeld`，属主易主报 `ErrLost`。
- **续期**：属主校验通过才 `PEXPIRE`，否则报 `ErrLost` 并停看门狗。

`Config.Client` 是接口而非 `*redis.Client`：只需 `redis.Scripter`（`Eval`/`EvalSha`/`ScriptExists`/`ScriptLoad`，`redis.NewScript.Run` 本身就收 Scripter）加一个 `PTTL`。因此 `*redis.Client`、`*redis.ClusterClient`、`*redis.Ring` 与 `redis.UniversalClient` 都能直接传入，包末尾有四个编译期断言钉住。若传入的是类型化空指针（`var c *redis.Client` 再赋给接口），构造期的 `Client == nil` 判不到，会到首次获取时由 `attempt` 的 recover 转成 error，不会 panic 逸出。

### consulstore Consul 后端

```go
import (
    xmconsul "github.com/xm-utils/sunmao/consul"
    "github.com/xm-utils/sunmao/lock/consulstore"
)

consulClient, _ := xmconsul.NewClient(...)          // 任何满足 consulstore.Backend 的对象皆可
mu, err := consulstore.New(consulstore.Config{
    Backend: consulClient,                          // 编译期已断言 *consul.Client 满足 Backend
    Prefix:  "xm_lock",                             // 实际 KV 键：<Prefix>/<Key>
    Config:  lock.Config{Key: "sharding:ddl:orders", TTL: 60 * time.Second},
})

err = mu.Lock(ctx)
sid := mu.SessionID()                               // 本轮会话 ID，排障时去 Consul 查会话状态
kv := mu.KVKey()                                    // 实际 KV 键
st := mu.Stats()                                    // 运行快照；本后端无栅栏，Fence 恒为 0
err = mu.Unlock(ctx)
err = mu.Close()
```

会话在**首次真正获取时惰性创建**、深度归零或 `Close` 时 `Release`+`Destroy` 两步回收——不持锁时不白占一份会话。注意 Consul 的 `KV Release` 只解除会话绑定、**不删键**：释放后锁键会作为「`Session` 为空的墓碑」留下，值仍是上一个持有者的会话 ID（便于排障）；判断锁是否被持有要看 KV 的 `Session` 字段，不能看键是否存在。重入由本地 `depth` + Owner 校验拦下（同 Session 重复 `Acquire` 在 Consul 侧本就幂等，再由本地校验挡住跨 goroutine 误重入）。

`Backend` 是 5 个方法的最小能力集（`CreateSession`/`DestroySession`/`RenewSession`/`AcquireLock`/`ReleaseLock`），方法签名与 `consul` 组件客户端逐一对应；抽象这一层的唯一目的是让测试无需真实 Consul 就能覆盖全部判定分支，生产直接传组件客户端。

### 自定义后端时复用的两块

```go
// 等待语义：抖动轮询 + 双截止 + panic 隔离 + 异常返回检测
err := lock.WaitAcquire(ctx, &cfg, func(ctx context.Context) (acquired, busy bool, err error) {
    return /* 一次原子的服务端获取尝试 */
})
err := lock.WaitAcquireSignal(ctx, &cfg, attempt, wake) // wake 非 nil 时释放即唤醒

// 续期语义：周期 Tick，续期回调报 ErrLost/ErrNotHeld 即停止空转
wd, err := lock.NewWatchdog(&cfg, func(ctx context.Context) error { return myRenew(ctx) })
wd.Start(); defer wd.Stop()
stop := wd.Tick(ctx)   // 测试可显式驱动，无需等待真实时间
```

`Config.Normalize()` 与 `Config.CurrentOwner()` 也已导出，自定义后端复用同一套规范化默认值与 Owner 求值规则，保证「换后端不换语义」。

## 观测与指标

三端都实现了 `lock.Statser`，`Stats()` 只读本地快照（不发起后端请求、不加会与记账路径互等的锁），在临界区内调用同样安全：

```go
st := mu.Stats()
fmt.Println(st)                                                     // 一行摘要，适合直接进日志
fmt.Printf("争用率 %.2f 平均等待 %v\n", st.ContentionRatio(), st.AvgWait())
```

| 字段 | 口径 |
|---|---|
| `Attempts` / `Acquired` / `Reentered` / `Busy` | 获取侧：尝试轮次（轮询每轮各计一次）、首次获得、重入命中、撞锁 |
| `Timeouts` / `Canceled` / `Lost` / `Errors` | 失败归类，与四个哨兵错误一一对应；ctx 自身取消走 `Canceled`，等到 `WaitTimeout` 走 `Timeouts` |
| `Released` / `Decreased` / `ReleaseMisses` | 释放侧：深度归零、出栈但未放锁、被拒的释放 |
| `Renewals` / `RenewFailures` | 续期侧，含看门狗、手动 `Refresh` 与 `consulstore` 重入顺带续期 |
| `WaitNanos` / `Waits` / `HoldNanos` | 时长累计，配对 `AvgWait()` / `AvgHold()` 取均值；等到超时的回次也计等待，丢锁那一轮不计入持有 |
| `Depth` / `Holding` / `Fence` / `Closed` | 读取那一刻的本地视图（服务端是否仍归本实例以下次操作结果为准）|

计数字段自实例创建起累计，复位只能靠重建实例（与「一个 Key 一个常驻实例」的用法一致）。`Recorder` 也已导出：自定义后端内置它、在 attempt 与 API 边界记账，再导出一个 `Stats()` 就能接下面的采集器。

接 Prometheus 走 `metrics/adapter/lock`（拉取式 Collector，`lock` 侧零依赖 `metrics`，被适配组件不改动）：

```go
import xmlock "github.com/xm-utils/sunmao/metrics/adapter/lock"

c := xmlock.New(ddlMu, recoverMu)            // 若干 lock.Statser，后端混用亦可
// 装配层已把锁收进注册表时：xmlock.NewWithProvider(func() map[string]lock.Stats { ... })
metrics.RegisterExporter(c)
```

输出五个指标族，label 统一为 `(key, backend)`：

| 指标 | 类型 | 额外 label |
|---|---|---|
| `xmt_lock_operations_total` | counter | `op=acquire/release/renew`、`result=…` |
| `xmt_lock_errors_total` | counter | — |
| `xmt_lock_duration_seconds_total` | counter | `phase=wait/hold`（秒）|
| `xmt_lock_contention_ratio` | gauge | —（`Attempts=0` 时不产出）|
| `xmt_lock_state` | gauge | `field=holding/depth/fence/closed` |

## 场景接入示范

组件侧不改动 `sharding` / `deadletter`（适配器模式优先），但用测试复现了两个场景的完整语义：`lock/local_test.go`、`redisstore`、`consulstore` 三处各有 `TestScenarioShardingConcurrentDDL` 与 deadletter 多副本捞取用例。装配层接入形态如下。

**① sharding 并发 DDL**（`sharding.go:336` / `primary_key.go:60` 那两条 `CREATE TABLE IF NOT EXISTS`）：

```go
// 启动期每个实例都会尝试建表；用 Lock 串行化，后进场者拿到锁时表已存在
if err := ddlMu.Lock(ctx); err != nil && !errors.Is(err, lock.ErrTimeout) {
    return fmt.Errorf("建表锁获取失败: %w", err)
}
defer func() { _ = ddlMu.Unlock(ctx) }()   // ErrNotHeld/ErrLost 只记录，不改变已建表的事实

if err := db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (id INT NOT NULL)").Error; err != nil {
    return err
}
```

断言的不变式：`ddlCalls == 1`、其余实例走「已存在」分支（`skipCalls == instances-1`）。

**② deadletter 多副本重复捞取**（`manager.go:76 StartRecovery` 的 ticker 回调）：

```go
// 每副本各自 ticker；TryLock 让同一轮只有一个副本真正捞取
func (m *QueueManager) recoverAndRetryLocked() error {
    if err := m.recoverMu.TryLock(context.Background()); err != nil {
        if errors.Is(err, lock.ErrBusy) {
            return nil    // 别的副本正在捞，本轮跳过即可，不是错误
        }
        if errors.Is(err, lock.ErrLost) {
            return nil    // 属主已失，本轮结果不可信，交给下一轮，绝不重试补偿
        }
        return err        // 后端故障：fail-closed，宁可本轮不跑
    }
    defer func() { _ = m.recoverMu.Unlock(context.Background()) }()

    // 捞取 + 重投……耗时若可能超过 TTL/2，要么留看门狗，要么自行周期 Refresh
    return nil
}
```

断言的不变式（`TryLock` 的正确用法不是「每轮恰好一次」而是「同一时刻只有一个副本在执行」）：`maxActive == 1`、每轮至少一次成功捞取、其余副本判 `ErrBusy` 跳过。

## 后端差异（避免踩坑）

| 差异点 | `redisstore` | `consulstore` |
|---|---|---|
| 栅栏 token | ✅ `Fence()`，服务端 `INCR` 单调 | ❌ 不提供；需要它请用 Redis |
| 重入计数落点 | **服务端** `depth` 字段，跨进程可见 | **本地** `depth`，同 Session 重复 Acquire 幂等 |
| ctx 覆盖范围 | 获取、释放、续期、剩余租期查询 | 仅获取等待；`consul` 组件客户端方法不吃 ctx，释放/续期无法被 ctx 打断 |
| 释放请求的 ctx 兜底 | ctx 已取消时内部换 `context.Background()` 保证归还 | 无 ctx 概念，直接发起释放 |
| 属主已失的暴露点 | 释放脚本返回码 / 续期脚本返回码 | `ReleaseLock` 返回 false / `RenewSession` 报错**或返回空条目**（真实 consul/api 对已失效会话给 `(nil, nil)`，本包两种形态都判为 `ErrLost`） |
| 崩溃后回收 | Key `PEXPIRE` 到点消失 | 会话 TTL 到点由 Consul 释放其占用 KV，**但他人接手还要等会话的 `lock_delay`（默认 15s）** |
| 运维可见性 | `HGETALL xm_lock:{key}`、`PTTL` | Consul UI/CLI 的 KV 目录 + `consul session list`（会话名带锁 Key 与持有者） |
| `DestroySession` 失败 | 不适用 | 锁已释放的事实不变，错误里明确说明会话将在 TTL 后自动回收 |

## 配置与热更新

配置模板见 [config.example.yaml](config.example.yaml)（装配层同构 struct 的键位说明与加载示例）。本组件**不内置 YAML 解析与配置源监听**，以保持根包零依赖：

1. 装配层把 yaml 反序列化进同构 struct，映射为 `lock.Config` / `redisstore.Config` / `consulstore.Config`，构造期就暴露非法值；
2. 实例与 Key 一一绑定，建议**常驻复用**（一个 Key 一个实例，多 goroutine 并发使用），不要每次加锁都 `New`——那样每次多一轮会话建立；
3. TTL、续期间隔等在构造期定形，不做运行期热改（改租期等于改安全边界，应走发布而非热更新）；若确需动态调整，按新配置重建实例并 `Close` 旧的。配置源监听可挂 `registry/config.Watch`，与 `logging` 动态调级同一手法。

## 与其他组件联动

| 组件 | 方式 |
|---|---|
| `redis` | 沿用其配置形态（addr/password/db）；因组件 `client` 未导出、全局 `Eval` 有 KEYS 双前缀问题，`redisstore` 直接接收使用方自己的客户端（`redisstore.Client` 接口，组件建好的 `*redis.Client` 可直接传入）执行脚本，**不改组件本体** |
| `consul` | `consulstore.Backend` 的方法集即 `*consul.Client` 的方法集（文件末尾有编译期断言，consul 侧改签名即编译失败），组件本体零改动 |
| `gorm/database/sharding` | 启动期 DDL 串行化的目标场景。接入在装配层（sharding 零改动），见上文示范 |
| `deadletter` | 多副本恢复去重的目标场景。同上，`StartRecovery` 的回调外包一层 `TryLock` |
| `registry` / `registry/config` | 后续 `scheduler`（3.3.2）用本组件做「多实例仅一个执行」的选主原语；锁配置可由 `registry/config` 下发 |
| `logging` | `OnRenewError` 回调接 `logging` 打告警级日志；`ErrLost` 出现必须显式记录——它意味着临界区可能已并发执行 |
| `metrics` | 已有 `metrics/adapter/lock` 拉取式采集器：把任意 `lock.Statser`（三端实例或自定义后端）交给 `New(...)` 即出 `xmt_lock_*` 指标，本组件保持零依赖 `metrics`，见「观测与指标」 |

## 测试

```bash
cd lock
GOWORK=off go test -race -count=2 ./...
```

- 根包：`Normalize` 默认值与全部非法值、`GoroutineOwner` 同/跨 goroutine 差异、`WaitAcquire` 重试成功/超时/ctx 透传/飞行中超时归类/attempt panic 隔离/异常返回检测/唤醒信号、`Watchdog.Tick` 四种错误语义表驱动 + 双 panic 隔离 + `Start/Stop` 幂等；
- `Do`（`do_test.go`）：参数校验、临界区内持有且返回后释放、获取失败不执行 `fn`、业务错误不被释放错误覆盖（`errors.Join` 两边都可 `errors.Is`）、`fn` panic 仍归还锁且 panic 原样抛回、同 goroutine 嵌套重入；
- `Local`：TryLock 互斥、同 goroutine 重入（栅栏值不变、深度 2）、跨 goroutine 释放被拒、释放即唤醒、等待超时、ctx 取消、已取消 ctx 下仍能释放、`Close` 复位与 `ErrClosed`、栅栏单调、两实例互不相知、静态 Owner 语义、32×20 并发临界区峰值恰为 1；
- `redisstore`：基于 `miniredis` 离线运行，覆盖服务端字段/深度/栅栏断言、`FastForward` 驱动租期过期与抢占、看门狗续期延长 `PTTL`、篡改属主后 `Tick` 停轮并回 `ErrLost`、`Refresh` 手动续期、后端断开时 fail-closed、跨 goroutine 释放被拒而 `Close` 仍能归还、**在飞续期与释放互不等（`TestUnlockWithRenewInFlightDoesNotDeadlock`）**、`redis.UniversalClient` 走完整获取/释放与类型化空客户端转 error、3 实例 36 并发峰值恰为 1；
- `consulstore`：自建 `fakeBackend` 复刻 Session+KV 语义，覆盖会话惰性创建与销毁、busy 路径销毁临时会话、重入只加本地深度并顺带续期、会话 TTL 回收后被他副本抢占、`ReleaseLock` 被拒报 `ErrLost`、**续期返回 `(nil, nil)` 时同样判 `ErrLost`**（真实 consul/api 的形态，Refresh/看门狗/重入三条路径）、`DestroySession` 失败时「锁已释放」事实不变、Backend panic 一律转 error、跨 goroutine 释放被拒而 `Close` 仍能归还、在飞续期与释放互不等、3 副本 × 10 轮并发峰值恰为 1；
- 两个场景用例（DDL、死信捞取）在三处各跑一遍，把「只执行一次 / 同一时刻只一个副本」的不变式钉成契约；
- 观测（`stats_test.go` 与两端 `stats_test.go`）：`Recorder` 失败归类（`ErrBusy` 不计错、`ErrLost` 作废持有时长）、`Local` 完整生命周期计数、`Attempts` 恒等式（8×25 并发下 `Attempts == Acquired+Reentered+Busy+异常轮次`）、三端 `Stats()` 的 backend/栅栏/续期口径、`Statser` 契约断言；
- 示例（`example_test.go`、`redisstore/example_test.go`、`consulstore/example_test.go`）：全部带 `// Output:` 断言，由 `go test` 执行，覆盖重入与 Owner 策略、`Do` 的错误/panic 归还、TryLock 跳过、超时与 ctx 取消归类、栅栏单调、服务端过期与丢锁、`Stats()` 与 `Statser` 聚合、按可选接口探测后端能力——文档与行为同源，API 变了示例即失败。

### 集成用例（需真实服务，默认不跑）

```bash
cd lock
XM_LOCK_TEST_REDIS_ADDR=127.0.0.1:6379 \
XM_LOCK_TEST_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001 \
XM_LOCK_TEST_CONSUL_ADDR=127.0.0.1:8500 \
  go test -tags integration -race -count=1 -timeout 180s ./...
```

两个后端的 `integration_test.go` 挂在 `//go:build integration` 标签下：**不带标签的 `go test` 永远不编译它们**，所以没装 Redis/Consul 的机器与 CI 默认作业不会被拖红；加了标签但服务不可用时逐条 `t.Skip` 并打印补救提示，同样不判失败。

它们只验内存替身兜不住的事——真服务端的 `PEXPIRE`/`EVALSHA` 行为、真实时钟下的看门狗续期、真会话回收与 `ErrLost` 归类：

| 用例 | 覆盖点 |
|---|---|
| `redisstore/TestIntegrationExclusiveAndRelease` | 两副本互斥、服务端 `HGET owner/depth` 直读、跨持有者释放被拒、释放后 Hash 消失 |
| `redisstore/TestIntegrationRealTTLExpiryAndLost` | 真实时钟下 TTL 到点：`RemainingTTL` 归零、他人抢占、旧持有者 `ErrLost`、`Stats().Lost==1` 且 `HoldNanos==0` |
| `redisstore/TestIntegrationWatchdogKeepsLease` | TTL 2s 持有 5s（跨约 7 个续期周期）不被抢占，`Renewals>=3` 且零失败 |
| `redisstore/TestIntegrationFenceMonotonicAcrossCycles` | 3 轮换人的栅栏单调递增，且栅栏 Key 不随释放回收 |
| `redisstore/TestIntegrationConcurrentHoldersSerialised` | 8 副本 × `lock.Do` 并发，临界区峰值并发恰为 1 |
| `redisstore/TestIntegrationDoReleasesOnPanic` | panic 穿过 `Do` 后服务端键已归还，其他副本立刻可拿 |
| `redisstore/TestIntegrationClusterScriptsAndSameSlot` | `CLUSTER KEYSLOT` 对比两个 Key 同槽 + `ClusterClient` 走完整获取/释放（未配 env 则跳过） |
| `consulstore/TestIntegrationSessionLifecycle` | 持锁期间 KV 绑定本会话且旁路会话抢不动；释放后 KV 的 `Session` 已清空（墓碑仍在）、会话不可再续、旁路会话可获得且能归还，随后 b 可获取 |
| `consulstore/TestIntegrationExpiryPreemption` | 会话被 TTL 回收：`Renew` 只返空条目不报错、`Refresh` 报 `ErrLost`、本地视图作废、KV 不再绑定会话；再轮询等到 `lock_delay`（实测约 15s）后 b 完成抢占 |
| `consulstore/TestIntegrationWatchdogHoldsLease` | 跨 2×生效租期持续续期不被抢占，零续期失败 |
| `consulstore/TestIntegrationBackendRejectsForeignRelease` | 直接走 `Backend` 核对 Consul 的 CAS 语义（不依赖组件本地校验） |

环境变量一览（均有缺省值，仅显式指定时才生效）：

| 变量 | 缺省 | 用途 |
|---|---|---|
| `XM_LOCK_TEST_REDIS_ADDR` | `127.0.0.1:6379` | 单机/主从地址 |
| `XM_LOCK_TEST_REDIS_PASSWORD` | 空 | 密码 |
| `XM_LOCK_TEST_REDIS_DB` | `0` | 库号 |
| `XM_LOCK_TEST_REDIS_CLUSTER_ADDRS` | 空（跳过 Cluster 用例） | 逗号分隔的节点列表 |
| `XM_LOCK_TEST_CONSUL_ADDR` | `127.0.0.1:8500` | Consul agent 地址 |
| `XM_LOCK_TEST_CONSUL_TOKEN` | 空 | ACL Token |
| `XM_LOCK_TEST_CONSUL_MAXTTL` | `30`（秒） | 会话生效租期超过此值则跳过两条耗时用例（`session_ttl_min` 配得很大的 agent） |
| `XM_LOCK_TEST_CONSUL_LOCKDELAY` | `15`（秒） | 过期用例等待抢占的上限，需与 agent 会话的 `lock_delay` 对齐（本组件不传该字段，故为 Consul 默认 15s） |

集成用例只在自建的 `xmlock_it:*` / `xmlock_it/*` 键上动作，`t.Cleanup` 里删除锁键（含 Consul 的 `Session` 已清空的墓碑）、销毁会话，**不会碰库里的其他键，也不做 `FLUSHDB`/`purge`**。耗时看真实租期：Consul agent 默认 `session_ttl_min=10s` 时 consulstore 整批约 30~40s（过期用例还要叠加 15s `lock_delay`），把 agent 配成 1s 后实测约 22s；redisstore 整批约 8s。建议单独作业而非每次 PR。

本地起一个适合跑集成用例的 Consul（`session_ttl_min` 只能走配置文件，不是 CLI flag）：

```bash
echo '{"session_ttl_min": "1s"}' > /tmp/consul_it.json
docker run -d --name xmlock-consul-it -p 127.0.0.1:8500:8500 \
  -v /tmp/consul_it.json:/consul/config/it.json:ro \
  hashicorp/consul:1.22.7 consul agent -dev -client 0.0.0.0 -config-dir=/consul/config
```

## 已知边界

- **不做 Redlock 多节点算法**：`redisstore` 是单 Key（或同一 `{tag}` slot）方案，主从切换时理论上存在丢锁窗口。对强一致有要求的场景，用 `consulstore`（Raft 多数派）；
- **Cluster 只做到「用例可跑」而非「CI 常驻」**：`redisstore.Client` 已接受 `*redis.ClusterClient`，锁 Key 的 `{tag}` 也能保证两个 Key 同 slot，`TestIntegrationClusterScriptsAndSameSlot` 会用 `CLUSTER KEYSLOT` 核对同槽并跑一轮完整获取/释放；但 `miniredis` 不支持 `CLUSTER` 命令，该用例需要显式配 `XM_LOCK_TEST_REDIS_CLUSTER_ADDRS` 才生效，默认作业里不会跑——首次上 Cluster 前请实跑一次（并顺手跑 DDL 场景）；同理，`redis.Ring` 的一致性哈希由客户端控制，请确认 Key 真的落同一分片；
- **等待不保证公平**：抖动轮询不排队，极端情况下某副本可能长时间抢不到。需要严格 FIFO 请自行用 Redis `BLPOP` 排队或在 `consul` 侧用 blocking query（后者不吃 ctx，取消语义会失真，故未采用）；
- **`ErrLost` 只报告事实、不做补偿**：一旦出现说明临界区可能已并发执行，组件不会自动重跑，调用方必须按幂等设计任务；
- **`Local` 无租期**：`TTL`/`RenewInterval` 只参与校验，持锁 goroutine 泄漏就永久阻塞同实例等待者——用它做初始化保护时要确保 `defer Unlock`；
- **`consulstore` 无栅栏 token**：需要「旧持有者写入被拒」的场景请用 Redis（递增计数随写提交）；
- **Consul 服务端的 `session_ttl_min`（默认 10s）会抬高更小的 TTL**：把 `TTL` 配成 3s 并不会让崩溃回收变快，实际租约由服务端参数决定——要调快回收请改 Consul 的 `session_ttl_min`，而不是组件配置；
- **崩溃交接 = TTL + `lock_delay`（实测约 15s）**：持有者未显式释放而会话被 TTL 回收时，Consul 会对该键施加会话的 `lock_delay`（默认 15s），期间他人 `Acquire` 一律返回 false。所以 `WaitTimeout` 要按「TTL + 15s」量级设，而不是只看 TTL；正常 `Unlock` 显式释放不触发该延迟，他人可立刻接手（集成用例已钉住两种路径）。想去掉这 15s 只能在自己创建的会话上设 `LockDelay=0`，需自备 `Backend` 实现（`consul` 组件的 `CreateSession` 不透出该字段）；
- **`consulstore` 释放后 KV 不删**：`Release` 只解绑会话，锁键作为墓碑留下（值为上一个持有者的会话 ID）；静态 Key 下不会无限增长，但按日期/任务 ID 动态生成海量 Key 时需自行回收；
- **`redisstore` 的栅栏计数 Key 常驻**：`xm_lock:{key}:fence` 不过期也不删除（单调性所需），每个锁 Key 对应一个极小 Key；动态创建海量锁 Key 的场景需自行回收；
- **`consulstore` 等待期间每轮尝试建/销一次会话**：会话创建是 Raft 写，长时间 `Lock` 等待应上调 `PollInterval`（默认 100ms 对分钟级等待仍偏激进），能接受跳过就优先 `TryLock`；
- **`redisstore` 每次获取一个 RTT**：跨机房部署时轮询等待会放大抖动，建议 `PollInterval` 上调（默认 100ms 起）并优先 `TryLock` 跳过；
- **计数是累计值而非区间值**：`Stats()` 与 `xmt_lock_contention_ratio` 反映实例创建至今的总账，需要区间竞争率请对 `xmt_lock_operations_total` 做 `rate()` 推导；重启进程即归零，多副本聚合看指标时记得按 `instance` 维度分开。

## 许可

MIT
