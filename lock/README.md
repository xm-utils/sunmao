# lock — 分布式互斥锁组件

跨实例协同原语：**一个 `Mutex` 契约 + 进程内 / Redis / Consul 三类后端，租期与看门狗续期、try-lock、阻塞获取、goroutine 绑定重入、ctx 取消感知全部统一**。对应架构路线图 3.3.1（P1）。

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
├── config.example.yaml         # 配置模板（装配层同构 struct）
├── redisstore/                 # Redis 后端：Lua 原子脚本 + PEXPIRE 租期 + 栅栏 token
└── consulstore/                # Consul 后端：Session(TTL 租约) + KV Acquire/Release
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
| 典型场景 | 单测、单机部署、同进程多 goroutine 初始化竞争 | 已有 Redis、需要栅栏 token 防旧持有者写入 | 已用 Consul 做注册中心、想少一套存储 |

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
```

三个方法**一律不 panic**：后端异常、内部脚本返回值异常、回调 panic 全部 recover 后包成 error（仓库设计约定二）。判定一律用 `errors.Is`，不要比对字符串。

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
| 配置非法（空 `Key`、`RenewInterval >= TTL`、`PollInterval < 1ms` 等） | 构造期返回包装 `ErrInvalidConfig` 的 error |

## Config

各后端以内嵌方式复用同一份 `lock.Config`，**换后端不换默认值**：

| 字段 | 默认 | 语义 |
|---|---|---|
| `Key` | 必填 | 锁标识，如 `sharding:ddl:orders_202611`；实际存储键由后端加前缀 |
| `TTL` | 30s | 租期。持有者崩溃后最迟经此时长被抢占；`<=0` 取默认 |
| `WaitTimeout` | 0（不限） | `Lock` 最长等待，仅受 ctx 之外的约束；超时报 `ErrTimeout` |
| `PollInterval` | 100ms | 竞争重试间隔，内部加 **±25% 抖动**避免多副本同点风暴；下限 1ms |
| `RenewInterval` | `TTL/3` | 看门狗续期间隔；`-1` 关闭自动续期（此后长任务必须自行周期调用 `Refresh`）；`>= TTL` 直接报错 |
| `Owner` | 空 | 持有者标识。留空即默认 `OwnerFunc = GoroutineOwner`；设为固定串则同 Owner 视为同一持有者（不做 goroutine 隔离） |
| `OwnerFunc` | `GoroutineOwner` | 每次操作求值，优先级高于 `Owner` |
| `OnRenewError` | nil | 续期失败回调（看门狗 goroutine 内调用，不可阻塞）；nil 时静默，仅 `Held()` 变 false |

**默认 Owner 为什么是 goroutine 绑定**：`GoroutineOwner()` 返回 `<hostname>-<pid>-<8hex>-g<goid>`——进程前缀防止多进程 goid 串号误判重入，goid 让「同一 goroutine 递归加锁加深深度、跨 goroutine 仍需等待」成立，这正是路线图 3.3.1 要的「可重入（goroutine 绑定）」。推论：默认配置下**必须谁加锁谁在同一 goroutine 里解锁**，跨 goroutine 传递锁实例做解锁会拿到 `ErrNotHeld`；确需跨 goroutine 收尾（如统一的 `Close`）时组件仍会归还租约，不会白占到 TTL。

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
    Client: client,
    Config: lock.Config{Key: "deadletter:recover:order-pay", TTL: 30 * time.Second},
})

err = mu.Lock(ctx)                                 // 阻塞获取
err = mu.TryLock(ctx)                              // 非阻塞；ErrBusy 即本轮跳过
err = mu.Unlock(ctx)                               // ctx 已取消也会归还（内部用 Background 兜底）
err = mu.Refresh(ctx)                              // 手动续期（关闭看门狗时使用）
fence := mu.Fence()                                // 栅栏 token，随每次真正获取单调递增
left, err := mu.RemainingTTL(ctx)                  // 服务端剩余租期，观测/巡检用
depth := mu.Depth()                                // 服务端记录的重入深度的本地视图
err = mu.Close()
```

服务端形态：Hash `xm_lock:{<key>}`（字段 `owner` / `depth` / `fence`）+ 计数 Key `xm_lock:{<key>}:fence`。两者同 `{tag}` 同 slot，Cluster 下不会 `CROSSSLOT`。获取/释放/续期各自一段 Lua 脚本，单例复用（`EVALSHA` 优先，`NOSCRIPT` 自动回落 `EVAL`）：

- **获取**：空闲则 `HSET owner/depth=1` + `INCR` 栅栏 + `PEXPIRE`；同 owner 则 `HINCRBY depth 1` + `PEXPIRE`；他人持有返回 busy。全程原子，不存在「判完再写」的窗口。
- **释放**：属主校验通过后深度减一，归零才 `DEL`。Key 不存在（租期已过）报 `ErrNotHeld`，属主易主报 `ErrLost`。
- **续期**：属主校验通过才 `PEXPIRE`，否则报 `ErrLost` 并停看门狗。

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
err = mu.Unlock(ctx)
err = mu.Close()
```

会话在**首次真正获取时惰性创建**、深度归零或 `Close` 时 `Release`+`Destroy` 两步回收——不持锁时不白占一份会话。重入由本地 `depth` + Owner 校验拦下（同 Session 重复 `Acquire` 在 Consul 侧本就幂等，再由本地校验挡住跨 goroutine 误重入）。

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

`Config.Normalize()` 与 `Config.CurrentOwner()` 也已导出，自定义后端复用电平化默认值与 Owner 求值规则，保证「换后端不换语义」。

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
| 属主已失的暴露点 | 释放脚本返回码 / 续期脚本返回码 | `ReleaseLock` 返回 false / `RenewSession` 报错 |
| 崩溃后回收 | Key `PEXPIRE` 到点消失 | 会话 TTL 到点由 Consul 释放其占用 KV |
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
| `redis` | 沿用其配置形态（addr/password/db）；因组件 `client` 未导出、全局 `Eval` 有 KEYS 双前缀问题，`redisstore` 直接接收使用方持有的 `*redis.Client` 执行脚本，**不改组件本体** |
| `consul` | `consulstore.Backend` 的方法集即 `*consul.Client` 的方法集（文件末尾有编译期断言，consul 侧改签名即编译失败），组件本体零改动 |
| `gorm/database/sharding` | 启动期 DDL 串行化的目标场景。接入在装配层（sharding 零改动），见上文示范 |
| `deadletter` | 多副本恢复去重的目标场景。同上，`StartRecovery` 的回调外包一层 `TryLock` |
| `registry` / `registry/config` | 后续 `scheduler`（3.3.2）用本组件做「多实例仅一个执行」的选主原语；锁配置可由 `registry/config` 下发 |
| `logging` | `OnRenewError` 回调接 `logging` 打告警级日志；`ErrLost` 出现必须显式记录——它意味着临界区可能已并发执行 |
| `metrics` | 暂无适配器。锁的争用/续期计数若要上指标，按 `metrics/adapter/*` 手法在 metrics 侧加拉取式适配器（本组件保持零依赖 `metrics`） |

## 测试

```bash
cd lock
GOWORK=off go test -race -count=2 ./...
```

- 根包：`Normalize` 默认值与全部非法值、`GoroutineOwner` 同/跨 goroutine 差异、`WaitAcquire` 重试成功/超时/ctx 透传/飞行中超时归类/attempt panic 隔离/异常返回检测/唤醒信号、`Watchdog.Tick` 四种错误语义表驱动 + 双 panic 隔离 + `Start/Stop` 幂等；
- `Local`：TryLock 互斥、同 goroutine 重入（栅栏值不变、深度 2）、跨 goroutine 释放被拒、释放即唤醒、等待超时、ctx 取消、已取消 ctx 下仍能释放、`Close` 复位与 `ErrClosed`、栅栏单调、两实例互不相知、静态 Owner 语义、32×20 并发临界区峰值恰为 1；
- `redisstore`：基于 `miniredis` 离线运行，覆盖服务端字段/深度/栅栏断言、`FastForward` 驱动租期过期与抢占、看门狗续期延长 `PTTL`、篡改属主后 `Tick` 停轮并回 `ErrLost`、`Refresh` 手动续期、后端断开时 fail-closed、跨 goroutine 释放被拒而 `Close` 仍能归还、3 实例 36 并发峰值恰为 1；
- `consulstore`：自建 `fakeBackend` 复刻 Session+KV 语义，覆盖会话惰性创建与销毁、busy 路径销毁临时会话、重入只加本地深度并顺带续期、会话 TTL 回收后被他副本抢占、`ReleaseLock` 被拒报 `ErrLost`、`DestroySession` 失败时「锁已释放」事实不变、Backend panic 一律转 error、跨 goroutine 释放被拒而 `Close` 仍能归还、3 副本 × 10 轮并发峰值恰为 1；
- 两个场景用例（DDL、死信捞取）在三处各跑一遍，把「只执行一次 / 同一时刻只一个副本」的不变式钉成契约。

## 已知边界与待办

- **不做 Redlock 多节点算法**：`redisstore` 是单 Key（或同一 `{tag}` slot）方案，主从切换时理论上存在丢锁窗口。对强一致有要求的场景，用 `consulstore`（Raft 多数派）或等 `lock` 增加 `Multi` 聚合后端；
- **等待不保证公平**：抖动轮询不排队，极端情况下某副本可能长时间抢不到。需要严格 FIFO 请自行用 Redis `BLPOP` 排队或在 `consul` 侧用 blocking query（后者不吃 ctx，取消语义会失真，故未采用）；
- **`ErrLost` 只报告事实、不做补偿**：一旦出现说明临界区可能已并发执行，组件不会自动重跑，调用方必须按幂等设计任务；
- **`Local` 无租期**：`TTL`/`RenewInterval` 只参与校验，持锁 goroutine 泄漏就永久阻塞同实例等待者——用它做初始化保护时要确保 `defer Unlock`；
- **`consulstore` 无栅栏 token**：需要「旧持有者写入被拒」的场景请用 Redis，或等 `consul` 组件透出 `ModifyIndex`；
- **`redisstore` 每次获取一个 RTT**：跨机房部署时轮询等待会放大抖动，建议 `PollInterval` 上调（默认 100ms 起）并优先 `TryLock` 跳过；
- 待办：`metrics/adapter/lock`（争用与续期计数）、`scheduler`（3.3.2）落地后把「选主执行」的锁调用收进调度器、`mq/redis/stream` pending 巡检启用时接入本组件。

## 许可

MIT
