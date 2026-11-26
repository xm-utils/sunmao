# lock — 5 分钟上手

从零到能用：进程内锁 → 分布式锁 → 租期与续期 → 重入语义 → 错误分级 → 排障与指标。

想看能跑的完整用法，直接读 `example_test.go` 与两个后端的 `example_test.go`——它们带 `// Output:` 断言，跑 `go test ./...` 就在验证，不会过期。

## 1. 引入与最小可用

```bash
go get github.com/xm-utils/sunmao/lock
```

```go
import "github.com/xm-utils/sunmao/lock"

mu, err := lock.New(lock.Config{Key: "job:daily-report"})  // 进程内实现，零仓外依赖
if err != nil {
    return err                                             // 空 Key、续期>=租期等在此暴露
}
defer mu.Close()

if err := mu.Lock(ctx); err != nil {                       // 阻塞获取
    return err
}
defer func() { _ = mu.Unlock(ctx) }()                      // 谁加锁谁解锁

// 临界区
```

`lock.Config` 零值即安全：`TTL` 30s（下限 1ms）、轮询 100ms 带 ±25% 抖动、续期 `TTL/3`、重入按 goroutine 绑定。**记住这个前提：`Local` 的互斥范围只在同一实例内**，多副本部署要换后端（见第 3 步）。

不想自己管解锁点就用 `lock.Do`，获取-执行-释放一次调用完成（契约层可用，换后端不影响）：

```go
err := lock.Do(ctx, mu, func() error {
    return buildReport()          // panic 也会先归还锁再把 panic 抛回
})
```

业务错误与释放错误同时存在时 `Do` 用 `errors.Join` 合并返回，不会拿释放错误盖掉业务错误。

## 2. 定时任务：抢不到就跳过（`TryLock`）

死信恢复、巡检这类「每个副本都有 ticker」的任务，正确姿势不是等待而是跳过：

```go
func (r *Recoverer) tick() error {
    if err := r.mu.TryLock(context.Background()); err != nil {
        if errors.Is(err, lock.ErrBusy) {
            return nil                     // 别的副本正在捞，本轮跳过，不是错误
        }
        if errors.Is(err, lock.ErrLost) {
            return nil                     // 属主已失，本轮结果不可信，交给下一轮
        }
        return err                         // 后端故障：fail-closed，宁可本轮不跑
    }
    defer func() { _ = r.mu.Unlock(context.Background()) }()

    return r.recoverOnce()                 // 捞取 + 重投
}
```

不变式：**同一时刻只有一个副本在执行捞取**。多副本下每轮都会有一批 `ErrBusy`，属正常现象，不要当错误告警。

## 3. 换后端只换构造器（业务代码不动一行）

`Lock` / `TryLock` / `Unlock` / `Close` 三端完全一致，改的只有构造那几行：

```go
var mu lock.Mutex   // 上层只依赖契约，不依赖具体后端

// ① 进程内（单测、单机部署）
mu, err = lock.New(lock.Config{Key: "sharding:ddl:orders_202611", TTL: 60 * time.Second})

// ② Redis（跨实例互斥 + 栅栏 token）
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})   // 或 redis 组件建好的客户端
mu, err = redisstore.New(redisstore.Config{
    Client: client,                       // 接口类型：ClusterClient / Ring / UniversalClient 同可
    Config: lock.Config{Key: "sharding:ddl:orders_202611", TTL: 60 * time.Second},
})

// ③ Consul（跨实例互斥 + 会话 TTL 原生回收）
mu, err = consulstore.New(consulstore.Config{
    Backend: consulClient,                                          // 直接传 consul 组件客户端
    Config:  lock.Config{Key: "sharding:ddl:orders_202611", TTL: 60 * time.Second},
})
```

启动期 DDL 串行化就用 ②/③ + `Lock`（后进场者拿到锁时表已存在，走 `IF NOT EXISTS` 空转分支即可）。

## 4. 租期与看门狗：别让锁在半路被抢走

持有者进程崩溃时，锁靠租期兜底（Redis `PEXPIRE` / Consul Session TTL）。**租期内的正常长任务靠看门狗续命**：

```go
mu, err := redisstore.New(redisstore.Config{
    Client: client,
    Config: lock.Config{
        Key:           "sharding:ddl:orders_202611",
        TTL:           60 * time.Second,   // 必须 ≥ 2 × 最坏临界区耗时
        RenewInterval: 0,                  // 0 → 自动取 TTL/3 = 20s
        OnRenewError: func(err error) {    // 续期失败回调（看门狗 goroutine 内，勿阻塞）
            logging.Context(ctx).Errorf("锁续期失败: %v", err)   // ErrLost 必须显式记录
        },
    },
})
```

- 默认配置就够跑绝大多数任务：首次获得锁自动起看门狗，深度归零或 `Close` 自动停；
- **长任务 + 不想后台续期**：`RenewInterval: -1` 关闭看门狗，此后必须自己周期调 `Refresh`：

```go
// Refresh 是服务端后端才有的能力（lock.Refresher）；mu 若声明为 lock.Mutex 契约，取一次断言即可
rmu := mu.(lock.Refresher)

stop := make(chan struct{})
go func() {                          // 自带调度器的场景（如 worker 池统一节拍）
    t := time.NewTicker(10 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-t.C:
            if err := rmu.Refresh(ctx); err != nil {   // ErrLost = 已被抢占，立刻收手
                return
            }
        case <-stop:
            return
        }
    }
}()
```

- 一旦 `ErrLost` 出现，说明**临界区可能已被并发执行**：任务必须幂等，且不要靠重试补偿。

## 5. 重入语义：默认按 goroutine 绑定

```go
func (s *Service) Run(ctx context.Context) error {
    if err := mu.Lock(ctx); err != nil { return err }
    defer func() { _ = mu.Unlock(ctx) }()

    return s.inner(ctx)                 // 同 goroutine 递归加锁 → 深度 2，不阻塞
}

func (s *Service) inner(ctx context.Context) error {
    if err := mu.Lock(ctx); err != nil { return err }   // 重入，直接返回 nil
    defer func() { _ = mu.Unlock(ctx) }()
    ...
}
```

| 想要 | 怎么配 | 行为 |
|---|---|---|
| goroutine 绑定重入（默认） | 不填 `Owner`/`OwnerFunc` | 同 goroutine 递归加深深度；跨 goroutine 必须等待；**必须同 goroutine 解锁** |
| 按持有者标识共享重入 | `Owner: "order-svc-3"` | 同 Owner 即同持有者，跨 goroutine 也算重入、也能解锁——失去 goroutine 隔离 |
| 完全关闭重入 | `OwnerFunc: func() string { return 每轮唯一串 }` | 每次获取都是新属主，递归会自锁，适合「一次一用」的任务锁 |

注意：`Owner` 只在**同一存储 Key 上**比较属主，所以跨进程共享重入必须所有副本用同一个静态串（如 `podName`），否则会各拿一次深度。

## 6. 错误分级：`errors.Is` 一张表

```go
switch {
case err == nil:
    // 进入临界区
case errors.Is(err, lock.ErrBusy):
    return nil                       // 本轮跳过（TryLock 的正常结果）
case errors.Is(err, lock.ErrTimeout):
    return nil                       // 排队没等到，降级/延后，别当致命错误
case errors.Is(err, lock.ErrLost):
    log.Errorf("锁已易主，本轮结果不可信")   // 临界区可能并发过：告警 + 幂等复核
case errors.Is(err, lock.ErrNotHeld):
    log.Warnf("释放未持有的锁")        // 通常是解锁点写错/跨持有者解锁
case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
    return nil                       // 调用方自己的 ctx，原样透传，可区分于超时
default:
    return err                       // 后端故障（fail-closed：绝不会伪装成"拿到锁"）
}
```

`ErrLost` 与 `ErrNotHeld` 会同时出现在错误链里（`%w` 多重包装），两者都能 `errors.Is` 命中，按你关心的粒度选。

## 7. 观测与排障

```go
mu.Fence()                       // redisstore/Local：栅栏值，交给下游做旧写拒绝
mu.Held()                        // 本地视图（不保证服务端还没被抢）
mu.Depth()                       // 重入深度
left, _ := mu.RemainingTTL(ctx)  // redisstore：服务端剩余租期，巡检「锁快到期了」
sid := mu.SessionID()            // consulstore：本轮会话 ID
kv := mu.KVKey()                 // consulstore：实际 KV 键
```

后端侧直接看：

```bash
redis-cli HGETALL 'xm_lock:{sharding:ddl:orders_202611}'   # owner / depth / fence
redis-cli PTTL    'xm_lock:{sharding:ddl:orders_202611}'   # 剩余租期毫秒
consul kv get -recurse xm_lock/                            # 当前有哪些锁
consul session list -verbose                               # 会话名格式 xm_lock:<key>:<owner>
```

排障口诀：**「锁没释放」看 PTTL 是否在走、「谁拿着」看 owner 字段（含主机名+PID+goid，可直接定位实例与 goroutine）、「反复丢锁」对比 TTL 与任务耗时**。

### 拉一份运行快照

三端都实现了 `Stats()`，纯本地读、不发后端请求，在临界区内调同样安全：

```go
st := mu.Stats()
log.Println(st)                              // 一行摘要：attempts/busy/timeouts/lost/avg_wait…
if st.ContentionRatio() > 0.5 {
    log.Println("这把锁竞争很凶，考虑拆 Key 或改 TryLock 跳过")
}
if st.Lost > 0 {
    log.Println("出现过丢锁，临界区可能并发执行过，检查任务幂等")
}
```

口径两句话：**等到超时的回次也计等待时长，丢锁那一轮不计持有时长**——否则「被抢占」会被读成「正常持有」。计数自实例创建起累计，复位只能靠重建实例。

### 接进 Prometheus

```go
import (
    "github.com/xm-utils/sunmao/metrics"
    xmlock "github.com/xm-utils/sunmao/metrics/adapter/lock"
)

metrics.RegisterExporter(xmlock.New(ddlMu, recoverMu))   // 任意 lock.Statser，后端可混用
```

采集器每次抓取实时拉取一次 `Stats()`，输出 `xmt_lock_operations_total{op,result}`、`xmt_lock_errors_total`、`xmt_lock_duration_seconds_total{phase}`、`xmt_lock_contention_ratio`、`xmt_lock_state{field}` 五个指标族；告警示例 `rate(xmt_lock_operations_total{result="lost"}[5m]) > 0`（细节见 `metrics/QUICKSTART.md`）。

## 8. 想跑真实服务端的集成用例

日常 `go test ./...` 全跑在 `miniredis` / `fakeBackend` 上，不碰任何外部服务。要确认「真 Redis 的 `PEXPIRE`、真 Cluster 的 `{tag}` 同槽、真 Consul 会话回收」时，加构建标签跑一次（环境不可用会自动跳过，不判失败）：

```bash
cd lock
XM_LOCK_TEST_REDIS_ADDR=127.0.0.1:6379 \
XM_LOCK_TEST_CONSUL_ADDR=127.0.0.1:8500 \
  go test -tags integration -race -count=1 -timeout 180s ./...

# 只验 Cluster 承诺（需已起的多节点集群）
XM_LOCK_TEST_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001 \
  go test -tags integration -run TestIntegrationCluster ./redisstore
```

用例只在自建的 `xmlock_it:*` 键上动作并在 `Cleanup` 里回收（不 `FLUSHDB`、不删 Consul 以外的键）。完整变量表与逐条覆盖点见 `README.md` 的「集成用例」一节。

跑 consulstore 时建议给 agent 配 `session_ttl_min=1s`（它是配置文件项，不是 CLI flag），否则过期与续期两条要各自等 10s 以上：

```bash
echo '{"session_ttl_min": "1s"}' > /tmp/consul_it.json
docker run -d --name xmlock-consul-it -p 127.0.0.1:8500:8500 \
  -v /tmp/consul_it.json:/consul/config/it.json:ro \
  hashicorp/consul:1.22.7 consul agent -dev -client 0.0.0.0 -config-dir=/consul/config
```

实跑（Consul 1.22.7）实测到三条只能从服务端拿到的事实，已写进断言与文档：`KV Release` 不删键（锁键留作 `Session` 为空的墓碑）、`Session().Renew` 对已失效会话返回 `(nil, nil)` 而非 error（`consulstore` 据此判 `ErrLost`）、会话被 TTL 回收后还要等 `lock_delay`（默认 15s）别人才能接手。

## 常见错误

| 错误写法 | 后果 | 正确做法 |
|---|---|---|
| 多副本部署用 `lock.New`（Local） | 各副本都以为拿到了锁，重复执行不报错 | 跨实例一律 `redisstore` / `consulstore` |
| `TTL` 取得比任务耗时还短 | 任务跑到一半被抢占 → `ErrLost`，临界区并发 | `TTL ≥ 2 × 最坏耗时`，或关闭看门狗自行 `Refresh` |
| 忘了 `Unlock`（尤其错误分支） | 服务端要等满租期才恢复，期间全部副本跳过 | 直接用 `lock.Do(ctx, mu, fn)` 把配对收敛成一次调用；手写时 `defer func() { _ = mu.Unlock(ctx) }()` 紧跟获取成功之后 |
| A goroutine 加锁、B goroutine 解锁 | `ErrNotHeld`（默认 goroutine 绑定属主） | 同 goroutine 成对使用；确需跨 goroutine 收尾就统一用 `Close` 或设静态 `Owner` |
| 把 `ErrBusy` 当日志错误/告警 | 告警风暴，掩盖真问题 | `errors.Is(err, lock.ErrBusy)` 直接本轮跳过 |
| `err.Error() == "lock: busy"` | 消息格式变动即失效 | 一律 `errors.Is` 判哨兵 |
| 每次加锁都 `New` 一个实例 | 多轮会话/脚本建立开销，观测上会话乱跳 | 一个 Key 一个实例常驻，多 goroutine 并发复用 |
| 用 `Local` 时指望 `TTL` 自动过期 | `Local` 没有租期，持锁 goroutine 泄漏就永久阻塞 | 进程内也要保证解锁；跨进程需求换后端 |
| `WaitTimeout` 与 ctx 都不设，纯 `Lock` 死等 | 后端持续 busy 时 goroutine 长挂 | 设 `WaitTimeout`，并把 `ErrTimeout` 当作可容忍的降级分支 |
| `TTL` 设成亚毫秒 | 服务端只认毫秒，静默接受会得到一抢就过期的空锁 | `TTL >= 1ms`（构造期即报 `ErrInvalidConfig`），业务锁建议 5s 起 |
| consul 后端把 `WaitTimeout` 只设成 `TTL` | 持有者崩溃后会话回收要再等 `lock_delay`（默认 15s），等待方先报 `ErrTimeout` | 按「`TTL` + 15s」量级设 `WaitTimeout`，或定时任务直接用 `TryLock` 跳过本轮 |
| `OnRenewError` 里做阻塞 IO | 拖慢看门狗节拍，续期赶不上租期 | 只做投递（channel / 打点），重活交给外层 |
| 用 `lock.Do` 做 ticker 任务的「抢不到就跳过」 | `Do` 内部是 `Lock`，会排队而不是跳过 | 跳过语义自己写 `TryLock` + `errors.Is(err, lock.ErrBusy)`（见第 2 步）|

