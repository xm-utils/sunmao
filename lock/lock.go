// Package lock 提供跨实例互斥的统一契约与多后端实现。
//
// 立项动因是组件内已出现、但至今无原语支撑的多实例竞争场景:
//
//   - gorm/database/sharding 启动期自动建表——多实例并发 DDL 互相阻塞甚至报 1091/1061;
//   - deadletter 定时恢复——多副本各跑一轮 ticker,同一批死信被重复捞取重投;
//   - mq/redis stream pending 巡检——启用后即面临重复告警。
//
// 契约只有一个接口 Mutex(Key 维度一实例),换后端换构造器、业务代码不动一行:
//
//	redisstore.New(redisstore.Config{Client: rc, Config: lock.Config{Key: "deadletter:recover"}})
//	consulstore.New(consulstore.Config{Backend: consulClient, Config: lock.Config{Key: "sharding:ddl:orders"}})
//	lock.New(lock.Config{Key: "job:report"})               // 进程内, 测试与单机部署
//
// 租期(TTL)与看门狗续期、try-lock、阻塞获取、Context 取消感知、同 Owner 可重入
// 在本包以 Config 统一表达,各后端语义一致;差异写在各包 README 的「后端差异」表。
//
// 需要「加锁-执行-解锁」配对时用 Do(ctx, mu, fn)——它保证错误分支与 panic
// 都不会把锁漏在半路;周期任务「抢不到就跳过」仍应自己写 TryLock。
//
// 本包只依赖标准库:进程内实现与契约零仓外依赖,Redis/Consul 后端在子包按需引入。
package lock

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

// 哨兵错误:各后端共用,error 链一律 %w 包装,调用方按 errors.Is 分级处理。
var (
	// ErrBusy 锁已被他人持有(TryLock 立即失败时用)
	ErrBusy = errors.New("lock: busy")
	// ErrTimeout 在 WaitTimeout 或 ctx 期限内未获得锁(Lock 等待失败时用)
	ErrTimeout = errors.New("lock: acquire timeout")
	// ErrNotHeld 当前实例并未持有该锁(释放/续期未持有的锁时用)
	ErrNotHeld = errors.New("lock: not held")
	// ErrLost 持锁期间租期到期被抢占,或续期时发现属主已易主。
	// 出现该错误说明临界区可能已被并发执行,调用方应放弃本轮任务而非重试补偿。
	ErrLost = errors.New("lock: ownership lost")
	// ErrClosed 锁实例已关闭( Close 后继续使用)
	ErrClosed = errors.New("lock: closed")
	// ErrInvalidConfig 配置非法,构造期返回,不 panic
	ErrInvalidConfig = errors.New("lock: invalid config")
)

// Mutex 是分布式互斥锁的统一契约。实例与一个 Key 一一绑定,可被多个 goroutine
// 并发使用;Lock/TryLock 成功即进入临界区,必须成对 Unlock。
//
// 三个方法都不 panic:内部异常一律 recover 后包装为 error 返回。
type Mutex interface {
	// Lock 阻塞获取锁,直到成功、被 ctx 取消/超时、达到 WaitTimeout 或后端故障。
	// 等待失败返回包装 ErrTimeout 的 error;同 Owner 重入时直接加深计数返回 nil。
	Lock(ctx context.Context) error

	// TryLock 非阻塞获取:未被占用返回 nil,被占用返回包装 ErrBusy 的 error。
	// 适合「有副本在做就本轮跳过」的定时任务。
	TryLock(ctx context.Context) error

	// Unlock 释放锁:可重入时只减一层深度,减到 0 才真正释放服务端状态。
	// 本实例(或本 goroutine,取决于 Owner 策略)未持锁时返回包装 ErrNotHeld 的 error——
	// 跨持有者释放被拒绝,避免把别人的临界区提前打开。
	Unlock(ctx context.Context) error

	// Close 停止看门狗并释放本实例资源;仍持有时先 Unlock 再关闭。
	// 无论释放成败,本地资源复位与看门狗停止都会完成;返回释放过程中的错误
	// (无远端 IO 的 Local 恒为 nil),可重复调用。关闭后继续使用报 ErrClosed。
	Close() error
}

// Fencer 是可选能力:返回本次持锁的单调递增栅栏值(fencing token)。
// 下游存储可用它拒绝旧持有者的写入。仅 redisstore 与 Local 实现。
type Fencer interface {
	// Fence 返回最近一次成功获取的栅栏值;未持锁或后端不支持时为 0。
	Fence() uint64
}

// Holder 是可选能力:查询本实例当前是否处于持锁状态(含重入深度 > 0)。
type Holder interface {
	// Held 报告本实例是否持有锁。注意它只反映本地视图,
	// 服务端租期是否已被抢占以下一次操作的结果为准。
	Held() bool
}

// Refresher 是可选能力:手动延长本次持有的租期。服务端后端(redisstore/consulstore)
// 实现它;无租期概念的进程内 Local 不实现(它没有会过期的持有权)。
type Refresher interface {
	// Refresh 立即续期一次。默认看门狗已按 TTL/3 自动续期,无需调用;
	// 关闭看门狗(Config.RenewInterval = -1)后,长任务必须自行周期调用它,
	// 否则租期到点会被其他实例抢占。未持有时返回包装 ErrNotHeld 的 error,
	// 属主已失(租期过期/会话回收)时报 ErrLost。
	Refresh(ctx context.Context) error
}

// 不提供时间源抽象:锁的租期由服务端计时(Redis PEXPIRE / Consul Session TTL),
// 本地只负责等待与续期调度;测试的确定性入口是 Watchdog.Tick。

// 默认值取 30s 租期,对 DDL/恢复类任务足够宽,
// 又能在持有者崩溃时于半分钟内恢复可用。
const (
	defaultTTL          = 30 * time.Second
	defaultPollInterval = 100 * time.Millisecond
	minPollInterval     = time.Millisecond
	// minTTL 租期下限:Redis 侧 PEXPIRE 只认毫秒,亚毫秒 TTL 会被截成 0,
	// 脚本会「获得锁」后立即把 Key 过期掉——互斥形同空操作,必须在构造期拒掉
	minTTL = time.Millisecond
	// renewDivisor 看门狗默认续期间隔取 TTL/renewDivisor,留出 2 次续期失败的容错余量
	renewDivisor = 3
)

// Config 锁行为配置,各后端以内嵌方式复用同一份语义(yaml 键位见 config.example.yaml)。
// 零值可用:TTL 等取默认值;非法值报 ErrInvalidConfig,不做静默纠正。
type Config struct {
	// Key 锁标识,如 "sharding:ddl:orders";实际存储键由各后端加前缀(xm_lock:...)
	Key string
	// TTL 租期:持有者进程崩溃后最迟经过该时长被他人抢占。<=0 取默认 30s;
	// 下限 1ms(服务端毫秒计时),更短直接判非法而非静默截断
	TTL time.Duration
	// WaitTimeout Lock 的最长等待时间;0 表示不限(仅受 ctx 约束)。
	// 超时返回包装 ErrTimeout 的 error
	WaitTimeout time.Duration
	// PollInterval 竞争时的重试间隔,默认 100ms,内部加 ±25% 抖动避免同步风暴
	PollInterval time.Duration
	// RenewInterval 看门狗续期间隔;0 取 TTL/3;<0(即 -1)关闭自动续期
	// (关闭后长任务必须自行周期调用 Refresher.Refresh)
	RenewInterval time.Duration
	// Owner 持有者标识:同 Owner 的多个实例之间可重入(计数叠加),不同 Owner 互斥。
	// 置为静态串(如 "order-svc-3")即按「同 Owner 即同持有者」处理,
	// 不再做 goroutine 隔离——同进程内不同 goroutine 共用该实例会互相重入,
	// 需跨实例共享重入语义时才建议显式设置
	Owner string
	// OwnerFunc 每次获取时求值 Owner,优先级高于 Owner。
	// 默认(Owner 与 OwnerFunc 均为空)取 GoroutineOwner,自带
	// 「可重入(goroutine 绑定)」:同一 goroutine 递归加锁加深深度,
	// 跨 goroutine 仍需等待;如需关闭重入语义,设为返回固定随机串的函数即可
	OwnerFunc func() string
	// OnRenewError 看门狗续期失败回调(异步 goroutine 内调用,不可阻塞)。
	// 续期失败导致属主丢失时,错误同时包装 ErrLost;nil 则静默(仅 Held 变 false)
	OnRenewError func(error)
}

// normalize 校验并填默认值,返回包装 ErrInvalidConfig 的 error。
// 允许就地修改(调用方传入的多为值拷贝),故接收指针。
func (c *Config) normalize() error {
	if c.Key == "" {
		return fmt.Errorf("%w: Key 不能为空", ErrInvalidConfig)
	}
	if c.TTL < 0 {
		return fmt.Errorf("%w: TTL 不能为负, 当前 %v", ErrInvalidConfig, c.TTL)
	}
	if c.TTL == 0 {
		c.TTL = defaultTTL
	}
	if c.TTL < minTTL {
		return fmt.Errorf("%w: TTL 不能小于 %v(服务端以毫秒计时, 亚毫秒租期会让加锁成为空操作), 当前 %v",
			ErrInvalidConfig, minTTL, c.TTL)
	}
	if c.WaitTimeout < 0 {
		return fmt.Errorf("%w: WaitTimeout 不能为负, 当前 %v", ErrInvalidConfig, c.WaitTimeout)
	}
	if c.PollInterval < 0 {
		return fmt.Errorf("%w: PollInterval 不能为负, 当前 %v", ErrInvalidConfig, c.PollInterval)
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.PollInterval < minPollInterval {
		return fmt.Errorf("%w: PollInterval 不能小于 %v, 当前 %v", ErrInvalidConfig, minPollInterval, c.PollInterval)
	}
	// RenewInterval 允许负数以显式关闭看门狗,0 走 TTL/3 默认
	if c.RenewInterval < 0 && c.RenewInterval != -1 {
		return fmt.Errorf("%w: RenewInterval 只允许 0(默认)或 -1(关闭), 当前 %v", ErrInvalidConfig, c.RenewInterval)
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = c.TTL / renewDivisor
	}
	if c.RenewInterval < 0 {
		c.RenewInterval = 0 // 关闭自动续期,内部以 0 表示不起 goroutine
	}
	if c.RenewInterval >= c.TTL {
		return fmt.Errorf("%w: RenewInterval(%v) 必须小于 TTL(%v), 否则租期会先于续期到期",
			ErrInvalidConfig, c.RenewInterval, c.TTL)
	}
	if c.Owner == "" && c.OwnerFunc == nil {
		// 默认 goroutine 绑定:同 goroutine 可重入、跨 goroutine 互斥
		c.OwnerFunc = GoroutineOwner
	}
	return nil
}

// Normalize 校验并填默认值,返回包装 ErrInvalidConfig 的 error。
// 各后端子包(redisstore/consulstore)的 Config 内嵌 lock.Config 后,构造时必须调用一次,
// 以保证「换后端不换默认值」(TTL 30s、轮询 100ms 带抖动、续期 TTL/3、goroutine 绑定重入)。
func (c *Config) Normalize() error { return c.normalize() }

// CurrentOwner 返回本次操作应使用的持有者标识:OwnerFunc 优先(每调用求值一次)。
// 必须在 Normalize 之后调用,否则默认 OwnerFunc 尚未注入会返回空串。
func (c *Config) CurrentOwner() string { return c.ownerFor() }

// ownerFor 取本次操作的持有者标识:OwnerFunc 优先(每调用求值一次)。
// 返回空串视为调用方提供的 Owner 有误,由上层报错而非猜默认值。
func (c *Config) ownerFor() string {
	if c.OwnerFunc != nil {
		return c.OwnerFunc()
	}
	return c.Owner
}

// jitterInterval 给轮询间隔加 ±25% 抖动:多副本同点重试会放大后端压力
func (c *Config) jitterInterval() time.Duration {
	base := c.PollInterval
	if base <= 0 {
		return 0
	}
	delta := float64(base) * 0.25
	return time.Duration(float64(base) + (rand.Float64()*2-1)*delta)
}

// deadline 计算 Lock 等待的截止时刻:WaitTimeout 与 ctx 取更早者。
// 返回的 cancel 必须 defer 调用以释放 timer。
func (c *Config) deadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.WaitTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.WaitTimeout)
}

var (
	defaultOwnerSeq atomic.Uint64
	// procID 进程级唯一前缀: <hostname>-<pid>-<8hex>。
	// goroutine id 仅在本进程内唯一, 不拼上前缀会导致多进程间 g1234 串号、误判重入。
	procID = initProcID()
)

// initProcID 组装进程标识;crypto/rand 失败(极少数)时退化为无随机后缀,
// 仍有 hostname+pid 保证同一机器上不碰撞。
func initProcID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [4]byte
	if _, randErr := cryptorand.Read(b[:]); randErr == nil {
		return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), b)
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// defaultOwner 生成实例级兜底标识(goid 提取失败或需进程内区分实例时使用):
// <procID>#<序号>。
func defaultOwner() string {
	return fmt.Sprintf("%s#%d", procID, defaultOwnerSeq.Add(1))
}
