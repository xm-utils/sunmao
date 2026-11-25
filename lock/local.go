package lock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Local 进程内互斥实现:零仓外依赖,用于单元测试与「同一进程内共享锁对象」的
// 并发防护(如同一处初始化的多 goroutine)。
//
// 互斥范围只限于同一实例:两个 Key 相同但分别构造的 Local 彼此看不见,不构成互斥——
// 需跨实例/跨进程互斥(多副本 deadletter 恢复、多实例 sharding 建表)必须用
// redisstore 或 consulstore,它们的属主与深度存于服务端。
//
// 与服务端后端的语义差异只有一处:Local 没有租期——进程内不存在「持有者进程崩溃后
// 无人释放」的问题(除非代码路径泄漏),所以 Config.TTL 与 RenewInterval 只参与配置
// 校验、不驱动任何行为;Config.RenewInterval 设 -1 亦无副作用。
//
// 等待策略:释放即唤醒(不轮询),同 Owner 重入加深度,跨 Owner 阻塞。
type Local struct {
	cfg Config
	rec Recorder // 运行计数,供 Stats() 拉取(无锁,可在持锁临界区内记账)

	mu     sync.Mutex
	heldBy string // 当前属主,空串表示空闲
	depth  int    // 重入深度
	fence  uint64 // 本次持有的栅栏值,空闲时为 0
	seq    uint64 // 栅栏计数,只在真正获得(非重入)时递增
	notify chan struct{}
	closed bool
}

var (
	_ Mutex   = (*Local)(nil)
	_ Fencer  = (*Local)(nil)
	_ Holder  = (*Local)(nil)
	_ Statser = (*Local)(nil)
)

// New 创建进程内锁;配置非法返回包装 ErrInvalidConfig 的 error,不 panic
func New(cfg Config) (*Local, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &Local{cfg: cfg, notify: make(chan struct{})}, nil
}

// Key 返回锁标识
func (l *Local) Key() string { return l.cfg.Key }

// Lock 阻塞获取,直到成功、ctx 取消/超时或 WaitTimeout 到点
func (l *Local) Lock(ctx context.Context) error {
	start := time.Now()
	err := WaitAcquireSignal(ctx, &l.cfg, l.attempt, l.wake)
	// 等待时长无条件累计:等到超时/被取消同样花了时间
	l.rec.NoteWait(time.Since(start))
	if err != nil {
		l.rec.NoteFailure(err)
		return err
	}
	return nil
}

// TryLock 非阻塞获取;被占用返回包装 ErrBusy 的 error
func (l *Local) TryLock(ctx context.Context) error {
	acquired, busy, err := callAttempt(l.attempt, ctx)
	if err != nil {
		l.rec.NoteFailure(err)
		return err
	}
	if acquired {
		return nil
	}
	if !busy {
		// 不可能分支:Local.attempt 只会返回已得或 busy(panic 已由 callAttempt 转 err)
		return l.rejectAttempt(fmt.Errorf("%w: key=%s 获取尝试返回异常状态", ErrInvalidConfig, l.cfg.Key))
	}
	return fmt.Errorf("%w: key=%s 正被持有", ErrBusy, l.cfg.Key)
}

// rejectAttempt 统一「异常返回」的记账与返回(计入 Errors,不计入撞锁)
func (l *Local) rejectAttempt(err error) error {
	l.rec.NoteFailure(err)
	return err
}

// Unlock 释放一层深度,减到 0 时真正释放并唤醒等待者。
// 本 goroutine/实例非属主时返回包装 ErrNotHeld 的 error。
// ctx 已取消仍执行释放:资源必须归还,不因取消而泄漏。
func (l *Local) Unlock(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	owner := l.cfg.ownerFor()
	switch {
	case l.closed:
		return l.rejectRelease(fmt.Errorf("%w: key=%s 已关闭且未持有", ErrNotHeld, l.cfg.Key))
	case l.heldBy == "":
		return l.rejectRelease(fmt.Errorf("%w: key=%s 未被持有", ErrNotHeld, l.cfg.Key))
	case l.heldBy != owner:
		return l.rejectRelease(fmt.Errorf("%w: key=%s 属主为 %q, 当前调用方 %q 无权释放",
			ErrNotHeld, l.cfg.Key, l.heldBy, owner))
	}

	l.depth--
	if l.depth > 0 {
		l.rec.NoteReleased(false)
		return nil // 重入未出栈,保持持有
	}
	l.heldBy = ""
	l.fence = 0
	l.rec.NoteReleased(true)
	l.broadcast()
	return nil
}

// rejectRelease 统一「释放被拒」的记账与返回
func (l *Local) rejectRelease(err error) error {
	l.rec.NoteReleaseRejected(err)
	return err
}

// Close 停止使用:仍持有则先释放(唤醒等待者),标记关闭。幂等,恒返回 nil。
// Close 后继续 Lock/TryLock 返回包装 ErrClosed 的 error。
func (l *Local) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	if l.heldBy != "" {
		l.heldBy = ""
		l.depth = 0
		l.fence = 0
		// 算一次完整释放:否则 Acquired 与 Released 会永远对不上账
		l.rec.NoteReleased(true)
		l.broadcast()
	}
	return nil
}

// Held 报告当前调用方是否为属主(配合默认 goroutine 绑定 Owner 使用时,
// 它反映的是本 goroutine 的持锁视图)
func (l *Local) Held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.closed && l.heldBy != "" && l.heldBy == l.cfg.ownerFor()
}

// Fence 返回本次持有的栅栏值,空闲时为 0。同 Owner 重入不改变栅栏值。
func (l *Local) Fence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.heldBy == "" {
		return 0
	}
	return l.fence
}

// Depth 返回当前重入深度(观测用,空闲时为 0)
func (l *Local) Depth() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.depth
}

// attempt 一次本地获取尝试:空闲即占用并推进栅栏,同 Owner 重入只加深度。
// 记账只记到这里能确定的事实(尝试轮次、成功/重入、撞锁),
// 失败归类留给 Lock/TryLock 边界,避免一次失败被计两回。
func (l *Local) attempt(ctx context.Context) (bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return false, false, err // ctx 已终止:本轮未真正打到锁上,不记账
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return false, false, fmt.Errorf("%w: key=%s", ErrClosed, l.cfg.Key)
	}
	owner := l.cfg.ownerFor()
	if owner == "" {
		return false, false, fmt.Errorf("%w: OwnerFunc 返回空串, 无法标识持有者", ErrInvalidConfig)
	}
	l.rec.NoteAttempt()
	switch {
	case l.heldBy == "":
		l.heldBy = owner
		l.depth = 1
		l.seq++
		l.fence = l.seq
		l.rec.NoteAcquired(true)
		return true, false, nil
	case l.heldBy == owner:
		l.depth++
		l.rec.NoteAcquired(false)
		return true, false, nil
	default:
		l.rec.NoteBusy()
		return false, true, nil
	}
}

// Stats 返回本实例的运行计数快照(实现 Statser)。
// Holding/Depth/Fence 是本实例视图:Local 无租期,因此它们只随 Unlock/Close 变化。
func (l *Local) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rec.Snapshot(Stats{
		Key:     l.cfg.Key,
		Backend: "local",
		TTL:     l.cfg.TTL,
		Depth:   l.depth,
		Holding: l.depth > 0,
		Fence:   l.fence,
		Closed:  l.closed,
	})
}

// wake 供等待循环取当前广播渠道;须在锁内读取,避免与 broadcast 竞态
func (l *Local) wake() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.notify
}

// broadcast 关闭旧渠道并换新:所有等待者下次 select 立即醒来重试。调用方须持锁。
// 注意 wake() 与 attempt 分别取锁,二者之间发生的 broadcast 只会让等待者多跑一次
// 空尝试,不会漏唤醒——等待者醒来后总会重新 attempt。
func (l *Local) broadcast() {
	close(l.notify)
	l.notify = make(chan struct{})
}
