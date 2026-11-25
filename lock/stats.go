package lock

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"
)

// Stats 单把锁的运行快照,由后端 Stats() 实时读出(拉取式,不写任何外部存储)。
// 计数字段自实例创建起累计,复位只能靠重建实例;瞬时字段(Depth/Holding/Fence/Closed)
// 是读取那一刻的本地视图。
//
// 与哨兵错误一一对应:Busy↔ErrBusy、Timeouts↔ErrTimeout、Lost↔ErrLost、
// Canceled 是 ctx 自身取消/超时(与 Timeouts 区分,见 README 错误语义表),
// Errors 收后端故障与生命周期误用(ErrClosed、ErrInvalidConfig、未知返回码)。
type Stats struct {
	// Key 锁标识;Backend 后端名(local/redis/consul);TTL 生效租期(已规范化)
	Key     string        `json:"key"`
	Backend string        `json:"backend"`
	TTL     time.Duration `json:"ttl"`

	Attempts      uint64 `json:"attempts"`       // 向后端发起的获取尝试总数(轮询每轮各计一次)
	Acquired      uint64 `json:"acquired"`       // 首次获得(非重入)
	Reentered     uint64 `json:"reentered"`      // 同 Owner 重入命中
	Busy          uint64 `json:"busy"`           // 撞锁次数(竞争强度分子)
	Timeouts      uint64 `json:"timeouts"`       // 等到 WaitTimeout 仍未拿到
	Lost          uint64 `json:"lost"`           // 丢锁:租期到期或属主易主
	Canceled      uint64 `json:"canceled"`       // ctx 自身取消/超时导致的终止
	Errors        uint64 `json:"errors"`         // 后端故障与误用
	Released      uint64 `json:"released"`       // 深度归零的完整释放
	Decreased     uint64 `json:"decreased"`      // 重入出栈但未放锁
	ReleaseMisses uint64 `json:"release_misses"` // 释放被拒(未持有/非属主/已过期)
	Renewals      uint64 `json:"renewals"`       // 续期成功(含看门狗与重入顺带续期)
	RenewFailures uint64 `json:"renew_failures"` // 续期失败(其中丢锁已计入 Lost)
	WaitNanos     int64  `json:"wait_nanos"`     // 阻塞等待累计时长(含等到超时的回次)
	Waits         uint64 `json:"waits"`          // Lock 调用次数, 与 WaitNanos 配对推导平均等待
	HoldNanos     int64  `json:"hold_nanos"`     // 完整持有的临界区时长累计

	Depth   int    `json:"depth"`   // 当前重入深度(本地视图)
	Holding bool   `json:"holding"` // 本实例当前是否持有(深度 > 0)
	Fence   uint64 `json:"fence"`   // 本轮栅栏值,未持有为 0(consulstore 恒为 0)
	Closed  bool   `json:"closed"`  // 是否已 Close
}

// ContentionRatio 撞锁比例(0~1):Busy / Attempts。
// 无尝试时返回 0。它反映「想进临界区却排在别人后面」的频率,不等于失败率——
// 一次 Lock 可能轮询多次才成功,那属于正常串行化而非异常。
func (s Stats) ContentionRatio() float64 {
	if s.Attempts == 0 {
		return 0
	}
	busy := s.Busy
	if busy > s.Attempts {
		busy = s.Attempts
	}
	return float64(busy) / float64(s.Attempts)
}

// AvgWait 平均一次阻塞等待时长(按 Lock 调用次数取均, 等待最终超时的回次也算在内——
// 那部分时间确实花了)。未调用过 Lock 时返回 0;TryLock 不进入本指标。
func (s Stats) AvgWait() time.Duration {
	if s.Waits == 0 || s.WaitNanos <= 0 {
		return 0
	}
	return time.Duration(s.WaitNanos / int64(s.Waits))
}

// AvgHold 平均临界区时长;无完整释放时返回 0。
// 丢锁(Timeouts/Lost)不计入持有时长,以免把「被抢占」误读成「正常持有」。
func (s Stats) AvgHold() time.Duration {
	if s.Released == 0 || s.HoldNanos <= 0 {
		return 0
	}
	return time.Duration(s.HoldNanos / int64(s.Released))
}

// String 一行摘要,适合直接打进日志(键值对形式,免 JSON 依赖)。
func (s Stats) String() string {
	return "lock.Stats{key=" + s.Key + " backend=" + s.Backend +
		" attempts=" + strconv.FormatUint(s.Attempts, 10) +
		" acquired=" + strconv.FormatUint(s.Acquired, 10) +
		" busy=" + strconv.FormatUint(s.Busy, 10) +
		" timeouts=" + strconv.FormatUint(s.Timeouts, 10) +
		" lost=" + strconv.FormatUint(s.Lost, 10) +
		" errors=" + strconv.FormatUint(s.Errors, 10) +
		" renew_failures=" + strconv.FormatUint(s.RenewFailures, 10) +
		" depth=" + strconv.Itoa(s.Depth) +
		" holding=" + strconv.FormatBool(s.Holding) +
		" contention=" + strconv.FormatFloat(s.ContentionRatio(), 'f', 3, 64) +
		" avg_wait=" + s.AvgWait().String() +
		" avg_hold=" + s.AvgHold().String() + "}"
}

// Statser 可选接口:后端暴露自身运行快照。
// Local / redisstore.Mutex / consulstore.Mutex 都已实现;
// 自定义后端复用 Recorder 后实现它即可接入 metrics/adapter/lock。
type Statser interface {
	Stats() Stats
}

// Recorder 锁运行计数收集器:三后端内置,零值即可用。
//
// 全部方法只做原子自增、不加锁也不做 IO,因此在后端持有实例互斥锁的临界区内
// 调用同样安全(不会出现「Stats 读计数 → 取实例锁 → 与记账路径互等」这类死锁)。
// 记账点若漏在某条分支上只会让计数偏小,不影响锁语义。
type Recorder struct {
	attempts      atomic.Uint64
	acquired      atomic.Uint64
	reentered     atomic.Uint64
	busy          atomic.Uint64
	timeouts      atomic.Uint64
	lost          atomic.Uint64
	canceled      atomic.Uint64
	errs          atomic.Uint64
	released      atomic.Uint64
	decreased     atomic.Uint64
	releaseMisses atomic.Uint64
	renewals      atomic.Uint64
	renewFails    atomic.Uint64
	waits         atomic.Uint64
	waitNanos     atomic.Int64
	holdNanos     atomic.Int64

	holdStart atomic.Int64 // 本轮持有的起始时刻(unixnano),0 表示未在持有中
}

// NoteAttempt 记一次向后端发起的获取尝试(轮询的每轮各记一次)。
func (r *Recorder) NoteAttempt() { r.attempts.Add(1) }

// NoteAcquired 记一次获取成功。first=true 表示首次获得(就此开始计持有时长),
// false 表示同 Owner 重入(不重置计时,否则持有时长会被递归截短)。
func (r *Recorder) NoteAcquired(first bool) {
	if first {
		r.acquired.Add(1)
		r.holdStart.Store(time.Now().UnixNano())
		return
	}
	r.reentered.Add(1)
}

// NoteBusy 记一次撞锁(锁被他人持有)。
func (r *Recorder) NoteBusy() { r.busy.Add(1) }

// NoteWait 累加一次阻塞等待的时长。不论最终是否拿到锁都要记——等到超时的回次
// 同样消耗了调用方的时间,只记成功会低估竞争。
func (r *Recorder) NoteWait(d time.Duration) {
	r.waits.Add(1)
	if d > 0 {
		r.waitNanos.Add(int64(d))
	}
}

// NoteFailure 按哨兵错误归类一次失败。调用点只在 API 边界(Lock/TryLock/Unlock)记,
// 一次获取的轮询循环中只在终止那次记一回——轮询轮次靠 NoteAttempt/NoteBusy 体现。
// ErrBusy 在这里直接跳过:它已由 NoteBusy 单独计数,再记一次会把正常竞争算成错误。
func (r *Recorder) NoteFailure(err error) {
	switch {
	case err == nil, errors.Is(err, ErrBusy):
		return
	case errors.Is(err, ErrTimeout):
		r.timeouts.Add(1)
	case errors.Is(err, ErrLost):
		r.noteLost()
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		r.canceled.Add(1)
	default:
		r.errs.Add(1)
	}
}

// NoteReleased 记一次释放:full=true 表示深度归零、锁已归还(就此结算持有时长)。
func (r *Recorder) NoteReleased(full bool) {
	if !full {
		r.decreased.Add(1)
		return
	}
	r.released.Add(1)
	if start := r.holdStart.Swap(0); start > 0 {
		if d := time.Since(time.Unix(0, start)); d > 0 {
			r.holdNanos.Add(int64(d))
		}
	}
}

// NoteReleaseRejected 记一次被拒的释放(未持有/非属主/已过期);
// 其中属主易主同样计入 Lost,并作废持有时长。
func (r *Recorder) NoteReleaseRejected(err error) {
	r.releaseMisses.Add(1)
	if errors.Is(err, ErrLost) {
		r.noteLost()
		return
	}
	// 未持有/已过期同样意味着本轮没有正常持有到释放,清掉计时起点避免脏累加
	r.holdStart.Store(0)
}

// NoteRenew 记一次续期结果(err == nil 即成功)。
// 看门狗自动续期、手动 Refresh、重入时顺带续期都走这里,口径统一。
func (r *Recorder) NoteRenew(err error) {
	if err == nil {
		r.renewals.Add(1)
		return
	}
	r.renewFails.Add(1)
	if errors.Is(err, ErrLost) {
		r.noteLost()
		return
	}
	if errors.Is(err, ErrNotHeld) {
		// 本地已不持有:不该继续把这段算作持有
		r.holdStart.Store(0)
		return
	}
	// 后端故障不归类为丢锁:锁可能仍在手上,只是这次没续上
	r.errs.Add(1)
}

// noteLost 记一次丢锁并作废持有时长:被抢占的这段时间不能算「正常持有」。
func (r *Recorder) noteLost() {
	r.lost.Add(1)
	r.holdStart.Store(0)
}

// Snapshot 把累计值填入 base 并返回。base 由调用方填 Key/Backend/TTL 与瞬时字段;
// 计数字段一律以 Recorder 为准,调用方传进来的值会被覆盖。
func (r *Recorder) Snapshot(base Stats) Stats {
	base.Attempts = r.attempts.Load()
	base.Acquired = r.acquired.Load()
	base.Reentered = r.reentered.Load()
	base.Busy = r.busy.Load()
	base.Timeouts = r.timeouts.Load()
	base.Lost = r.lost.Load()
	base.Canceled = r.canceled.Load()
	base.Errors = r.errs.Load()
	base.Released = r.released.Load()
	base.Decreased = r.decreased.Load()
	base.ReleaseMisses = r.releaseMisses.Load()
	base.Renewals = r.renewals.Load()
	base.RenewFailures = r.renewFails.Load()
	base.Waits = r.waits.Load()
	base.WaitNanos = r.waitNanos.Load()
	base.HoldNanos = r.holdNanos.Load()
	return base
}
