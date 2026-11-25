package lock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Attempt 一次原子获取尝试,由后端提供:
// acquired=true 表示本次已获得(或重入成功);
// acquired=false 且 busy=true 表示锁被他人持有,应等待重试;
// 两者皆 false 且 err=nil 属实现缺陷,由 WaitAcquire 报错而非死循环。
type Attempt func(ctx context.Context) (acquired, busy bool, err error)

// WaitAcquire 阻塞获取的通用轮询循环(无额外唤醒信号),供各后端复用——
// 轮询间隔带抖动、ctx 取消与 WaitTimeout 双截止、panic 转 error 全部在此收敛,
// 保证「换后端不换等待语义」。
//
// 返回:获得锁为 nil;等待超时报 ErrTimeout(或原样透传 ctx 的取消/超时错误);
// 后端故障与配置问题透传 attempt 的 error。
func WaitAcquire(ctx context.Context, cfg *Config, attempt Attempt) error {
	return WaitAcquireSignal(ctx, cfg, attempt, nil)
}

// WaitAcquireSignal 在轮询之外多一个唤醒渠道:wake 每次循环求值一次,
// 返回的 channel 被关闭则立即重试(不必等满一个间隔)。
// 进程内 Local 用它做到释放即唤醒;服务端后端因多进程无共享内存,
// 传 nil 保持纯轮询。wake 为 nil 与返回 nil channel 均退化为轮询。
func WaitAcquireSignal(ctx context.Context, cfg *Config, attempt Attempt, wake func() <-chan struct{}) error {
	if attempt == nil {
		return fmt.Errorf("%w: attempt 不能为 nil", ErrInvalidConfig)
	}
	waitCtx, cancel := cfg.deadline(ctx)
	defer cancel()

	for {
		acquired, busy, err := callAttempt(attempt, waitCtx)
		if err != nil {
			// 截止时刻已到说明错误是「等待期限」自身引发的(尝试在飞行中被 waitCtx 取消),
			// 按契约报 ErrTimeout / 透传父 ctx,不能把内部 ctx 的错误泄给调用方
			if werr := waitInterrupted(ctx, waitCtx, cfg); werr != nil {
				return werr
			}
			return err
		}
		if acquired {
			return nil
		}
		if !busy {
			return fmt.Errorf("%w: 获取尝试返回 (false, false, nil), 后端实现异常", ErrInvalidConfig)
		}
		// 已被占用:先看截止,再等一个抖动间隔或唤醒信号
		if err := waitInterrupted(ctx, waitCtx, cfg); err != nil {
			return err
		}

		var ch <-chan struct{}
		if wake != nil {
			ch = wake()
		}
		timer := time.NewTimer(cfg.jitterInterval())
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
		case <-waitCtx.Done():
			timer.Stop()
			return waitInterrupted(ctx, waitCtx, cfg)
		}
	}
}

// waitInterrupted 把等待终止的原因翻译成契约错误:
// 父 ctx 自身取消/超时优先原样返回(调用方能 errors.Is(err, context.Canceled) 判定);
// 仅 WaitTimeout 到点则报 ErrTimeout。
func waitInterrupted(parent, waitCtx context.Context, cfg *Config) error {
	if parent != nil && parent.Err() != nil {
		return parent.Err()
	}
	if waitCtx.Err() != nil {
		return fmt.Errorf("%w: 等待 %v 未获得锁", ErrTimeout, cfg.WaitTimeout)
	}
	return nil
}

// callAttempt 隔离后端尝试里的 panic,转为 error(仓库设计约定:对外不抛 panic)
func callAttempt(attempt Attempt, ctx context.Context) (acquired, busy bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			acquired, busy = false, false
			err = fmt.Errorf("lock: 获取尝试 panic: %v", r)
		}
	}()
	return attempt(ctx)
}

// Watchdog 租期续期看门狗:周期执行续期回调,续期间隔由 Config.RenewInterval 决定。
// redisstore/consulstore 在首次成功获取时 Start、彻底释放或 Close 时 Stop,
// 两端语义一致。RenewInterval 为 0(即配置 -1 关闭)时 Start/Stop 均为空操作。
//
// 续期回调返回的 error 若包装 ErrLost/ErrNotHeld(属主已失),看门狗停止后续续期,
// 避免在锁已易主时空转;其余错误只上报、继续重试。
type Watchdog struct {
	interval time.Duration
	renew    func(context.Context) error
	onError  func(error)

	mu      sync.Mutex
	running bool
	stop    chan struct{}
	done    chan struct{}
}

// NewWatchdog 按配置构造看门狗;cfg 须已 normalize(RenewInterval 已定形)。
// renew 不可为 nil;错误上报走 cfg.OnRenewError(nil 则静默)。
func NewWatchdog(cfg *Config, renew func(context.Context) error) (*Watchdog, error) {
	if renew == nil {
		return nil, fmt.Errorf("%w: renew 不能为 nil", ErrInvalidConfig)
	}
	return &Watchdog{
		interval: cfg.RenewInterval,
		renew:    renew,
		onError:  cfg.OnRenewError,
	}, nil
}

// Start 启动续期循环(幂等:已在跑则直接返回)
func (w *Watchdog) Start() {
	if w.interval <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return
	}
	w.stop = make(chan struct{})
	w.done = make(chan struct{})
	w.running = true
	stop, done := w.stop, w.done

	go func() {
		defer close(done)
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if stopRenew := w.Tick(context.Background()); stopRenew {
					return
				}
			case <-stop:
				return
			}
		}
	}()
}

// Stop 停止续期循环并等待 goroutine 退出(幂等)
func (w *Watchdog) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	stop, done := w.stop, w.done
	w.mu.Unlock()

	close(stop)
	<-done
}

// Tick 立即执行一次续期,返回 true 表示属主已失、循环应当停止。
// 导出以便测试与「手动驱动」场景(如自带调度器的长任务)复用同一套判定语义。
func (w *Watchdog) Tick(ctx context.Context) (stopRenew bool) {
	err := w.renewOnce(ctx)
	if err == nil {
		return false
	}
	if w.onError != nil {
		w.invokeOnError(err)
	}
	return errors.Is(err, ErrLost) || errors.Is(err, ErrNotHeld)
}

// renewOnce 执行续期回调并隔离 panic
func (w *Watchdog) renewOnce(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("lock: 续期 panic: %v", r)
		}
	}()
	return w.renew(ctx)
}

// invokeOnError 隔离回调 panic:续期观测链路不能反过来打挂持有者
func (w *Watchdog) invokeOnError(err error) {
	defer func() {
		_ = recover()
	}()
	w.onError(err)
}
