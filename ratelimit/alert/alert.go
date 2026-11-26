// Package alert 提供 ratelimit → breaker 告警通道的联动桥:
// 周期性差分 Local 的判定计数快照,某 Key 在巡检间隔内的限流拒绝率持续超阈值时,
// 产出告警事件并投递到 breaker 的告警体系(breaker.AlertEvent / AlertCallback)。
//
// 设计约束:
//   - breaker 本体零改动——复用其 alert.go 的事件模型与告警出口,适配写在本包(调用方原则);
//   - 判定语义与 breaker.AlertManager 对齐:拒绝率阈值 + 最小样本量 + 冷却期防告警风暴;
//   - panic 安全:回调 panic 被 recover 并转为 OnPanic 通知,不击穿监控协程;
//   - 计数为累计值,本包按两次采样做差分得到"区间拒绝率",进程重启后计数自然归零重算。
package alert

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xm-utils/sunmao/breaker"
	"github.com/xm-utils/sunmao/ratelimit"
)

// Config 告警巡检配置
type Config struct {
	// Snapshots 快照提供方,通常传 local.Snapshots;返回 map 每次应为实时拉取
	Snapshots func() map[string]ratelimit.KeySnapshot
	// Threshold 区间拒绝率告警阈值(如 0.8 表示 80%),<=0 或 >1 时取默认 0.8
	Threshold float64
	// MinRequests 单 Key 区间内最小判定样本数,不足则不告警;<=0 时取默认 10
	MinRequests int64
	// Interval 巡检间隔,<=0 时取默认 10s(与 breaker.AlertManager 节奏一致)
	Interval time.Duration
	// Cooldown 同一 Key 两次告警的最小间隔,<=0 时取默认 5min(防告警风暴)
	Cooldown time.Duration
	// OnAlert 告警回调:事件可直接透传 breaker.AlertManager 的出口;
	// 回调 panic 由本包 recover 后转 OnPanic,不影响监控协程存活
	OnAlert func(*breaker.AlertEvent)
	// OnPanic 回调 panic 通知(nil 时静默丢弃)
	OnPanic func(any)
	// Clock 时间源(冷却期判定),nil 时使用 SystemClock
	Clock ratelimit.Clock
}

// Watcher 拒绝率告警巡检器:Check 供手动/测试触发,Start 启动后台周期巡检。
type Watcher struct {
	cfg Config

	mu      sync.Mutex
	prev    map[string]ratelimit.KeySnapshot // 上次采样的累计计数
	alerted map[string]time.Time             // 各 Key 最近一次告警时间
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// New 创建巡检器;Snapshots 为空返回 error,不 panic。
func New(cfg Config) (*Watcher, error) {
	if cfg.Snapshots == nil {
		return nil, fmt.Errorf("ratelimit/alert: Config.Snapshots 不能为空")
	}
	if cfg.Threshold <= 0 || cfg.Threshold > 1 {
		cfg.Threshold = 0.8
	}
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = 10
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	return &Watcher{
		cfg:     cfg,
		prev:    make(map[string]ratelimit.KeySnapshot),
		alerted: make(map[string]time.Time),
		stopCh:  make(chan struct{}),
	}, nil
}

// Check 执行一次巡检:拉取快照、与上次采样差分计算各 Key 区间拒绝率,
// 超阈值且过了冷却期的 Key 触发 OnAlert。返回本次触发的事件列表。
// 首次采样只记录基线不告警(无差分可用);快照源 panic 被隔离为本轮跳过。
// 并发调用安全,但通常由 Start 单协程驱动。
func (w *Watcher) Check() []*breaker.AlertEvent {
	snaps, ok := w.safeSnapshots()
	if !ok {
		return nil
	}
	now := w.cfg.Clock.Now()

	var fired []*breaker.AlertEvent
	w.mu.Lock()
	for key, s := range snaps {
		totalDelta, rejectedDelta := w.diffLocked(key, s)
		w.prev[key] = s
		if totalDelta < w.cfg.MinRequests {
			continue
		}
		rate := float64(rejectedDelta) / float64(totalDelta)
		if rate < w.cfg.Threshold {
			continue
		}
		if last, exists := w.alerted[key]; exists && now.Sub(last) < w.cfg.Cooldown {
			continue
		}
		w.alerted[key] = now
		fired = append(fired, w.newEvent(key, totalDelta, rejectedDelta, rate, now))
	}
	// 清理已消失 Key 的基线(进程内 Key 理论上只增不减,防御残留)
	for key := range w.prev {
		if _, exists := snaps[key]; !exists {
			delete(w.prev, key)
		}
	}
	w.mu.Unlock()

	// 回调在锁外执行,避免回调内再访问 Watcher/Local 时死锁
	for _, ev := range fired {
		w.invoke(ev)
	}
	return fired
}

// safeSnapshots 调用快照提供方并把 panic 转为本轮跳过(不击穿监控协程)。
func (w *Watcher) safeSnapshots() (snaps map[string]ratelimit.KeySnapshot, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			if w.cfg.OnPanic != nil {
				w.cfg.OnPanic(r)
			}
			snaps, ok = nil, false
		}
	}()
	return w.cfg.Snapshots(), true
}

// Start 启动后台周期巡检,立即先跑一次。返回的 error 仅在配置异常时出现
// (New 已校验,此处恒为 nil,保留 error 签名以对齐组件惯例)。
// 与 breaker.AlertManager.StartMonitoring 不同,本方法阻塞至 ctx 取消或 Close,
// 便于装配层纳入统一生命周期管理;需要后台运行时由调用方自行 go watcher.Start(ctx, nil)。
func (w *Watcher) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	w.wg.Add(1)
	defer w.wg.Done()

	w.Check()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.stopCh:
			return nil
		case <-ticker.C:
			w.Check()
		}
	}
}

// Close 停止后台巡检,幂等;等待已运行的 Start 协程退出。
func (w *Watcher) Close() error {
	w.mu.Lock()
	if !w.stopped {
		w.stopped = true
		close(w.stopCh)
	}
	w.mu.Unlock()
	w.wg.Wait()
	return nil
}

// diffLocked 计算指定 Key 的区间差分;计数回退(使用方重置等)按新基线处理,
// 本轮不告警。必须在持有 w.mu 时调用。
func (w *Watcher) diffLocked(key string, s ratelimit.KeySnapshot) (totalDelta, rejectedDelta int64) {
	p, exists := w.prev[key]
	if !exists {
		// 首次见到该 Key:以当前值为基线,本轮差分为 0
		return 0, 0
	}
	if s.Total < p.Total || s.Rejected < p.Rejected {
		return 0, 0
	}
	return s.Total - p.Total, s.Rejected - p.Rejected
}

// newEvent 产出 breaker 告警事件;区间差分计数直接入消息,避免与累计值混淆。
// CircuitName 加 "ratelimit:" 前缀,与同通道内熔断器告警区分来源。
func (w *Watcher) newEvent(key string, totalDelta, rejectedDelta int64, rate float64, now time.Time) *breaker.AlertEvent {
	return &breaker.AlertEvent{
		Timestamp:   now,
		Level:       breaker.AlertLevelError,
		CircuitName: "ratelimit:" + key,
		Message: fmt.Sprintf("限流拒绝率 %.2f%% 超过阈值 %.2f%%(区间判定 %d, 拒绝 %d)",
			rate*100, w.cfg.Threshold*100, totalDelta, rejectedDelta),
		State: breaker.StateClosed,
	}
}

// invoke 安全调用 OnAlert:回调 panic 转为 OnPanic,不击穿巡检协程。
func (w *Watcher) invoke(ev *breaker.AlertEvent) {
	if w.cfg.OnAlert == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if w.cfg.OnPanic != nil {
				w.cfg.OnPanic(r)
			}
		}
	}()
	w.cfg.OnAlert(ev)
}
