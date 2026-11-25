package alert

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xm-utils/sunmao/breaker"
	"github.com/xm-utils/sunmao/ratelimit"
)

// fakeClock 可手动推进的测试时钟
type fakeClock struct {
	mu sync.Mutex
	n  time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n = f.n.Add(d)
}

// stubProvider 固定快照源,测试中直接改 map
type stubProvider struct {
	mu    sync.Mutex
	snaps map[string]ratelimit.KeySnapshot
	calls int
}

func (s *stubProvider) Snapshots() map[string]ratelimit.KeySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	out := make(map[string]ratelimit.KeySnapshot, len(s.snaps))
	for k, v := range s.snaps {
		out[k] = v
	}
	return out
}

func (s *stubProvider) set(key string, total, rejected int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps[key] = ratelimit.KeySnapshot{Key: key, Total: total, Rejected: rejected, Limit: 100}
}

func (s *stubProvider) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps = map[string]ratelimit.KeySnapshot{}
}

func newWatcher(t *testing.T, cfg Config) *Watcher {
	t.Helper()
	if cfg.Snapshots == nil {
		t.Fatal("测试必须提供 Snapshots")
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestNewDefaults(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("Snapshots 为空应报错")
	}
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	w, err := New(Config{Snapshots: stub.Snapshots, Threshold: 3, MinRequests: -1, Interval: 0})
	if err != nil {
		t.Fatal(err)
	}
	if w.cfg.Threshold != 0.8 || w.cfg.MinRequests != 10 || w.cfg.Interval != 10*time.Second || w.cfg.Cooldown != 5*time.Minute {
		t.Fatalf("非法/零值配置应回落默认: %+v", w.cfg)
	}
}

// TestCheckFirstSampleNoAlert 首次采样只记基线,即使累计拒绝率很高也不告警
func TestCheckFirstSampleNoAlert(t *testing.T) {
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api:order", 100, 100)

	var fired []*breaker.AlertEvent
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.8,
		MinRequests: 10,
		Clock:       &fakeClock{n: time.Unix(1700000000, 0)},
		OnAlert:     func(ev *breaker.AlertEvent) { fired = append(fired, ev) },
	})
	if got := w.Check(); len(got) != 0 {
		t.Fatalf("首次采样不应告警, 得 %d 条", len(got))
	}
	if len(fired) != 0 {
		t.Fatalf("不应触发回调, 得 %d 条", len(fired))
	}

	// 第二个区间:新增 20 判定全被拒 → 区间拒绝率 100%,告警
	stub.set("api:order", 120, 120)
	if got := w.Check(); len(got) != 1 {
		t.Fatalf("期望告警 1 条, 得 %d", len(got))
	}
	ev := fired[0]
	if ev.Level != breaker.AlertLevelError {
		t.Errorf("级别期望 ERROR, 得 %v", ev.Level)
	}
	if ev.CircuitName != "ratelimit:api:order" {
		t.Errorf("CircuitName 期望带来源前缀, 得 %q", ev.CircuitName)
	}
	// 消息携带区间差分而非累计值
	if !strings.Contains(ev.Message, "20") || strings.Contains(ev.Message, "120") {
		t.Errorf("消息应含区间判定数 20 且不含累计 120: %q", ev.Message)
	}
}

// TestCheckBelowThresholdAndSample 拒绝率不足或样本量不足都不告警
func TestCheckBelowThresholdAndSample(t *testing.T) {
	clock := &fakeClock{n: time.Unix(1700000000, 0)}
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("low.rate", 10, 0)
	stub.set("low.sample", 0, 0)

	var mu sync.Mutex
	var count int
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.8,
		MinRequests: 10,
		Clock:       clock,
		OnAlert:     func(*breaker.AlertEvent) { mu.Lock(); count++; mu.Unlock() },
	})
	w.Check()

	// low.rate 新增 10 判定 0 拒绝;low.sample 仅新增 5 判定全拒(样本不足)
	stub.set("low.rate", 20, 0)
	stub.set("low.sample", 5, 5)
	if got := w.Check(); got != nil {
		t.Fatalf("阈值/样本量不足不应告警, 得 %d 条", len(got))
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 0 {
		t.Fatalf("不应触发回调, 得 %d 次", count)
	}
}

// TestCooldown 同 Key 冷却期内不重复告警,过期后可再次告警
func TestCooldown(t *testing.T) {
	clock := &fakeClock{n: time.Unix(1700000000, 0)}
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api", 0, 0)

	var fired int
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.5,
		MinRequests: 5,
		Cooldown:    time.Minute,
		Clock:       clock,
		OnAlert:     func(*breaker.AlertEvent) { fired++ },
	})
	w.Check() // 基线

	// 第 1 轮:全拒 → 告警
	stub.set("api", 10, 10)
	w.Check()
	if fired != 1 {
		t.Fatalf("首轮期望告警 1 次, 得 %d", fired)
	}
	// 第 2 轮:仍全拒,但冷却期内不重复
	clock.Advance(30 * time.Second)
	stub.set("api", 20, 20)
	w.Check()
	if fired != 1 {
		t.Fatalf("冷却期内不应重复告警, 得 %d", fired)
	}
	// 冷却期过后恢复告警
	clock.Advance(2 * time.Minute)
	stub.set("api", 30, 30)
	w.Check()
	if fired != 2 {
		t.Fatalf("冷却期过后应再次告警, 得 %d", fired)
	}
}

// TestCounterReset 使用方重启/重置导致计数回退时按新基线处理,不误报
func TestCounterReset(t *testing.T) {
	clock := &fakeClock{n: time.Unix(1700000000, 0)}
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api", 1000, 1000)

	var fired int
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.5,
		MinRequests: 5,
		Clock:       clock,
		OnAlert:     func(*breaker.AlertEvent) { fired++ },
	})
	w.Check()

	// 计数回退(视为新基线),本轮不告警
	stub.set("api", 3, 3)
	if got := w.Check(); len(got) != 0 {
		t.Fatalf("计数回退不应告警, 得 %d", len(got))
	}
	// 回退后的新基线上继续增长,正常判定
	stub.set("api", 13, 13)
	if got := w.Check(); len(got) != 1 {
		t.Fatalf("回退基线后应恢复正常告警, 得 %d", len(got))
	}
}

// TestKeyDisappeared 快照中消失的 Key 清理基线,再回来时重新记基线
func TestKeyDisappeared(t *testing.T) {
	clock := &fakeClock{n: time.Unix(1700000000, 0)}
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api", 0, 0)

	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.5,
		MinRequests: 5,
		Clock:       clock,
	})
	w.Check()
	if len(w.prev) != 1 {
		t.Fatalf("基线应含 1 个 Key, 得 %d", len(w.prev))
	}

	// Key 从快照消失
	stub.clear()
	w.Check()
	if len(w.prev) != 0 {
		t.Fatalf("消失 Key 应清理基线, 剩 %d", len(w.prev))
	}

	// 再回来:高累计值也只记基线,不告警
	stub.set("api", 100, 100)
	if got := w.Check(); len(got) != 0 {
		t.Fatalf("重新出现的 Key 首轮只记基线, 得 %d 条告警", len(got))
	}
}

// TestCallbackPanicIsolated 回调 panic 被 recover 转 OnPanic,巡检协程继续存活
func TestCallbackPanicIsolated(t *testing.T) {
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api", 0, 0)

	var panics int
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.5,
		MinRequests: 5,
		Clock:       &fakeClock{n: time.Unix(1700000000, 0)},
		OnAlert:     func(*breaker.AlertEvent) { panic("boom") },
		OnPanic:     func(any) { panics++ },
	})
	w.Check()
	stub.set("api", 10, 10)
	if got := w.Check(); len(got) != 1 {
		t.Fatalf("期望仍产出告警事件, 得 %d", len(got))
	}
	if panics != 1 {
		t.Fatalf("回调 panic 应转 OnPanic 1 次, 得 %d", panics)
	}

	// Start 协程未因回调 panic 退出:关闭上下文后可正常结束
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start 应正常退出: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start 未在 ctx 取消后退出")
	}
}

// TestProviderPanic 快照源 panic 被隔离:Check 跳过本轮,监控协程不崩溃
func TestProviderPanic(t *testing.T) {
	var panics int
	w := newWatcher(t, Config{
		Snapshots: func() map[string]ratelimit.KeySnapshot {
			panic(errors.New("provider down"))
		},
		Clock:   &fakeClock{n: time.Unix(1700000000, 0)},
		OnPanic: func(any) { panics++ },
	})
	if got := w.Check(); got != nil {
		t.Fatalf("快照源 panic 轮应跳过, 得 %d 条告警", len(got))
	}
	if panics != 1 {
		t.Fatalf("快照源 panic 应转 OnPanic 1 次, 得 %d", panics)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start 未退出")
	}
}

func TestCheckConcurrent(t *testing.T) {
	stub := &stubProvider{snaps: map[string]ratelimit.KeySnapshot{}}
	stub.set("api", 0, 0)
	w := newWatcher(t, Config{
		Snapshots:   stub.Snapshots,
		Threshold:   0.5,
		MinRequests: 5,
		Clock:       &fakeClock{n: time.Unix(1700000000, 0)},
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stub.set("api", int64(i*10+5), int64(i*10+5))
			w.Check()
		}()
	}
	wg.Wait()
}
