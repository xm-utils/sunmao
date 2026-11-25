package lock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigNormalizeRequiresKey(t *testing.T) {
	var c Config
	err := c.Normalize()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("空 Key 应报 ErrInvalidConfig, 实际: %v", err)
	}
}

func TestConfigNormalizeDefaults(t *testing.T) {
	c := Config{Key: "job:x"}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if c.TTL != defaultTTL {
		t.Errorf("TTL 默认应为 %v, 实际 %v", defaultTTL, c.TTL)
	}
	if c.PollInterval != defaultPollInterval {
		t.Errorf("PollInterval 默认应为 %v, 实际 %v", defaultPollInterval, c.PollInterval)
	}
	if want := defaultTTL / renewDivisor; c.RenewInterval != want {
		t.Errorf("RenewInterval 默认应为 TTL/3=%v, 实际 %v", want, c.RenewInterval)
	}
	// 默认 goroutine 绑定:OwnerFunc 被注入且求值非空
	if c.OwnerFunc == nil {
		t.Fatal("默认应注入 OwnerFunc(goroutine 绑定重入)")
	}
	if owner := c.CurrentOwner(); owner == "" {
		t.Error("CurrentOwner 不应为空串")
	}
}

func TestConfigNormalizeRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"TTL 负值", Config{Key: "k", TTL: -time.Second}},
		{"WaitTimeout 负值", Config{Key: "k", WaitTimeout: -time.Second}},
		{"PollInterval 负值", Config{Key: "k", PollInterval: -time.Second}},
		{"PollInterval 过小", Config{Key: "k", PollInterval: time.Microsecond}},
		{"RenewInterval 非 -1 负值", Config{Key: "k", RenewInterval: -2 * time.Second}},
		{"RenewInterval 不小于 TTL", Config{Key: "k", TTL: time.Second, RenewInterval: 2 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cfg
			if err := c.Normalize(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("应报 ErrInvalidConfig, 实际: %v", err)
			}
		})
	}
}

func TestConfigNormalizeDisableRenew(t *testing.T) {
	c := Config{Key: "k", RenewInterval: -1}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if c.RenewInterval != 0 {
		t.Errorf("RenewInterval=-1 应归一为 0(不起看门狗), 实际 %v", c.RenewInterval)
	}
}

func TestConfigStaticOwnerKept(t *testing.T) {
	c := Config{Key: "k", Owner: "order-svc-3"}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if c.OwnerFunc != nil {
		t.Error("显式设置 Owner 时不应注入 OwnerFunc")
	}
	if got := c.CurrentOwner(); got != "order-svc-3" {
		t.Errorf("CurrentOwner=%q", got)
	}
	// OwnerFunc 优先级高于 Owner
	c2 := Config{Key: "k", Owner: "static", OwnerFunc: func() string { return "dynamic" }}
	if err := c2.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got := c2.CurrentOwner(); got != "dynamic" {
		t.Errorf("OwnerFunc 应优先, 实际 %q", got)
	}
}

func TestGoroutineOwner(t *testing.T) {
	self := GoroutineOwner()
	if !strings.Contains(self, procID) {
		t.Errorf("Owner 必须含进程前缀以避免跨进程 goid 串号, 实际 %q", self)
	}
	if again := GoroutineOwner(); again != self {
		t.Errorf("同 goroutine 应返回同一标识: %q != %q", again, self)
	}

	other := make(chan string, 1)
	go func() { other <- GoroutineOwner() }()
	if got := <-other; got == self {
		t.Errorf("跨 goroutine 标识应不同, 均为 %q", got)
	}
}

func TestWaitAcquireSucceedsAfterBusy(t *testing.T) {
	cfg := Config{Key: "k", PollInterval: time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	var tries atomic.Int64
	attempt := func(context.Context) (bool, bool, error) {
		if tries.Add(1) >= 3 {
			return true, false, nil
		}
		return false, true, nil
	}
	if err := WaitAcquire(context.Background(), &cfg, attempt); err != nil {
		t.Fatalf("应在第 3 次尝试成功, 实际 err=%v, tries=%d", err, tries.Load())
	}
	if got := tries.Load(); got != 3 {
		t.Errorf("尝试次数应为 3, 实际 %d", got)
	}
}

func TestWaitAcquireTimeout(t *testing.T) {
	cfg := Config{Key: "k", WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	start := time.Now()
	err := WaitAcquire(context.Background(), &cfg, func(context.Context) (bool, bool, error) {
		return false, true, nil
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("应报 ErrTimeout, 实际 %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("超时未及时返回, 耗时 %v", elapsed)
	}
}

func TestWaitAcquirePropagatesCtxCancel(t *testing.T) {
	cfg := Config{Key: "k", PollInterval: time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := WaitAcquire(ctx, &cfg, func(context.Context) (bool, bool, error) {
		return false, true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应原样透传 context.Canceled, 实际 %v", err)
	}
	if errors.Is(err, ErrTimeout) {
		t.Error("ctx 取消不应被报成 ErrTimeout——调用方需能区分两种失败")
	}
}

func TestWaitAcquireTimeoutDuringInFlightAttempt(t *testing.T) {
	// 尝试在飞行中被 WaitTimeout 的派生 ctx 取消:必须归为 ErrTimeout,
	// 不能把内部 ctx 的 DeadlineExceeded 泄给调用方(否则与真后端故障混淆)
	cfg := Config{Key: "k", WaitTimeout: 20 * time.Millisecond, PollInterval: time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	err := WaitAcquire(context.Background(), &cfg, func(ctx context.Context) (bool, bool, error) {
		<-ctx.Done()
		return false, false, fmt.Errorf("后端调用失败: %w", ctx.Err())
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("应报 ErrTimeout, 实际 %v", err)
	}

	// 父 ctx 自身超时则仍应透传父 ctx 错误, 不伪报为锁等待超时
	// (父截止时刻故意早于 WaitTimeout, 使两种错误不会同点发生)
	longCfg := Config{Key: "k", WaitTimeout: 500 * time.Millisecond, PollInterval: time.Millisecond}
	if err := longCfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	pctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = WaitAcquire(pctx, &longCfg, func(ctx context.Context) (bool, bool, error) {
		<-ctx.Done()
		return false, false, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("应透传 context.DeadlineExceeded, 实际 %v", err)
	}
	if errors.Is(err, ErrTimeout) {
		t.Error("父 ctx 超时不应被报成 ErrTimeout")
	}
}

func TestWaitAcquireAttemptPanicBecomesError(t *testing.T) {
	cfg := Config{Key: "k", WaitTimeout: 10 * time.Millisecond, PollInterval: time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	err := WaitAcquire(context.Background(), &cfg, func(context.Context) (bool, bool, error) {
		panic("后端炸了")
	})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("attempt panic 应转为 error, 实际 %v", err)
	}
	if errors.Is(err, ErrTimeout) {
		t.Error("panic 不应被吞成超时错误")
	}
}

func TestWaitAcquireAnomalyResult(t *testing.T) {
	cfg := Config{Key: "k"}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	// (false, false, nil) 属后端实现缺陷, 必须报错而不是死循环
	err := WaitAcquire(context.Background(), &cfg, func(context.Context) (bool, bool, error) {
		return false, false, nil
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("应报 ErrInvalidConfig, 实际 %v", err)
	}
	if err := WaitAcquire(context.Background(), &cfg, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil attempt 应报 ErrInvalidConfig, 实际 %v", err)
	}
}

func TestWaitAcquireWakeSignal(t *testing.T) {
	// 轮询间隔设成 1 小时:若唤醒信号不生效, 本用例必然超时
	cfg := Config{Key: "k", WaitTimeout: 2 * time.Second, PollInterval: time.Hour}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	var holds atomic.Int64
	wake := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(wake)
	}()
	err := WaitAcquireSignal(context.Background(), &cfg,
		func(context.Context) (bool, bool, error) {
			if holds.Add(1) >= 2 {
				return true, false, nil
			}
			return false, true, nil
		},
		func() <-chan struct{} { return wake },
	)
	if err != nil {
		t.Fatalf("唤醒信号应让等待者立即重试并成功, 实际 err=%v", err)
	}
	if got := holds.Load(); got != 2 {
		t.Errorf("应恰好尝试 2 次, 实际 %d", got)
	}
}

func TestWatchdogTickSemantics(t *testing.T) {
	cases := []struct {
		name     string
		renewErr error
		wantStop bool
	}{
		{"续期成功", nil, false},
		{"后端抖动继续重试", errors.New("connection reset"), false},
		{"属主丢失停止", fmt.Errorf("%w: 被抢占", ErrLost), true},
		{"未持有停止", fmt.Errorf("%w: 已释放", ErrNotHeld), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Key: "k"}
			if err := cfg.Normalize(); err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			var reported atomic.Value
			cfg.OnRenewError = func(err error) { reported.Store(err) }

			wd, err := NewWatchdog(&cfg, func(context.Context) error { return tc.renewErr })
			if err != nil {
				t.Fatalf("NewWatchdog: %v", err)
			}
			if got := wd.Tick(context.Background()); got != tc.wantStop {
				t.Errorf("Tick 停止标志=%v, 期望 %v", got, tc.wantStop)
			}
			if tc.renewErr != nil {
				v := reported.Load()
				if v == nil {
					t.Fatal("OnRenewError 应收到续期错误")
				}
				if got, ok := v.(error); !ok || !errors.Is(got, tc.renewErr) {
					t.Errorf("OnRenewError 收到 %v, 期望包含 %v", got, tc.renewErr)
				}
			}
		})
	}
}

func TestWatchdogRenewPanicIsolated(t *testing.T) {
	cfg := Config{Key: "k", OnRenewError: func(error) { panic("回调也炸") }}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	wd, err := NewWatchdog(&cfg, func(context.Context) error { panic("续期 panic") })
	if err != nil {
		t.Fatalf("NewWatchdog: %v", err)
	}
	// renew panic 与 onError 回调 panic 都必须被隔离, Tick 不能把 panic 抛给持有者
	done := make(chan bool, 1)
	go func() { done <- wd.Tick(context.Background()) }()
	select {
	case stop := <-done:
		if stop {
			t.Error("续期 panic 不属 ErrLost, 不应停止续期")
		}
	case <-time.After(time.Second):
		t.Fatal("Tick 卡住(疑为 panic 逃逸)")
	}
}

func TestWatchdogStartStop(t *testing.T) {
	cfg := Config{Key: "k", TTL: 300 * time.Millisecond, RenewInterval: 20 * time.Millisecond}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	var ticks atomic.Int64
	wd, err := NewWatchdog(&cfg, func(context.Context) error {
		ticks.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("NewWatchdog: %v", err)
	}

	wd.Start()
	wd.Start() // 幂等:不应起第二个循环
	time.Sleep(70 * time.Millisecond)
	wd.Stop()
	wd.Stop() // 幂等
	after := ticks.Load()
	if after == 0 {
		t.Fatal("看门狗未执行任何续期")
	}
	time.Sleep(60 * time.Millisecond)
	if got := ticks.Load(); got != after {
		t.Errorf("Stop 后仍在续期: %d -> %d", after, got)
	}
}

func TestWatchdogDisabledIsNoop(t *testing.T) {
	cfg := Config{Key: "k", RenewInterval: -1}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	var ticks atomic.Int64
	wd, err := NewWatchdog(&cfg, func(context.Context) error {
		ticks.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("NewWatchdog: %v", err)
	}
	wd.Start()
	time.Sleep(30 * time.Millisecond)
	wd.Stop()
	if got := ticks.Load(); got != 0 {
		t.Errorf("RenewInterval=-1 时不应自动续期, 实际 %d 次", got)
	}
	// 手动 Tick 仍可用(长任务自带调度器的场景)
	if wd.Tick(context.Background()) {
		t.Error("Tick 返回值应为 false")
	}
	if got := ticks.Load(); got != 1 {
		t.Errorf("手动 Tick 应执行续期, 实际 %d 次", got)
	}
}

func TestNewWatchdogRejectsNilRenew(t *testing.T) {
	cfg := Config{Key: "k"}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if _, err := NewWatchdog(&cfg, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil renew 应报 ErrInvalidConfig, 实际 %v", err)
	}
}

// TestSentinelWrapping 校验各后端都会用到的错误链形态:哨兵必须能经多层包装被 errors.Is 命中
func TestSentinelWrapping(t *testing.T) {
	chain := fmt.Errorf("lock/redisstore: 上层描述: %w: %w", ErrLost, ErrNotHeld)
	if !errors.Is(chain, ErrLost) || !errors.Is(chain, ErrNotHeld) {
		t.Fatalf("多哨兵包装应同时可判定, 实际 ErrLost=%v ErrNotHeld=%v",
			errors.Is(chain, ErrLost), errors.Is(chain, ErrNotHeld))
	}
}
