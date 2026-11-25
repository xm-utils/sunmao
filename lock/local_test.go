package lock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestLocal(t *testing.T, cfg Config) *Local {
	t.Helper()
	if cfg.Key == "" {
		cfg.Key = "test:local"
	}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// callFromOther 在独立 goroutine 中执行 fn 并回传其 error——
// 默认 Owner 为 goroutine 绑定,跨 goroutine 调用才能验真互斥/跨持有者语义。
func callFromOther(fn func() error) error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return <-ch
}

func TestLocalTryLockAndMutualExclusion(t *testing.T) {
	l := newTestLocal(t, Config{})
	ctx := context.Background()

	if err := l.TryLock(ctx); err != nil {
		t.Fatalf("首次 TryLock 应成功: %v", err)
	}
	if !l.Held() {
		t.Error("Held 应为 true")
	}
	if l.Depth() != 1 {
		t.Errorf("深度应为 1, 实际 %d", l.Depth())
	}

	// 另一 goroutine(不同 Owner)必须判 busy
	if err := callFromOther(func() error { return l.TryLock(ctx) }); !errors.Is(err, ErrBusy) {
		t.Fatalf("跨 goroutine TryLock 应报 ErrBusy, 实际 %v", err)
	}

	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if l.Held() {
		t.Error("释放后 Held 应为 false")
	}
	// 释放后他人可得
	if err := callFromOther(func() error { return l.TryLock(ctx) }); err != nil {
		t.Fatalf("释放后其他 goroutine 应能获得: %v", err)
	}
}

func TestLocalReentrantSameGoroutine(t *testing.T) {
	l := newTestLocal(t, Config{})
	ctx := context.Background()

	if err := l.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	fence1 := l.Fence()

	if err := l.Lock(ctx); err != nil {
		t.Fatalf("同 goroutine 重入应成功: %v", err)
	}
	if l.Depth() != 2 {
		t.Errorf("重入后深度应为 2, 实际 %d", l.Depth())
	}
	if fence2 := l.Fence(); fence2 != fence1 {
		t.Errorf("重入不应改变栅栏值: %d -> %d", fence1, fence2)
	}

	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("内层 Unlock: %v", err)
	}
	if !l.Held() || l.Depth() != 1 {
		t.Errorf("出栈一层后仍应持有, Held=%v Depth=%d", l.Held(), l.Depth())
	}

	// 仍持有时, 其他 goroutine 拿不到
	if err := callFromOther(func() error { return l.TryLock(ctx) }); !errors.Is(err, ErrBusy) {
		t.Fatalf("深度未归零时应报 ErrBusy, 实际 %v", err)
	}

	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("外层 Unlock: %v", err)
	}
	if l.Depth() != 0 || l.Fence() != 0 {
		t.Errorf("全部出栈后应复位, Depth=%d Fence=%d", l.Depth(), l.Fence())
	}
}

func TestLocalUnlockByNonOwnerRejected(t *testing.T) {
	l := newTestLocal(t, Config{})
	ctx := context.Background()

	if err := l.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	err := callFromOther(func() error { return l.Unlock(ctx) })
	if !errors.Is(err, ErrNotHeld) {
		t.Fatalf("跨 goroutine 释放应被拒绝(ErrNotHeld), 实际 %v", err)
	}
	if !l.Held() {
		t.Error("被拒绝的释放不应改变持有状态")
	}
	if err := l.Unlock(ctx); err != nil {
		t.Errorf("属主自行释放应成功: %v", err)
	}
	// 未持有时再释放
	if err := l.Unlock(ctx); !errors.Is(err, ErrNotHeld) {
		t.Errorf("重复释放应报 ErrNotHeld, 实际 %v", err)
	}
}

func TestLocalLockWakesOnRelease(t *testing.T) {
	// 轮询间隔 1 小时:等待者只能靠释放广播醒来, 否则本用例必然超时
	l := newTestLocal(t, Config{PollInterval: time.Hour, WaitTimeout: 5 * time.Second})
	ctx := context.Background()

	if err := l.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	acquired := make(chan error, 1)
	go func() {
		// 获得锁后由同一 goroutine 释放:默认 goroutine 绑定属主,他人无权解锁
		err := l.Lock(ctx)
		if err == nil {
			err = l.Unlock(ctx)
		}
		acquired <- err
	}()

	time.Sleep(20 * time.Millisecond) // 让等待者进入 select
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("释放后等待者应获得锁并能自行释放: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待者未被释放唤醒(疑为退化成轮询)")
	}
}

func TestLocalWaitTimeout(t *testing.T) {
	l := newTestLocal(t, Config{PollInterval: time.Millisecond, WaitTimeout: 20 * time.Millisecond})
	if err := l.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	err := callFromOther(func() error { return l.Lock(context.Background()) })
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("应报 ErrTimeout, 实际 %v", err)
	}
}

func TestLocalCtxCancelPropagates(t *testing.T) {
	l := newTestLocal(t, Config{PollInterval: time.Millisecond})
	if err := l.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- l.Lock(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("应透传 context.Canceled, 实际 %v", err)
	}
}

func TestLocalUnlockIgnoresCanceledCtx(t *testing.T) {
	l := newTestLocal(t, Config{})
	if err := l.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 资源必须归还:调用方 ctx 已取消也要能释放, 不能因此泄漏
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("已取消 ctx 下 Unlock 仍应成功: %v", err)
	}
	if l.Held() {
		t.Error("应已释放")
	}
}

func TestLocalCloseReleasesAndRejects(t *testing.T) {
	l := newTestLocal(t, Config{})
	if err := l.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("Close 应幂等: %v", err)
	}
	if err := l.TryLock(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 TryLock 应报 ErrClosed, 实际 %v", err)
	}
	if err := l.Lock(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Lock 应报 ErrClosed, 实际 %v", err)
	}
	if l.Held() {
		t.Error("Close 后不应显示持有")
	}
	if l.Depth() != 0 || l.Fence() != 0 {
		t.Errorf("Close 应复位持有视图, Depth=%d Fence=%d", l.Depth(), l.Fence())
	}
}

// TestLocalInstancesNotShared 明确记录 Local 的能力边界:同 Key 的两个实例互不相知。
// 跨实例互斥是 redisstore/consulstore 的领域,本用例防止误将 Local 用于多副本场景。
func TestLocalInstancesNotShared(t *testing.T) {
	ctx := context.Background()
	a := newTestLocal(t, Config{Key: "job:daily"})
	b := newTestLocal(t, Config{Key: "job:daily"})

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	if err := b.TryLock(ctx); err == nil {
		// 这不是 bug,而是 Local 的定义域:无共享状态就无从互斥
		t.Log("b 也拿到了锁——Local 仅在实例内互斥, 多副本场景必须用服务端后端")
	} else {
		t.Errorf("按当前语义 b 应能取得(不共享状态), 实际 %v", err)
	}
}

func TestLocalFenceMonotonicAcrossAcquisitions(t *testing.T) {
	l := newTestLocal(t, Config{})
	ctx := context.Background()

	var prev uint64
	for i := 0; i < 5; i++ {
		if err := l.TryLock(ctx); err != nil {
			t.Fatalf("第 %d 次 TryLock: %v", i+1, err)
		}
		fence := l.Fence()
		if fence <= prev {
			t.Fatalf("栅栏值必须单调递增: 第 %d 次=%d, 上次=%d", i+1, fence, prev)
		}
		prev = fence
		if err := l.Unlock(ctx); err != nil {
			t.Fatalf("Unlock: %v", err)
		}
		if got := l.Fence(); got != 0 {
			t.Errorf("释放后 Fence 应归零, 实际 %d", got)
		}
	}
}

// TestLocalStaticOwnerNoGoroutineIsolation 校验显式静态 Owner 时不做 goroutine 隔离:
// 同进程内不同 goroutine 共用同一实例会互相重入——这是配置语义而非缺陷,
// 用例把它钉下来,避免未来被“改回”成隐式 goid 行为。
func TestLocalStaticOwnerNoGoroutineIsolation(t *testing.T) {
	l := newTestLocal(t, Config{Owner: "job-worker-1"})
	ctx := context.Background()

	if err := l.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := callFromOther(func() error { return l.TryLock(ctx) }); err != nil {
		t.Errorf("静态 Owner 下跨 goroutine 应视作同一持有者重入, 实际 %v", err)
	}
	if got := l.Depth(); got != 2 {
		t.Errorf("重入深度应为 2, 实际 %d", got)
	}
}

func TestLocalConcurrentExclusive(t *testing.T) {
	l := newTestLocal(t, Config{PollInterval: time.Millisecond})

	const (
		goroutines = 32
		rounds     = 20
	)
	var (
		inCritical atomic.Int64
		maxSeen    atomic.Int64
		counter    int64
		wg         sync.WaitGroup
	)
	failCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if err := l.Lock(context.Background()); err != nil {
					failCh <- err
					return
				}
				cur := inCritical.Add(1)
				for {
					old := maxSeen.Load()
					if cur <= old || maxSeen.CompareAndSwap(old, cur) {
						break
					}
				}
				counter++ // 无保护写:仅在持锁期间, 由锁保证互斥
				inCritical.Add(-1)
				if err := l.Unlock(context.Background()); err != nil {
					failCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failCh)

	for err := range failCh {
		t.Fatalf("并发加解锁失败: %v", err)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("临界区最大并发数应为 1, 实际 %d", got)
	}
	if counter != goroutines*rounds {
		t.Errorf("计数应为 %d, 实际 %d", goroutines*rounds, counter)
	}
}

// TestScenarioShardingConcurrentDDL 复刻 sharding 启动期建表竞争:
// 多实例(此处以多 goroutine 表示)同时对同一张分片表执行 CREATE TABLE,
// 接入 lock 后 DDL 必须只执行一次, 其余实例走「已存在」分支。
func TestScenarioShardingConcurrentDDL(t *testing.T) {
	const instances = 8
	var (
		exists    atomic.Bool
		ddlCalls  atomic.Int64
		skipCalls atomic.Int64
	)

	mu := newTestLocal(t, Config{Key: "sharding:ddl:orders_202611", PollInterval: time.Millisecond})
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, instances)
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mu.Lock(ctx); err != nil {
				errs <- err
				return
			}
			defer func() { _ = mu.Unlock(ctx) }()

			if exists.Load() {
				skipCalls.Add(1) // 拿到锁后二次校验:别的实例已建好
				return
			}
			ddlCalls.Add(1)
			// 模拟 DDL 耗时, 放大竞争窗口
			time.Sleep(5 * time.Millisecond)
			exists.Store(true)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("实例加锁失败: %v", err)
	}

	if got := ddlCalls.Load(); got != 1 {
		t.Errorf("CREATE TABLE 应只执行 1 次, 实际 %d 次", got)
	}
	if got := skipCalls.Load(); got != instances-1 {
		t.Errorf("其余 %d 个实例应走已存在分支, 实际 %d", instances-1, got)
	}
}

// TestScenarioDeadLetterRecoverSingleClaim 复刻 deadletter 多副本定时恢复:
// 同一轮 ticker 到期时 4 个副本同时 TryLock,只有拿到锁的那个能捞取并重投死信。
// 启动屏障保证四者真实碰撞,断言的是两个硬不变式:
//  1. 每轮恰一个副本捞取(持锁者慢执行,其余必判 busy 而跳过)
//  2. 捞取动作从不并发发生(max in-flight == 1)
func TestScenarioDeadLetterRecoverSingleClaim(t *testing.T) {
	const (
		replicas = 4
		rounds   = 3
	)
	mu := newTestLocal(t, Config{Key: "deadletter:recover"})
	ctx := context.Background()

	var (
		claims    atomic.Int64
		inFlight  atomic.Int64
		maxActive atomic.Int64
		skips     atomic.Int64
	)

	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for i := 0; i < replicas; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate // 同点起跑, 制造真实竞争
				if err := mu.TryLock(ctx); err != nil {
					if !errors.Is(err, ErrBusy) {
						t.Errorf("副本 TryLock 异常: %v", err)
					}
					skips.Add(1) // busy 即本轮跳过, 不报错也不重复捞取
					return
				}
				cur := inFlight.Add(1)
				for {
					old := maxActive.Load()
					if cur <= old || maxActive.CompareAndSwap(old, cur) {
						break
					}
				}
				claims.Add(1)
				time.Sleep(5 * time.Millisecond) // 模拟捞取+重投耗时
				inFlight.Add(-1)
				if err := mu.Unlock(ctx); err != nil {
					t.Errorf("收尾 Unlock: %v", err)
				}
			}()
		}
		close(gate)
		wg.Wait()
	}

	if got := claims.Load(); got != int64(rounds) {
		t.Errorf("%d 轮应恰好执行 %d 次, 实际 %d", rounds, rounds, got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Errorf("捞取并发数应始终为 1, 实际峰值 %d", got)
	}
	if got := skips.Load(); got != int64(rounds*(replicas-1)) {
		t.Errorf("应有 %d 次 busy 跳过, 实际 %d", rounds*(replicas-1), got)
	}
}

// TestLocalOwnerFuncEmptyErrs 校验 OwnerFunc 返回空串时报错而非静默变成同一属主
func TestLocalOwnerFuncEmptyErrs(t *testing.T) {
	l := newTestLocal(t, Config{OwnerFunc: func() string { return "" }})
	err := l.TryLock(context.Background())
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("空 Owner 应报 ErrInvalidConfig, 实际 %v", err)
	}
	if errors.Is(err, ErrBusy) {
		t.Error("配置错误不应被误报为锁被占用")
	}
}
