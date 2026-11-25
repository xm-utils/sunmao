package redisstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/sunmao/lock"
)

type harness struct {
	mr     *miniredis.Miniredis
	client *redis.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return &harness{mr: mr, client: client}
}

// newMutex 构造一个"副本"实例。owner 非空时设为静态属主, 便于多实例用例里区分属主
// (同一 goroutine 内构造多实例时, 默认的 goroutine 绑定属主会互相误重入);
// owner 为空则保留默认属主——并发用例必须如此, 否则同实例上的多个 goroutine
// 因 Owner 相同而互相重入, 测不出真互斥。
func (h *harness) newMutex(t *testing.T, owner string, cfg lock.Config) *Mutex {
	t.Helper()
	cfg.Key = "test:lock"
	if owner != "" {
		cfg.Owner = owner
	}
	m, err := New(Config{Client: h.client, Config: cfg})
	if err != nil {
		t.Fatalf("New(%s): %v", owner, err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func (h *harness) hashGet(t *testing.T, field string) string {
	t.Helper()
	v := h.mr.HGet("xm_lock:{test:lock}", field)
	return v
}

func TestNewValidatesConfig(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name string
		cfg  Config
	}{
		{"缺 Client", Config{Config: lock.Config{Key: "k"}}},
		{"缺 Key", Config{Client: h.client}},
		{"续期不小于租期", Config{Client: h.client, Config: lock.Config{Key: "k", TTL: time.Second, RenewInterval: 2 * time.Second}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); !errors.Is(err, lock.ErrInvalidConfig) {
				t.Fatalf("应报 lock.ErrInvalidConfig, 实际 %v", err)
			}
		})
	}
}

func TestAcquireWritesServerState(t *testing.T) {
	h := newHarness(t)
	m := h.newMutex(t, "replica-a", lock.Config{TTL: 30 * time.Second})

	if err := m.TryLock(context.Background()); err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if got := h.hashGet(t, "owner"); got != "replica-a" {
		t.Errorf("服务端 owner 字段=%q, 期望 replica-a", got)
	}
	if got := h.hashGet(t, "depth"); got != "1" {
		t.Errorf("服务端 depth 字段=%q, 期望 1", got)
	}
	if got := h.hashGet(t, "fence"); got != "1" {
		t.Errorf("服务端 fence 字段=%q, 期望 1", got)
	}
	if m.Fence() != 1 {
		t.Errorf("本地栅栏值应为 1, 实际 %d", m.Fence())
	}
	ttl, err := m.RemainingTTL(context.Background())
	if err != nil {
		t.Fatalf("RemainingTTL: %v", err)
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Errorf("剩余租期应落在 (0, 30s], 实际 %v", ttl)
	}
}

func TestSecondInstanceSeesBusy(t *testing.T) {
	h := newHarness(t)
	a := h.newMutex(t, "replica-a", lock.Config{TTL: 30 * time.Second})
	b := h.newMutex(t, "replica-b", lock.Config{TTL: 30 * time.Second, WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond})

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	if err := b.TryLock(context.Background()); !errors.Is(err, lock.ErrBusy) {
		t.Fatalf("b.TryLock 应报 ErrBusy, 实际 %v", err)
	}
	// 阻塞获取同样失败, 但报的是超时(不是"抢到")
	if err := b.Lock(context.Background()); !errors.Is(err, lock.ErrTimeout) {
		t.Fatalf("b.Lock 应报 ErrTimeout, 实际 %v", err)
	}
	if b.Held() {
		t.Error("b 未取得锁, Held 必须为 false")
	}
}

func TestReentrantCountsOnServer(t *testing.T) {
	h := newHarness(t)
	// 同一实例、同一 goroutine(Owner 相同)递归加锁:服务端深度累加, 栅栏值不变
	m := h.newMutex(t, "worker-1", lock.Config{TTL: 30 * time.Second})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("外层 Lock: %v", err)
	}
	if err := m.Lock(ctx); err != nil {
		t.Fatalf("内层 Lock 应重入成功: %v", err)
	}
	if got := h.hashGet(t, "depth"); got != "2" {
		t.Errorf("服务端 depth 应为 2, 实际 %q", got)
	}
	if m.Depth() != 2 || m.Fence() != 1 {
		t.Errorf("重入后本地视图应 Depth=2/Fence=1, 实际 %d/%d", m.Depth(), m.Fence())
	}

	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("内层 Unlock: %v", err)
	}
	if got := h.hashGet(t, "depth"); got != "1" {
		t.Errorf("出栈一层后 depth 应为 1, 实际 %q", got)
	}
	if !h.mr.Exists("xm_lock:{test:lock}") {
		t.Error("depth 未归零时锁 Key 不应被删除")
	}

	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("外层 Unlock: %v", err)
	}
	if h.mr.Exists("xm_lock:{test:lock}") {
		t.Error("深度归零后锁 Key 应被删除")
	}
	if !h.mr.Exists("xm_lock:{test:lock}:fence") {
		t.Error("栅栏计数 Key 应保留以保证单调性")
	}
}

func TestFenceMonotonicAcrossAcquisitions(t *testing.T) {
	h := newHarness(t)
	m := h.newMutex(t, "worker-1", lock.Config{})
	ctx := context.Background()

	var prev uint64
	for i := 1; i <= 4; i++ {
		if err := m.TryLock(ctx); err != nil {
			t.Fatalf("第 %d 次 TryLock: %v", i, err)
		}
		if got := m.Fence(); got != uint64(i) {
			t.Fatalf("第 %d 次栅栏值应为 %d, 实际 %d", i, i, got)
		}
		if err := m.Unlock(ctx); err != nil {
			t.Fatalf("Unlock: %v", err)
		}
		if prev = m.Fence(); prev != 0 {
			t.Fatalf("释放后本地栅栏应归零, 实际 %d", prev)
		}
	}
}

func TestExpiredLeaseCanBePreempted(t *testing.T) {
	h := newHarness(t)
	a := h.newMutex(t, "replica-a", lock.Config{TTL: 100 * time.Millisecond, RenewInterval: -1})
	b := h.newMutex(t, "replica-b", lock.Config{TTL: 30 * time.Second})
	ctx := context.Background()

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	h.mr.FastForward(150 * time.Millisecond) // 租期到期, Redis 侧 Key 消失

	if err := b.TryLock(ctx); err != nil {
		t.Fatalf("租期过期后 b 应能抢占: %v", err)
	}
	// a 自认为仍持有, 服务端已易主: 释放必须同时报 ErrLost 与 ErrNotHeld
	err := a.Unlock(ctx)
	if !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("过期持有者 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
	if !errors.Is(err, lock.ErrLost) {
		t.Errorf("属主已失应同时报 ErrLost, 实际 %v", err)
	}
	if a.Held() || a.Depth() != 0 {
		t.Errorf("属主已失后 a 本地视图应复位, Held=%v Depth=%d", a.Held(), a.Depth())
	}
	// b 仍能正常释放自己的锁
	if err := b.Unlock(ctx); err != nil {
		t.Errorf("b.Unlock: %v", err)
	}
}

func TestWatchdogRenewExtendsLease(t *testing.T) {
	h := newHarness(t)
	var lost atomic.Value
	m := h.newMutex(t, "worker-1", lock.Config{
		TTL:           10 * time.Second,
		RenewInterval: time.Second,
		OnRenewError:  func(err error) { lost.Store(err) },
	})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	h.mr.FastForward(6 * time.Second)
	before, err := m.RemainingTTL(ctx)
	if err != nil {
		t.Fatalf("RemainingTTL: %v", err)
	}
	if before > 5*time.Second {
		t.Fatalf("FastForward 后剩余租期应缩短, 实际 %v", before)
	}

	if stop := m.wd.Tick(ctx); stop {
		t.Fatal("续期成功不应停止看门狗")
	}
	after, err := m.RemainingTTL(ctx)
	if err != nil {
		t.Fatalf("RemainingTTL: %v", err)
	}
	if after < 9*time.Second {
		t.Errorf("续期后剩余租期应回到接近 10s, 实际 %v", after)
	}
	if v := lost.Load(); v != nil {
		t.Errorf("续期成功不应触发 OnRenewError, 实际 %v", v)
	}
}

func TestWatchdogDetectsPreemption(t *testing.T) {
	h := newHarness(t)
	var reported atomic.Value
	m := h.newMutex(t, "worker-1", lock.Config{
		TTL:           10 * time.Second,
		RenewInterval: time.Second,
		OnRenewError:  func(err error) { reported.Store(err) },
	})
	if err := m.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// 模拟另一持有者抢占(租期到期后写入新属主)
	h.mr.HSet("xm_lock:{test:lock}", "owner", "thief")

	if stop := m.wd.Tick(context.Background()); !stop {
		t.Fatal("属主被抢占后看门狗应停止")
	}
	err, _ := reported.Load().(error)
	if err == nil || !errors.Is(err, lock.ErrLost) {
		t.Fatalf("OnRenewError 应收到包装 ErrLost 的错误, 实际 %v", err)
	}
	if !strings.Contains(err.Error(), "thief") {
		t.Logf("错误信息未含新属主(可接受): %v", err)
	}
	if m.Held() || m.Depth() != 0 {
		t.Errorf("属主已失后本地视图应复位, Held=%v Depth=%d", m.Held(), m.Depth())
	}
	// 服务端 Key 已不属于自己, 后续释放同样报 ErrLost
	if err := m.Unlock(context.Background()); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("未持有时 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
}

func TestUnlockAfterLocalReleaseIsNotHeld(t *testing.T) {
	h := newHarness(t)
	m := h.newMutex(t, "worker-1", lock.Config{})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// 本地深度为 0 时直接拒绝, 不浪费一次 RTT
	if err := m.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("重复 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
}

func TestFailClosedOnBackendError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	m, err := New(Config{Client: client, Config: lock.Config{Key: "test:lock", Owner: "worker-1", WaitTimeout: 50 * time.Millisecond, PollInterval: time.Millisecond}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = m.Close() }()

	mr.Close() // 后端故障

	// 关键契约:后端故障绝不能被当作"获得锁"
	if err := m.TryLock(context.Background()); err == nil {
		t.Fatal("后端故障时 TryLock 不得返回 nil")
	} else if errors.Is(err, lock.ErrBusy) {
		t.Errorf("后端故障不应被误报为 ErrBusy: %v", err)
	}
	if m.Held() {
		t.Error("后端故障时不得显示持有")
	}
	if err := m.Lock(context.Background()); err == nil {
		t.Error("后端故障时 Lock 不得返回 nil")
	}
}

func TestCtxCancelPropagates(t *testing.T) {
	h := newHarness(t)
	a := h.newMutex(t, "replica-a", lock.Config{})
	b := h.newMutex(t, "replica-b", lock.Config{PollInterval: time.Millisecond})
	ctx := context.Background()

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	callCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if err := b.Lock(callCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("应透传 context.Canceled, 实际 %v", err)
	}
}

func TestCloseReleasesHeldLock(t *testing.T) {
	h := newHarness(t)
	m := h.newMutex(t, "worker-1", lock.Config{TTL: 30 * time.Second})
	if err := m.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if h.mr.Exists("xm_lock:{test:lock}") {
		t.Error("Close 应释放服务端锁")
	}
	if err := m.TryLock(context.Background()); !errors.Is(err, lock.ErrClosed) {
		t.Errorf("关闭后应报 ErrClosed, 实际 %v", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close 应可重复调用: %v", err)
	}
	// 他人可立即获得
	other := h.newMutex(t, "worker-2", lock.Config{})
	if err := other.TryLock(context.Background()); err != nil {
		t.Errorf("Close 后他人应能获得: %v", err)
	}
}

// TestRefreshExtendsLeaseWhenWatchdogDisabled 钉住 RenewInterval=-1 的手动续期路径:
// 看门狗已闭,长任务只能靠 Refresh 撑住租期;错误语义与自动续期一致。
func TestRefreshExtendsLeaseWhenWatchdogDisabled(t *testing.T) {
	h := newHarness(t)
	m := h.newMutex(t, "worker-1", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	h.mr.FastForward(6 * time.Second)
	before, err := m.RemainingTTL(ctx)
	if err != nil || before > 5*time.Second {
		t.Fatalf("FastForward 后剩余租期应缩短, 实际 %v(err=%v)", before, err)
	}

	if err := m.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	after, err := m.RemainingTTL(ctx)
	if err != nil {
		t.Fatalf("RemainingTTL: %v", err)
	}
	if after < 9*time.Second {
		t.Errorf("Refresh 后租期应回到接近 10s, 实际 %v", after)
	}

	// 未持有时不得假装续期成功
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := m.Refresh(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("未持有 Refresh 应报 ErrNotHeld, 实际 %v", err)
	}

	// 租期已到期:Refresh 报 ErrLost 并复位本地视图
	if err := m.Lock(ctx); err != nil {
		t.Fatalf("再次 Lock: %v", err)
	}
	h.mr.FastForward(11 * time.Second)
	if err := m.Refresh(ctx); !errors.Is(err, lock.ErrLost) {
		t.Errorf("租期过期后 Refresh 应报 ErrLost, 实际 %v", err)
	}
	if m.Held() || m.Depth() != 0 {
		t.Errorf("属主已失后本地视图应复位, Held=%v Depth=%d", m.Held(), m.Depth())
	}
}

func TestConcurrentInstancesExclusive(t *testing.T) {
	h := newHarness(t)
	const (
		instances = 3
		perInst   = 12
	)
	locks := make([]*Mutex, instances)
	for i := range locks {
		// 不指定 Owner:默认按 goroutine 绑定, 同实例上的并发才是真互斥
		locks[i] = h.newMutex(t, "", lock.Config{
			TTL:          30 * time.Second,
			PollInterval: time.Millisecond,
			WaitTimeout:  10 * time.Second,
		})
	}

	var (
		inFlight  atomic.Int64
		maxActive atomic.Int64
		counter   int64
		wg        sync.WaitGroup
		errs      = make(chan error, instances*perInst)
	)
	for _, m := range locks {
		for r := 0; r < perInst; r++ {
			wg.Add(1)
			go func(m *Mutex, round int) {
				defer wg.Done()
				ctx := context.Background()
				if err := m.Lock(ctx); err != nil {
					errs <- err
					return
				}
				cur := inFlight.Add(1)
				for {
					old := maxActive.Load()
					if cur <= old || maxActive.CompareAndSwap(old, cur) {
						break
					}
				}
				counter += int64(round % 2)
				inFlight.Add(-1)
				if err := m.Unlock(ctx); err != nil {
					errs <- err
				}
			}(m, r)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发加解锁失败: %v", err)
	}
	if got := maxActive.Load(); got != 1 {
		t.Errorf("跨实例临界区并发数应为 1, 实际峰值 %d", got)
	}
}

// TestScenarioShardingConcurrentDDL 用 redisstore 复现多实例启动建表:
// 三个"实例"争抢同一把 DDL 锁, 建表语句只应执行一次。
func TestScenarioShardingConcurrentDDL(t *testing.T) {
	h := newHarness(t)
	names := []string{"pod-1", "pod-2", "pod-3"}
	locks := make([]*Mutex, 0, len(names))
	for _, n := range names {
		locks = append(locks, h.newMutex(t, n, lock.Config{
			TTL:          30 * time.Second,
			PollInterval: time.Millisecond,
			WaitTimeout:  5 * time.Second,
		}))
	}

	var (
		created  atomic.Bool
		ddlCalls atomic.Int64
		skipped  atomic.Int64
		wg       sync.WaitGroup
		errs     = make(chan error, len(names))
		gate     = make(chan struct{})
	)
	for _, m := range locks {
		wg.Add(1)
		go func(m *Mutex) {
			defer wg.Done()
			ctx := context.Background()
			<-gate
			if err := m.Lock(ctx); err != nil {
				errs <- err
				return
			}
			defer func() { _ = m.Unlock(ctx) }()

			if created.Load() {
				skipped.Add(1)
				return
			}
			ddlCalls.Add(1)
			time.Sleep(5 * time.Millisecond) // 模拟 DDL 耗时
			created.Store(true)
		}(m)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("实例加锁失败: %v", err)
	}

	if got := ddlCalls.Load(); got != 1 {
		t.Errorf("CREATE TABLE 应只执行 1 次, 实际 %d", got)
	}
	if got := skipped.Load(); got != int64(len(names)-1) {
		t.Errorf("其余实例应走已存在分支, 期望 %d 实际 %d", len(names)-1, got)
	}
}

// TestScenarioDeadLetterMultiReplicaClaim 用 redisstore 复现 deadletter 多副本恢复:
// 跨进程实例之间靠服务端属主判定互斥, 同轮只一个副本捞取。
func TestScenarioDeadLetterMultiReplicaClaim(t *testing.T) {
	h := newHarness(t)
	replicas := []string{"dlv1", "dlv2", "dlv3", "dlv4"}
	locks := make([]*Mutex, 0, len(replicas))
	for _, r := range replicas {
		locks = append(locks, h.newMutex(t, r, lock.Config{TTL: 30 * time.Second}))
	}

	var (
		claims    atomic.Int64
		inFlight  atomic.Int64
		maxActive atomic.Int64
		skips     atomic.Int64
	)
	const rounds = 3
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for _, m := range locks {
			wg.Add(1)
			go func(m *Mutex) {
				defer wg.Done()
				ctx := context.Background()
				<-gate
				if err := m.TryLock(ctx); err != nil {
					if !errors.Is(err, lock.ErrBusy) {
						t.Errorf("副本 TryLock 异常: %v", err)
					}
					skips.Add(1)
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
				time.Sleep(5 * time.Millisecond)
				inFlight.Add(-1)
				if err := m.Unlock(ctx); err != nil {
					t.Errorf("收尾 Unlock: %v", err)
				}
			}(m)
		}
		close(gate)
		wg.Wait()
	}

	if got := claims.Load(); got != rounds {
		t.Errorf("%d 轮应恰好 %d 次捞取, 实际 %d", rounds, rounds, got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Errorf("捞取并发数应始终为 1, 实际峰值 %d", got)
	}
	if got := skips.Load(); got != rounds*(int64(len(replicas))-1) {
		t.Errorf("应有 %d 次 busy 跳过, 实际 %d", rounds*(int64(len(replicas))-1), got)
	}
}

// callOther 在独立 goroutine 中执行 fn 并回传结果——默认 Owner 为 goroutine 绑定,
// 只有跨 goroutine 才能验真「仅持有者可释放」契约。
func callOther(fn func() error) error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return <-ch
}

// TestUnlockByOtherGoroutineRejected 钉住契约:默认 goroutine 绑定属主下,
// 跨 goroutine 释放必须被拒, 服务端状态保持不变;而 Close 作为生命周期收尾
// 仍应能归还租约, 否则锁会白白占到 TTL 到期。
func TestUnlockByOtherGoroutineRejected(t *testing.T) {
	h := newHarness(t)
	m, err := New(Config{Client: h.client, Config: lock.Config{Key: "test:lock", TTL: 30 * time.Second}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	held := h.hashGet(t, "owner")
	if held == "" {
		t.Fatal("获得锁后服务端应有属主记录")
	}

	if err := callOther(func() error { return m.Unlock(ctx) }); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("跨 goroutine Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
	if got := h.hashGet(t, "owner"); got != held {
		t.Errorf("被拒的释放不得改变服务端状态, owner %q -> %q", held, got)
	}

	// 他人也不能代本实例释放:用同一 Key 的第二个客户端尝试
	other := h.newMutex(t, "thief", lock.Config{TTL: 30 * time.Second})
	if err := other.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("未持有者 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}

	if err := callOther(func() error { return m.Close() }); err != nil {
		t.Errorf("跨 goroutine Close 应能归还租约, 实际 %v", err)
	}
	if h.mr.Exists("xm_lock:{test:lock}") {
		t.Error("Close 后服务端锁应已删除")
	}
}
