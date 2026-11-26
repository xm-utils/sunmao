package consulstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/hashicorp/consul/api"

	"github.com/xm-utils/sunmao/lock"
)

// fakeBackend 是 Backend 的内存替身,复刻 Consul 的 Session+KV 语义:
//   - KV 被某个存活会话占用时 Acquire 返回 false;
//   - Release 只对当前持有者生效, 他人释放返回 false;
//   - 会话失效(expireSession)会连带释放它占用的 KV——即进程崩溃后的 TTL 回收。
//
// 错误注入(errXxx)与 panicOn 用于覆盖 fail-closed 与 panic 隔离分支。
type fakeBackend struct {
	mu       sync.Mutex
	sessions map[string]*fakeSession
	kv       map[string]string // KV 键 -> 持有它的 sessionID
	seq      int
	calls    map[string]int

	errCreate  error
	errAcquire error
	errRelease error
	errRenew   error
	errDestroy error
	panicOn    string

	// renewEmptyEntry 复刻真实 consul/api 的返回形态:会话已不存在时
	// Renew 给 (nil, nil) 而不是 error(集成用例在 Consul 1.22 上实测到)。
	renewEmptyEntry bool
}

type fakeSession struct {
	name string
	ttl  time.Duration
	live bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		sessions: make(map[string]*fakeSession),
		kv:       make(map[string]string),
		calls:    make(map[string]int),
	}
}

func (f *fakeBackend) record(op string) { f.calls[op]++ }

func (f *fakeBackend) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

// holder 返回占用某 KV 的 sessionID, 无人占用返回空串
func (f *fakeBackend) holder(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kv[key]
}

func (f *fakeBackend) liveSessions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.sessions {
		if s.live {
			n++
		}
	}
	return n
}

func (f *fakeBackend) sessionName(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.sessions[id]; s != nil {
		return s.name
	}
	return ""
}

// expireSession 模拟会话 TTL 到期被 Consul 回收:会话失效并释放其占用的 KV
func (f *fakeBackend) expireSession(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.sessions[id]; s != nil {
		s.live = false
	}
	for k, v := range f.kv {
		if v == id {
			delete(f.kv, k)
		}
	}
}

func (f *fakeBackend) CreateSession(name string, ttl time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CreateSession")
	if f.panicOn == "CreateSession" {
		panic("后端 panic: CreateSession")
	}
	if f.errCreate != nil {
		return "", f.errCreate
	}
	f.seq++
	id := fmt.Sprintf("sess-%d", f.seq)
	f.sessions[id] = &fakeSession{name: name, ttl: ttl, live: true}
	return id, nil
}

func (f *fakeBackend) DestroySession(sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DestroySession")
	if f.panicOn == "DestroySession" {
		panic("后端 panic: DestroySession")
	}
	if f.errDestroy != nil {
		return f.errDestroy
	}
	if s := f.sessions[sessionID]; s != nil {
		s.live = false
	}
	for k, v := range f.kv {
		if v == sessionID {
			delete(f.kv, k)
		}
	}
	return nil
}

func (f *fakeBackend) RenewSession(sessionID string) (*api.SessionEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("RenewSession")
	if f.panicOn == "RenewSession" {
		panic("后端 panic: RenewSession")
	}
	if f.errRenew != nil {
		return nil, f.errRenew
	}
	if f.renewEmptyEntry {
		return nil, nil
	}
	s := f.sessions[sessionID]
	if s == nil || !s.live {
		return nil, errors.New("consul: session 已失效")
	}
	return &api.SessionEntry{ID: sessionID, Name: s.name, TTL: s.ttl.String()}, nil
}

func (f *fakeBackend) AcquireLock(key string, sessionID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("AcquireLock")
	if f.panicOn == "AcquireLock" {
		panic("后端 panic: AcquireLock")
	}
	if f.errAcquire != nil {
		return false, f.errAcquire
	}
	if cur, ok := f.kv[key]; ok {
		// 占用者会话已失效则视同空闲(真实 Consul 已自动释放)
		live := false
		if s := f.sessions[cur]; s != nil {
			live = s.live
		}
		if live {
			return cur == sessionID, nil // 同会话重复 Acquire 本就幂等
		}
		delete(f.kv, key)
	}
	f.kv[key] = sessionID
	return true, nil
}

func (f *fakeBackend) ReleaseLock(key string, sessionID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("ReleaseLock")
	if f.panicOn == "ReleaseLock" {
		panic("后端 panic: ReleaseLock")
	}
	if f.errRelease != nil {
		return false, f.errRelease
	}
	if f.kv[key] != sessionID {
		return false, nil
	}
	delete(f.kv, key)
	return true, nil
}

var _ Backend = (*fakeBackend)(nil)

// newFakeMutex 构造一个"副本"实例。owner 非空时设为静态属主(便于多实例用例区分);
// owner 为空则用默认的 goroutine 绑定属主——并发用例必须如此,
// 否则同一实例上的多个 goroutine 会因 Owner 相同而互相重入, 不是真互斥。
func newFakeMutex(t *testing.T, f *fakeBackend, owner string, cfg lock.Config) *Mutex {
	t.Helper()
	if cfg.Key == "" {
		cfg.Key = "test:lock"
	}
	if owner != "" {
		cfg.Owner = owner
	}
	m, err := New(Config{Backend: f, Config: cfg})
	if err != nil {
		t.Fatalf("New(%s): %v", owner, err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// callOther 在独立 goroutine 中执行 fn 并回传结果(默认 Owner 为 goroutine 绑定)
func callOther(fn func() error) error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return <-ch
}

func TestNewValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"缺 Backend", Config{Config: lock.Config{Key: "k"}}},
		{"缺 Key", Config{Backend: newFakeBackend()}},
		{"续期不小于租期", Config{Backend: newFakeBackend(), Config: lock.Config{Key: "k", TTL: time.Second, RenewInterval: 2 * time.Second}}},
		{"租期低于毫秒下限", Config{Backend: newFakeBackend(), Config: lock.Config{Key: "k", TTL: 500 * time.Microsecond}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); !errors.Is(err, lock.ErrInvalidConfig) {
				t.Fatalf("应报 lock.ErrInvalidConfig, 实际 %v", err)
			}
		})
	}
}

func TestKVKeyUsesPrefix(t *testing.T) {
	f := newFakeBackend()
	def, err := New(Config{Backend: f, Config: lock.Config{Key: "job:daily"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := def.KVKey(), defaultPrefix+"/job:daily"; got != want {
		t.Errorf("默认键=%q, 期望 %q", got, want)
	}
	if def.Key() != "job:daily" {
		t.Errorf("Key()=%q", def.Key())
	}

	custom, err := New(Config{Backend: f, Prefix: "order_svc", Config: lock.Config{Key: "job:daily"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := custom.KVKey(); got != "order_svc/job:daily" {
		t.Errorf("自定义前缀键=%q", got)
	}
}

func TestAcquireCreatesSessionAndHoldsKV(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "replica-a", lock.Config{TTL: 30 * time.Second})

	if err := m.TryLock(context.Background()); err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if !m.Held() {
		t.Error("Held 应为 true")
	}
	sid := m.SessionID()
	if sid == "" {
		t.Fatal("获得锁后应已创建会话")
	}
	if got := f.holder(m.KVKey()); got != sid {
		t.Errorf("KV 占用者=%q, 期望 %q", got, sid)
	}
	// 会话名带上锁 Key 与持有者, 便于在 consul session list 里定位
	if name := f.sessionName(sid); !strings.Contains(name, "test:lock") || !strings.Contains(name, "replica-a") {
		t.Errorf("会话名=%q, 应含锁 Key 与持有者", name)
	}
	if got := f.count("CreateSession"); got != 1 {
		t.Errorf("首次获取应只建一次会话, 实际 %d", got)
	}

	if err := m.Unlock(context.Background()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if got := f.holder(m.KVKey()); got != "" {
		t.Errorf("释放后 KV 仍被 %q 占用", got)
	}
	if got := f.liveSessions(); got != 0 {
		t.Errorf("释放后会话应已销毁, 仍存活 %d 个", got)
	}
	if m.SessionID() != "" {
		t.Error("释放后不应残留会话 ID")
	}
}

func TestSecondReplicaBusyAndTimeout(t *testing.T) {
	f := newFakeBackend()
	a := newFakeMutex(t, f, "replica-a", lock.Config{TTL: 30 * time.Second})
	b := newFakeMutex(t, f, "replica-b", lock.Config{
		TTL: 30 * time.Second, WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond,
	})
	ctx := context.Background()

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	if err := b.TryLock(ctx); !errors.Is(err, lock.ErrBusy) {
		t.Fatalf("b.TryLock 应报 ErrBusy, 实际 %v", err)
	}
	if err := b.Lock(ctx); !errors.Is(err, lock.ErrTimeout) {
		t.Fatalf("b.Lock 应报 ErrTimeout, 实际 %v", err)
	}
	if b.Held() {
		t.Error("未取得锁的副本 Held 必须为 false")
	}
	// 关键清理:获取失败时创建的临时会话必须销毁, 否则每轮空跑都白占一份会话
	if got := f.liveSessions(); got != 1 {
		t.Errorf("仅 a 的会话应存活, 实际 %d 个", got)
	}
}

func TestReentrantIsLocalOnly(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("外层 Lock: %v", err)
	}
	if err := m.Lock(ctx); err != nil {
		t.Fatalf("内层 Lock 应重入: %v", err)
	}
	if got := f.count("AcquireLock"); got != 1 {
		t.Errorf("重入不应再次下发 Acquire, 实际 %d 次", got)
	}
	if m.Depth() != 2 {
		t.Errorf("重入深度应为 2, 实际 %d", m.Depth())
	}
	// 同 Session 重复 Acquire 在 Consul 侧本就幂等, 深度归本地计数——
	// 重入顺带续期一次, 避免深层递归长期不返回而临近过期
	if got := f.count("RenewSession"); got != 1 {
		t.Errorf("重入应顺带续期 1 次, 实际 %d", got)
	}

	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("内层 Unlock: %v", err)
	}
	if got := f.count("ReleaseLock"); got != 0 {
		t.Errorf("深度未归零不应释放 KV, 实际 %d 次", got)
	}
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("外层 Unlock: %v", err)
	}
	if got := f.count("ReleaseLock"); got != 1 {
		t.Errorf("归零时应恰好释放 1 次, 实际 %d", got)
	}
	if m.Depth() != 0 || m.SessionID() != "" {
		t.Errorf("全部出栈后应复位, Depth=%d Session=%q", m.Depth(), m.SessionID())
	}
}

func TestReentrantRenewFailureRollsBackDepth(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second, RenewInterval: -1})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("外层 Lock: %v", err)
	}
	f.errRenew = errors.New("consul: 续期不可用")
	if err := m.Lock(ctx); err == nil {
		t.Fatal("重入续期失败应报错而非假装获得")
	}
	if m.Depth() != 1 {
		t.Errorf("失败的重入不得留下深度, 实际 %d", m.Depth())
	}
	f.errRenew = nil
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

// TestEmptyRenewEntryDetectedAsLost 真实 consul/api 对已销毁/已过期的会话
// 返回 (nil, nil) 而非 error:若只看 error,看门狗会对着一个废会话一直"续期成功",
// 互斥性就此静默失效。手动 Refresh、看门狗 Tick、重入三条路径都得判出来。
func TestEmptyRenewEntryDetectedAsLost(t *testing.T) {
	ctx := context.Background()

	t.Run("手动 Refresh", func(t *testing.T) {
		f := newFakeBackend()
		m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
		if err := m.Lock(ctx); err != nil {
			t.Fatalf("Lock: %v", err)
		}
		f.renewEmptyEntry = true
		if err := m.Refresh(ctx); !errors.Is(err, lock.ErrLost) {
			t.Fatalf("空条目应判 ErrLost, 实际 %v", err)
		}
		if m.Held() || m.Depth() != 0 || m.SessionID() != "" {
			t.Errorf("判为丢锁后本地视图应复位, Held=%v Depth=%d Session=%q", m.Held(), m.Depth(), m.SessionID())
		}
		st := m.Stats()
		if st.Lost == 0 || st.RenewFailures == 0 {
			t.Errorf("空条目应计入 Lost 与 RenewFailures, 快照 %+v", st)
		}
		if err := m.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
			t.Errorf("复位后 Unlock 应报 ErrNotHeld, 实际 %v", err)
		}
	})

	t.Run("看门狗 Tick", func(t *testing.T) {
		f := newFakeBackend()
		var reported atomic.Value
		m := newFakeMutex(t, f, "worker-1", lock.Config{
			TTL:           10 * time.Second,
			RenewInterval: -1,
			OnRenewError:  func(err error) { reported.Store(err) },
		})
		if err := m.Lock(ctx); err != nil {
			t.Fatalf("Lock: %v", err)
		}
		f.renewEmptyEntry = true
		if stop := m.wd.Tick(ctx); !stop {
			t.Error("空条目续期后看门狗应停止, 不该继续空转")
		}
		if got, _ := reported.Load().(error); got == nil || !errors.Is(got, lock.ErrLost) {
			t.Errorf("OnRenewError 应收到 ErrLost, 实际 %v", got)
		}
	})

	t.Run("重入顺带续期", func(t *testing.T) {
		f := newFakeBackend()
		m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
		if err := m.Lock(ctx); err != nil {
			t.Fatalf("Lock: %v", err)
		}
		f.renewEmptyEntry = true
		if err := m.Lock(ctx); err == nil {
			t.Error("重入遇空条目应报错而非假装获得")
		}
		if m.Depth() != 1 {
			t.Errorf("失败的重入不得改变深度, 期望 1, 实际 %d", m.Depth())
		}
	})
}

func TestCrossGoroutineReleaseRejected(t *testing.T) {
	f := newFakeBackend()
	m, err := New(Config{Backend: f, Config: lock.Config{Key: "test:lock", TTL: 30 * time.Second}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	if err := callOther(func() error { return m.Unlock(ctx) }); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("跨 goroutine Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
	if got := f.count("ReleaseLock"); got != 0 {
		t.Errorf("被拒的释放不应发起后端调用, 实际 %d 次", got)
	}
	if sid := m.SessionID(); f.holder(m.KVKey()) != sid {
		t.Error("被拒的释放不应改变 KV 占用状态")
	}

	// Close 是生命周期收尾:即便不在持有 goroutine 上, 也必须归还 KV 并销毁会话
	if err := callOther(func() error { return m.Close() }); err != nil {
		t.Errorf("跨 goroutine Close 应成功归还, 实际 %v", err)
	}
	if got := f.holder(m.KVKey()); got != "" {
		t.Errorf("Close 后 KV 仍被 %q 占用", got)
	}
	if got := f.liveSessions(); got != 0 {
		t.Errorf("Close 后会话应已销毁, 仍存活 %d 个", got)
	}
	if err := m.TryLock(ctx); !errors.Is(err, lock.ErrClosed) {
		t.Errorf("关闭后应报 ErrClosed, 实际 %v", err)
	}
}

func TestExpiredSessionCanBePreempted(t *testing.T) {
	f := newFakeBackend()
	a := newFakeMutex(t, f, "replica-a", lock.Config{TTL: 15 * time.Second, RenewInterval: -1})
	b := newFakeMutex(t, f, "replica-b", lock.Config{TTL: 30 * time.Second})
	ctx := context.Background()

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a.TryLock: %v", err)
	}
	f.expireSession(a.SessionID()) // 模拟 a 进程假死、会话 TTL 到期

	if err := b.TryLock(ctx); err != nil {
		t.Fatalf("会话回收后 b 应能抢占: %v", err)
	}
	// a 自认仍持有, 服务端已易主: 释放被拒时须同时报 ErrLost 与 ErrNotHeld
	err := a.Unlock(ctx)
	if !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("过期持有者 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
	if !errors.Is(err, lock.ErrLost) {
		t.Errorf("属主已失应同时报 ErrLost, 实际 %v", err)
	}
	if a.Held() || a.Depth() != 0 || a.SessionID() != "" {
		t.Errorf("属主已失后本地视图应复位, Held=%v Depth=%d Session=%q", a.Held(), a.Depth(), a.SessionID())
	}
	if err := b.Unlock(ctx); err != nil {
		t.Errorf("b.Unlock: %v", err)
	}
}

func TestWatchdogRenewsAndDetectsLoss(t *testing.T) {
	f := newFakeBackend()
	var reported atomic.Value
	m := newFakeMutex(t, f, "worker-1", lock.Config{
		TTL:           10 * time.Second,
		RenewInterval: -1, // 不起后台轮询, 由本用例显式 Tick, 使续期计数可断言
		OnRenewError:  func(err error) { reported.Store(err) },
	})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	before := f.count("RenewSession")
	if stop := m.wd.Tick(ctx); stop {
		t.Fatal("续期成功不应停止看门狗")
	}
	if after := f.count("RenewSession"); after != before+1 {
		t.Errorf("Tick 应恰好续期一次: %d -> %d", before, after)
	}

	// 会话被回收后续期必然失败:本地深度作废 + ErrLost + 停止轮询
	f.expireSession(m.SessionID())
	if stop := m.wd.Tick(ctx); !stop {
		t.Fatal("会话失效后看门狗应停止")
	}
	got, _ := reported.Load().(error)
	if got == nil || !errors.Is(got, lock.ErrLost) {
		t.Fatalf("OnRenewError 应收到包装 ErrLost 的错误, 实际 %v", got)
	}
	if m.Held() || m.Depth() != 0 {
		t.Errorf("属主已失后本地视图应复位, Held=%v Depth=%d", m.Held(), m.Depth())
	}
	// 本地已复位, 后续释放不再触碰后端
	if err := m.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("复位后 Unlock 应报 ErrNotHeld, 实际 %v", err)
	}
}

// TestUnlockWithRenewInFlightDoesNotDeadlock 与 redisstore 同名用例钉同一条不变式:
// wd.Stop 等待续期 goroutine 退出,绝不能在持有实例互斥锁时调用——renew 入口要同一把锁。
func TestUnlockWithRenewInFlightDoesNotDeadlock(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 10 * time.Second, RenewInterval: time.Microsecond})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			if err := m.TryLock(context.Background()); err != nil {
				t.Errorf("第 %d 次 TryLock: %v", i, err)
				return
			}
			time.Sleep(50 * time.Microsecond) // 给看门狗一次在飞的续期
			if err := m.Unlock(context.Background()); err != nil {
				t.Errorf("第 %d 次 Unlock: %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Unlock 未返回: 释放路径与看门狗续期互等(锁内调用 wd.Stop)")
	}
}

func TestFailClosedOnBackendError(t *testing.T) {
	cases := []struct {
		name   string
		inject func(f *fakeBackend)
	}{
		{"建会话失败", func(f *fakeBackend) { f.errCreate = errors.New("consul: session 创建失败") }},
		{"Acquire 失败", func(f *fakeBackend) { f.errAcquire = errors.New("consul: KV 写失败") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeBackend()
			tc.inject(f)
			m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second})
			ctx := context.Background()

			// 关键契约:后端故障绝不能被当作"获得锁"
			if err := m.TryLock(ctx); err == nil {
				t.Fatal("后端故障时 TryLock 不得返回 nil")
			} else if errors.Is(err, lock.ErrBusy) {
				t.Errorf("后端故障不应被误报为 ErrBusy: %v", err)
			}
			if m.Held() || m.Depth() != 0 {
				t.Error("后端故障时不得显示持有")
			}
			if err := m.Lock(ctx); err == nil {
				t.Error("后端故障时 Lock 不得返回 nil")
			}
		})
	}
}

func TestBackendPanicBecomesError(t *testing.T) {
	ops := []string{"CreateSession", "AcquireLock"}
	for _, op := range ops {
		f := newFakeBackend()
		f.panicOn = op
		m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second, WaitTimeout: 20 * time.Millisecond, PollInterval: time.Millisecond})
		ctx := context.Background()

		// panic 一律转 error, 调用方不能被库炸掉
		requireNoPanic(t, fmt.Sprintf("%s TryLock", op), func() error { return m.TryLock(ctx) })
		requireNoPanic(t, fmt.Sprintf("%s Lock", op), func() error { return m.Lock(ctx) })
	}
}

func requireNoPanic(t *testing.T, what string, fn func() error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s 处 panic 逃逸为 error: %v", what, r)
		}
	}()
	if err := fn(); err == nil {
		t.Fatalf("%s 故障应返回 error", what)
	} else if !strings.Contains(err.Error(), "panic") {
		t.Errorf("%s 故障应报为 panic 转成的 error, 实际 %v", what, err)
	}
}

func TestReleaseFailurePropagatesError(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second, RenewInterval: -1})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	f.errRelease = errors.New("consul: 释放不可用")
	if err := m.Unlock(ctx); err == nil || !strings.Contains(err.Error(), "释放失败") {
		t.Fatalf("释放故障应回传错误, 实际 %v", err)
	}
	// 后端故障时不轻信"已释放":本地状态保留, 由调用方决定后续处置
	if !m.Held() {
		t.Error("释放失败后不应谎报已释放")
	}

	f.errRelease = nil
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("重试 Unlock: %v", err)
	}
	if f.holder(m.KVKey()) != "" {
		t.Error("重试成功后 KV 应已释放")
	}
}

func TestDestroySessionFailureIsReportedButLockReleased(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 30 * time.Second, RenewInterval: -1})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	f.errDestroy = errors.New("consul: 会话销毁失败")
	err := m.Unlock(ctx)
	if err == nil || !strings.Contains(err.Error(), "会话销毁失败") {
		t.Fatalf("销毁失败应回传, 实际 %v", err)
	}
	if f.holder(m.KVKey()) != "" {
		t.Error("锁已释放的事实不应因销毁失败而改变")
	}
	if m.Held() || m.Depth() != 0 {
		t.Error("本地视图应已复位")
	}
}

func TestCloseIdempotentWithoutHold(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{})
	if err := m.Close(); err != nil {
		t.Fatalf("未持有时 Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close 应幂等: %v", err)
	}
	if got := f.count("ReleaseLock"); got != 0 {
		t.Errorf("未持有时 Close 不应发起 Release, 实际 %d 次", got)
	}
	if got := f.count("DestroySession"); got != 0 {
		t.Errorf("无会话时 Close 不应发起 Destroy, 实际 %d 次", got)
	}
	if err := m.Lock(context.Background()); !errors.Is(err, lock.ErrClosed) {
		t.Errorf("关闭后 Lock 应报 ErrClosed, 实际 %v", err)
	}
}

func TestUnlockWithCanceledCtxStillReleases(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{})
	if err := m.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 资源必须归还:调用方 ctx 已取消也不能成为锁泄漏的理由
	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("已取消 ctx 下 Unlock 仍应成功: %v", err)
	}
	if f.holder(m.KVKey()) != "" {
		t.Error("KV 应已释放")
	}
}

// TestRefreshWhenWatchdogDisabled 钉住 RenewInterval=-1 的手动续期路径:
// 看门狗已闭,长任务只能靠 Refresh 撑住会话;错误语义与自动续期一致。
func TestRefreshWhenWatchdogDisabled(t *testing.T) {
	f := newFakeBackend()
	m := newFakeMutex(t, f, "worker-1", lock.Config{TTL: 15 * time.Second, RenewInterval: -1})
	ctx := context.Background()

	if err := m.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := m.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := f.count("RenewSession"); got != 1 {
		t.Errorf("Refresh 应恰好续期一次, 实际 %d", got)
	}

	// 会话被回收:报 ErrLost 并作废本地深度
	f.expireSession(m.SessionID())
	if err := m.Refresh(ctx); !errors.Is(err, lock.ErrLost) {
		t.Errorf("会话失效后 Refresh 应报 ErrLost, 实际 %v", err)
	}
	if m.Held() || m.Depth() != 0 {
		t.Errorf("属主已失后本地视图应复位, Held=%v Depth=%d", m.Held(), m.Depth())
	}
	// 本地已复位:Refresh 只报未持有, 不再触碰后端
	before := f.count("RenewSession")
	if err := m.Refresh(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("未持有 Refresh 应报 ErrNotHeld, 实际 %v", err)
	}
	if got := f.count("RenewSession"); got != before {
		t.Errorf("未持有时不应发起续期请求, %d -> %d", before, got)
	}
}

func TestConcurrentReplicasExclusive(t *testing.T) {
	f := newFakeBackend()
	const (
		replicas = 3
		perInst  = 10
	)
	locks := make([]*Mutex, replicas)
	for i := range locks {
		// 不指定 Owner:默认按 goroutine 绑定, 同一实例上的并发才是真互斥
		locks[i] = newFakeMutex(t, f, "", lock.Config{
			TTL: 30 * time.Second, PollInterval: time.Millisecond, WaitTimeout: 10 * time.Second,
		})
	}

	var (
		inFlight  atomic.Int64
		maxActive atomic.Int64
		counter   int64
		wg        sync.WaitGroup
		errs      = make(chan error, replicas*perInst)
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
		t.Errorf("跨副本临界区并发数应为 1, 实际峰值 %d", got)
	}
	if got := f.liveSessions(); got != 0 {
		t.Errorf("全部释放后不应残留存活会话, 实际 %d 个", got)
	}
}

// TestScenarioShardingConcurrentDDL 用 consulstore 复现分片启动期建表竞争:
// 多个进程副本同时 CREATE TABLE IF NOT EXISTS, 建表只应执行一次。
func TestScenarioShardingConcurrentDDL(t *testing.T) {
	f := newFakeBackend()
	names := []string{"pod-1", "pod-2", "pod-3"}
	locks := make([]*Mutex, 0, len(names))
	for _, n := range names {
		locks = append(locks, newFakeMutex(t, f, n, lock.Config{
			Key:          "sharding:ddl:orders_202611",
			TTL:          30 * time.Second,
			PollInterval: time.Millisecond,
			WaitTimeout:  10 * time.Second,
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
		t.Fatalf("副本加锁失败: %v", err)
	}

	if got := ddlCalls.Load(); got != 1 {
		t.Errorf("CREATE TABLE 应只执行 1 次, 实际 %d", got)
	}
	if got := skipped.Load(); got != int64(len(names)-1) {
		t.Errorf("其余副本应走已存在分支, 期望 %d 实际 %d", len(names)-1, got)
	}
}

// TestScenarioDeadLetterMultiReplicaClaim 用 consulstore 复现 deadletter 多副本恢复:
// TryLock 让每轮只一个副本捞取, 其余直接跳过而非排队空等。
func TestScenarioDeadLetterMultiReplicaClaim(t *testing.T) {
	f := newFakeBackend()
	replicas := []string{"dlv1", "dlv2", "dlv3", "dlv4"}
	locks := make([]*Mutex, 0, len(replicas))
	for _, r := range replicas {
		locks = append(locks, newFakeMutex(t, f, r, lock.Config{
			Key: "deadletter:recover:order-pay",
			TTL: 30 * time.Second,
		}))
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
	if got := f.liveSessions(); got != 0 {
		t.Errorf("各轮结束后不应残留会话, 实际 %d 个", got)
	}
}
