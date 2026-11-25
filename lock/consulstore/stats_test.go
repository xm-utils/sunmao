package consulstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xm-utils/sunmao/lock"
)

// TestStatsTracksBackendCounters 覆盖 Consul 后端的记账口径:
// 本地 busy 判定与服务端 busy 都计入 Busy;重入顺带的 RenewSession 计入 Renewals;
// 会话被回收后 Refresh 报 ErrLost 记 Lost 并作废持有计时;Fence 恒为 0(本后端无栅栏)。
func TestStatsTracksBackendCounters(t *testing.T) {
	f := newFakeBackend()
	ctx := context.Background()

	newMu := func(owner string, cfg lock.Config) *Mutex {
		cfg.Key = "test:lock"
		cfg.Owner = owner
		m, err := New(Config{Backend: f, Config: cfg})
		if err != nil {
			t.Fatalf("New(%s): %v", owner, err)
		}
		return m
	}

	a := newMu("replica-a", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	defer func() { _ = a.Close() }()
	b := newMu("replica-b", lock.Config{
		TTL: 10 * time.Second, WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond, RenewInterval: -1,
	})
	defer func() { _ = b.Close() }()

	if s := a.Stats(); s.Backend != "consul" || s.Key != "test:lock" || s.Attempts != 0 || s.Holding {
		t.Fatalf("初始快照不对: %+v", s)
	}

	// a 首次获得 + 重入(重入会顺带续期, 因此 Renewals 同步 +1)
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock: %v", err)
	}
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock 重入: %v", err)
	}
	s := a.Stats()
	if s.Attempts != 2 || s.Acquired != 1 || s.Reentered != 1 || s.Renewals != 1 {
		t.Errorf("a 获取阶段计数不对: %+v", s)
	}
	if !s.Holding || s.Depth != 2 || s.Fence != 0 {
		t.Errorf("a 持有视图不对(consulstore 无栅栏): %+v", s)
	}

	// b 撞锁 + 超时
	if err := b.TryLock(ctx); !errors.Is(err, lock.ErrBusy) {
		t.Fatalf("b.TryLock 应报 ErrBusy: %v", err)
	}
	if err := b.Lock(ctx); !errors.Is(err, lock.ErrTimeout) {
		t.Fatalf("b.Lock 应报 ErrTimeout: %v", err)
	}
	sb := b.Stats()
	if sb.Busy < 2 || sb.Timeouts != 1 || sb.Acquired != 0 || sb.Holding {
		t.Errorf("b 竞争阶段计数不对: %+v", sb)
	}
	if sb.WaitNanos < int64(30*time.Millisecond) {
		t.Errorf("等待时长应累计到超时刻, 实际 %v", time.Duration(sb.WaitNanos))
	}
	// 每轮尝试都建了一次会话:可用于评估 Raft 写放大
	if f.count("CreateSession") < 2 {
		t.Errorf("b 的等待轮次应留下建会话记录, 实际 %d", f.count("CreateSession"))
	}

	// a 出栈一层再完整释放
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a.Unlock 内层: %v", err)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a.Unlock 外层: %v", err)
	}
	s = a.Stats()
	if s.Decreased != 1 || s.Released != 1 || s.HoldNanos <= 0 || s.Holding {
		t.Errorf("释放阶段计数不对: %+v", s)
	}

	// 未持有时释放 → ReleaseMisses;会话被回收导致的丢锁 → Lost 且不计持有时长
	if err := a.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("未持有时 Unlock 应报 ErrNotHeld: %v", err)
	}
	if err := b.Lock(ctx); err != nil {
		t.Fatalf("b.Lock: %v", err)
	}
	held := b.Stats().HoldNanos
	f.expireSession(b.SessionID())
	if err := b.Refresh(ctx); !errors.Is(err, lock.ErrLost) {
		t.Fatalf("会话被回收后 Refresh 应报 ErrLost: %v", err)
	}
	if s = a.Stats(); s.ReleaseMisses != 1 {
		t.Errorf("a 的误释放应记 ReleaseMisses, 实际 %+v", s)
	}
	sc := b.Stats()
	if sc.Lost != 1 || sc.RenewFailures != 1 || sc.Holding {
		t.Errorf("b 丢锁后计数不对: %+v", sc)
	}
	if sc.Released != 0 || sc.HoldNanos != held {
		t.Errorf("被抢占的这轮不该结算持有时长: released=%d hold=%v(之前 %v)", sc.Released, time.Duration(sc.HoldNanos), time.Duration(held))
	}

	_ = b.Close()
	_ = a.Close()
	if s = a.Stats(); !s.Closed {
		t.Error("Close 后快照应 Closed=true")
	}
	var _ lock.Statser = a
}
