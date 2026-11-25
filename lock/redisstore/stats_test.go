package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xm-utils/sunmao/lock"
)

// TestStatsTracksBackendCounters 覆盖 Redis 后端的记账口径:
// 轮询每轮计 Attempts/Busy、重入计 Reentered、手动续期与丢锁分别落 Renewals/
// RenewFailures+Lost, 以及「本地已作废后释放记 ReleaseMisses」。
func TestStatsTracksBackendCounters(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	a := h.newMutex(t, "replica-a", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	b := h.newMutex(t, "replica-b", lock.Config{
		TTL: 10 * time.Second, WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond, RenewInterval: -1,
	})

	if s := a.Stats(); s.Key != "test:lock" || s.Backend != "redis" || s.TTL != 10*time.Second {
		t.Fatalf("初始快照的标识字段不对: %+v", s)
	}

	// a 首次获得 + 同 Owner 重入
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock: %v", err)
	}
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock 重入: %v", err)
	}
	s := a.Stats()
	if s.Attempts != 2 || s.Acquired != 1 || s.Reentered != 1 || s.Busy != 0 {
		t.Errorf("a 获取阶段计数不对: %+v", s)
	}
	if !s.Holding || s.Depth != 2 || s.Fence != 1 {
		t.Errorf("a 持有视图不对: holding=%v depth=%d fence=%d", s.Holding, s.Depth, s.Fence)
	}

	// b 撞锁 + 等待超时:busy 每轮各计一次, 超时只在终止那次计一次
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
	if sb.ContentionRatio() <= 0 {
		t.Errorf("b 全程撞锁, 竞争率应大于 0, 实际 %v", sb.ContentionRatio())
	}

	// 手动续期成功
	if err := a.Refresh(ctx); err != nil {
		t.Fatalf("a.Refresh: %v", err)
	}
	if s = a.Stats(); s.Renewals != 1 || s.RenewFailures != 0 {
		t.Errorf("续期成功应记 Renewals=1: %+v", s)
	}

	// 篡改属主后续期 → ErrLost:续期失败 + 丢锁 + 本地视图作废
	h.mr.HSet("xm_lock:{test:lock}", "owner", "thief")
	if err := a.Refresh(ctx); !errors.Is(err, lock.ErrLost) {
		t.Fatalf("属主被篡改后 Refresh 应报 ErrLost: %v", err)
	}
	s = a.Stats()
	if s.RenewFailures != 1 || s.Lost != 1 || s.Holding || s.Depth != 0 {
		t.Errorf("丢锁后计数与视图不对: %+v", s)
	}
	// 本地已不持有, 再释放记为被拒而非成功
	if err := a.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("丢锁后 Unlock 应报 ErrNotHeld: %v", err)
	}
	if s = a.Stats(); s.ReleaseMisses != 1 || s.Released != 0 {
		t.Errorf("未持有时的释放应记 ReleaseMisses: %+v", s)
	}

	// 干净的一轮:成功获取 → 完整释放, 持有时长与释放计数结账
	h.mr.Del("xm_lock:{test:lock}")
	if err := b.Lock(ctx); err != nil {
		t.Fatalf("b.Lock(清理后): %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := b.Unlock(ctx); err != nil {
		t.Fatalf("b.Unlock: %v", err)
	}
	sb = b.Stats()
	if sb.Acquired != 1 || sb.Released != 1 || sb.HoldNanos <= 0 || sb.Closed {
		t.Errorf("完整一轮后计数不对: %+v", sb)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("b.Close: %v", err)
	}
	if s = b.Stats(); !s.Closed {
		t.Error("Close 后快照应 Closed=true")
	}
	// 契约自检
	var _ lock.Statser = a
}
