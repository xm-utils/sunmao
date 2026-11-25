package lock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecorderClassifiesOutcomes(t *testing.T) {
	var r Recorder

	r.NoteAttempt()
	r.NoteAcquired(true) // 首次获得
	r.NoteAttempt()
	r.NoteAcquired(false) // 重入
	r.NoteAttempt()
	r.NoteBusy()
	r.NoteFailure(fmt.Errorf("%w: 等待 %v 未获得锁", ErrTimeout, time.Second))
	r.NoteFailure(fmt.Errorf("%w: 属主已易主", ErrLost))
	r.NoteFailure(context.Canceled)
	r.NoteFailure(errors.New("redis: connection refused"))
	r.NoteFailure(fmt.Errorf("%w: key=k 正被持有", ErrBusy)) // 已由 NoteBusy 记过, 不得重复
	r.NoteReleased(false)
	r.NoteReleased(true)
	r.NoteReleaseRejected(fmt.Errorf("%w: 未被持有", ErrNotHeld))
	r.NoteRenew(nil)
	r.NoteRenew(errors.New("consul: session not found"))
	r.NoteWait(120 * time.Millisecond)

	s := r.Snapshot(Stats{Key: "k", Backend: "local"})
	checks := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"Attempts", s.Attempts, 3},
		{"Acquired", s.Acquired, 1},
		{"Reentered", s.Reentered, 1},
		{"Busy", s.Busy, 1},
		{"Timeouts", s.Timeouts, 1},
		{"Lost", s.Lost, 1},
		{"Canceled", s.Canceled, 1},
		{"Errors", s.Errors, 2}, // 后端故障两处: 获取一次 + 续期一次
		{"Released", s.Released, 1},
		{"Decreased", s.Decreased, 1},
		{"ReleaseMisses", s.ReleaseMisses, 1},
		{"Renewals", s.Renewals, 1},
		{"RenewFailures", s.RenewFailures, 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s 期望 %d, 实际 %d", c.name, c.want, c.got)
		}
	}
	if s.WaitNanos != int64(120*time.Millisecond) {
		t.Errorf("WaitNanos 期望 120ms, 实际 %v", time.Duration(s.WaitNanos))
	}
	if s.Key != "k" || s.Backend != "local" {
		t.Errorf("base 字段应保留, 实际 %q/%q", s.Key, s.Backend)
	}
	// 竞争率 = Busy / Attempts = 1/3
	if ratio := s.ContentionRatio(); ratio < 0.32 || ratio > 0.34 {
		t.Errorf("竞争率期望 ~0.333, 实际 %v", ratio)
	}
	if avg := s.AvgWait(); avg != 120*time.Millisecond {
		t.Errorf("平均等待期望 120ms, 实际 %v", avg)
	}
}

func TestRecorderHoldTimer(t *testing.T) {
	t.Run("完整持有按时长累计", func(t *testing.T) {
		var r Recorder
		r.NoteAcquired(true)
		time.Sleep(20 * time.Millisecond)
		r.NoteAcquired(false) // 重入不得重置计时起点
		time.Sleep(20 * time.Millisecond)
		r.NoteReleased(true)

		s := r.Snapshot(Stats{})
		if s.HoldNanos < int64(40*time.Millisecond) {
			t.Errorf("持有时长应 >= 40ms, 实际 %v", time.Duration(s.HoldNanos))
		}
		if avg := s.AvgHold(); avg < 40*time.Millisecond {
			t.Errorf("AvgHold 期望 >= 40ms, 实际 %v", avg)
		}
	})

	t.Run("丢锁后不作废则持有时长虚高", func(t *testing.T) {
		var r Recorder
		r.NoteAcquired(true)
		r.NoteRenew(fmt.Errorf("%w: 租期已过", ErrLost)) // 续期发现易主
		r.NoteReleased(true)                         // 之后的释放不应再累加时长

		s := r.Snapshot(Stats{})
		if s.Lost != 1 {
			t.Errorf("续期报 ErrLost 应计 Lost, 实际 %d", s.Lost)
		}
		if s.HoldNanos != 0 {
			t.Errorf("被抢占的这段时间不该算正常持有, HoldNanos=%v", time.Duration(s.HoldNanos))
		}
	})

	t.Run("负值与零值不产出误导均值", func(t *testing.T) {
		s := Stats{Released: 3, HoldNanos: -5, Acquired: 2, WaitNanos: -1}
		if s.AvgHold() != 0 || s.AvgWait() != 0 {
			t.Errorf("负时长应归零, 实际 wait=%v hold=%v", s.AvgWait(), s.AvgHold())
		}
	})
}

func TestStatsDerivedGuards(t *testing.T) {
	// 空快照:派生值一律 0, 不 panic、不 NaN
	var zero Stats
	if zero.ContentionRatio() != 0 || zero.AvgWait() != 0 || zero.AvgHold() != 0 {
		t.Error("零值快照的派生指标应为 0")
	}
	// 自定义 Recorder 交出 Busy > Attempts 时夹持, 比例不得越过 1
	var r Recorder
	r.NoteBusy()
	r.NoteBusy()
	s := r.Snapshot(Stats{Attempts: 0})
	if s.Busy != 2 {
		t.Fatalf("Busy 计数应为 2, 实际 %d", s.Busy)
	}
	if ratio := s.ContentionRatio(); ratio != 0 {
		t.Errorf("Attempts=0 时比例应为 0(而非除零), 实际 %v", ratio)
	}
	r.NoteAttempt()
	if ratio := r.Snapshot(Stats{}).ContentionRatio(); ratio > 1 {
		t.Errorf("比例上限应为 1, 实际 %v", ratio)
	}

	if got := (Stats{Key: "job:x", Backend: "redis", Attempts: 4, Busy: 2}).String(); !strings.Contains(got, "key=job:x") ||
		!strings.Contains(got, "backend=redis") || !strings.Contains(got, "contention=0.500") {
		t.Errorf("String 摘要应含关键字段, 实际 %q", got)
	}
}

func TestLocalStatsLifecycle(t *testing.T) {
	l, err := New(Config{Key: "stats:local", WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()
	ctx := context.Background()

	s := l.Stats()
	if s.Key != "stats:local" || s.Backend != "local" || s.Attempts != 0 || s.Holding {
		t.Fatalf("初始快照应为空计数, 实际 %+v", s)
	}

	// 同 goroutine 两层获取 + 两层释放
	if err := l.Lock(ctx); err != nil {
		t.Fatalf("外层 Lock: %v", err)
	}
	if err := l.Lock(ctx); err != nil {
		t.Fatalf("内层 Lock 应重入: %v", err)
	}
	if s = l.Stats(); s.Acquired != 1 || s.Reentered != 1 || s.Depth != 2 || !s.Holding || s.Fence != 1 {
		t.Errorf("两层持有后快照应 Acquired=1/Reentered=1/Depth=2/Holding/Fence=1, 实际 %+v", s)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("内层 Unlock: %v", err)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("外层 Unlock: %v", err)
	}
	if s = l.Stats(); s.Released != 1 || s.Decreased != 1 || s.HoldNanos <= 0 || s.Holding {
		t.Errorf("两层出栈后应 Released=1(仅深度归零那次)/Decreased=1/不再持有, 实际 %+v", s)
	}

	// 换一个 goroutine 持有:主 goroutine 的 TryLock 应记 busy, Unlock 记被拒
	held := make(chan error, 1)
	go func() { held <- l.TryLock(ctx) }()
	if err := <-held; err != nil {
		t.Fatalf("另一 goroutine TryLock: %v", err)
	}
	if err := l.TryLock(ctx); !errors.Is(err, ErrBusy) {
		t.Fatalf("主 goroutine 应报 ErrBusy, 实际 %v", err)
	}
	if err := l.Unlock(ctx); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("非属主释放应报 ErrNotHeld, 实际 %v", err)
	}
	s = l.Stats()
	if s.Busy != 1 || s.ReleaseMisses != 1 {
		t.Errorf("期望 Busy=1/ReleaseMisses=1, 实际 %+v", s)
	}
	// 等待超时:轮询若干轮后报 ErrTimeout, 只计一次 Timeouts
	if err := l.Lock(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Lock 应超时, 实际 %v", err)
	}
	s = l.Stats()
	if s.Timeouts != 1 {
		t.Errorf("期望 Timeouts=1, 实际 %d", s.Timeouts)
	}
	if s.Attempts < 3 {
		t.Errorf("轮询每轮都应计 Attempts, 实际 %d", s.Attempts)
	}

	// Close 归还他人持有的租约同样结账
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s = l.Stats()
	if !s.Closed || s.Released != 2 || s.Holding {
		t.Errorf("Close 后应 Closed=true 且把他人持有的那轮结账(Released=2), 实际 %+v", s)
	}
}

// TestStatsConcurrentReadsWithoutLockingRecorder 验证 Recorder 无锁:
// 记账发生在实例互斥锁临界区内, 若 Recorder 自己带锁就可能与 Stats() 互等。
func TestStatsConcurrentReadsWithoutLockingRecorder(t *testing.T) {
	l, err := New(Config{Key: "stats:race"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()
	ctx := context.Background()

	const goroutines, rounds = 8, 25
	var calls, ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				calls.Add(1)
				if err := l.TryLock(ctx); err == nil {
					ok.Add(1)
					_ = l.Stats() // 读侧与写侧并发
					_ = l.Unlock(ctx)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			_ = l.Stats()
		}
		close(done)
	}()
	wg.Wait()
	<-done

	s := l.Stats()
	if s.Attempts != uint64(goroutines*rounds) {
		t.Errorf("每次 TryLock 都应记一轮尝试: 期望 %d, 实际 %d", goroutines*rounds, s.Attempts)
	}
	if s.Acquired+s.Busy != s.Attempts {
		t.Errorf("获得与撞锁之和应等于尝试总数: acquired=%d busy=%d attempts=%d", s.Acquired, s.Busy, s.Attempts)
	}
	if s.Acquired != uint64(ok.Load()) {
		t.Errorf("Acquired 应与成功次数一致: 快照 %d, 实际 %d", s.Acquired, ok.Load())
	}
}

// TestStatserImplemented 契约层自检:三端都满足 Statser(断言也在各后端文件里)。
func TestStatserImplemented(t *testing.T) {
	l, err := New(Config{Key: "stats:contract"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()
	var s Statser = l
	if got := s.Stats(); got.Backend != "local" {
		t.Errorf("Backend 应为 local, 实际 %q", got.Backend)
	}
}
