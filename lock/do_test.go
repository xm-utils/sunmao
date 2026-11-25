package lock

import (
	"context"
	"errors"
	"testing"
)

// stubMutex 只按脚本回放返回值, 用于钉住 Do 的错误合并与调用时序
type stubMutex struct {
	lockErr   error
	tryErr    error
	unlockErr error
	locks     int
	unlocks   int
}

func (s *stubMutex) Lock(context.Context) error    { s.locks++; return s.lockErr }
func (s *stubMutex) TryLock(context.Context) error { return s.tryErr }
func (s *stubMutex) Unlock(context.Context) error  { s.unlocks++; return s.unlockErr }
func (s *stubMutex) Close() error                  { return nil }

var _ Mutex = (*stubMutex)(nil)

func TestDoValidatesArgs(t *testing.T) {
	cases := []struct {
		name string
		mu   Mutex
		fn   func() error
	}{
		{"mu 为 nil", nil, func() error { return nil }},
		{"fn 为 nil", &stubMutex{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := Do(context.Background(), tc.mu, tc.fn); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("应报 ErrInvalidConfig, 实际 %v", err)
			}
		})
	}
}

func TestDoRunsInsideCriticalSectionAndReleases(t *testing.T) {
	mu, err := New(Config{Key: "job:report"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer mu.Close()
	ctx := context.Background()

	var heldDuring bool
	var depthDuring int
	if err := Do(ctx, mu, func() error {
		heldDuring = mu.Held()
		depthDuring = mu.Depth()
		return nil
	}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !heldDuring || depthDuring != 1 {
		t.Errorf("临界区内应 Held=true/Depth=1, 实际 %v/%d", heldDuring, depthDuring)
	}
	if mu.Held() || mu.Depth() != 0 {
		t.Errorf("Do 返回后应已释放, Held=%v Depth=%d", mu.Held(), mu.Depth())
	}
	// 释放后其他 goroutine 能立刻拿到, 不必等租期
	other := make(chan error, 1)
	go func() { other <- mu.TryLock(ctx) }()
	if err := <-other; err != nil {
		t.Errorf("Do 之后其他 goroutine 应能获得锁: %v", err)
	}
}

func TestDoAcquireFailureSkipsFn(t *testing.T) {
	sentinel := errors.New("后端不可达")
	stub := &stubMutex{lockErr: sentinel}
	ran := false
	err := Do(context.Background(), stub, func() error { ran = true; return nil })
	if ran {
		t.Error("获取失败时不得执行 fn")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("应原样返回获取错误, 实际 %v", err)
	}
	if stub.unlocks != 0 {
		t.Errorf("获取失败不应发起释放, unlocks=%d", stub.unlocks)
	}

	// ErrTimeout 之类的哨兵同样可被调用方 errors.Is 分出
	tmo := &stubMutex{lockErr: context.DeadlineExceeded}
	if err := Do(context.Background(), tmo, func() error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx 超时错误应透传, 实际 %v", err)
	}
}

func TestDoFnErrorIsNotMaskedByUnlockError(t *testing.T) {
	fnErr := errors.New("业务失败")
	unlockErr := errors.New("释放失败")
	stub := &stubMutex{unlockErr: unlockErr}

	err := Do(context.Background(), stub, func() error { return fnErr })
	if !errors.Is(err, fnErr) {
		t.Fatalf("业务错误必须保留在错误链里, 实际 %v", err)
	}
	if !errors.Is(err, unlockErr) {
		t.Errorf("释放错误应一并回传(errors.Join), 实际 %v", err)
	}
	if stub.unlocks != 1 {
		t.Errorf("fn 报错后仍要释放一次, unlocks=%d", stub.unlocks)
	}

	// fn 成功时, 释放错误就是唯一返回值
	onlyUnlock := &stubMutex{unlockErr: unlockErr}
	if err := Do(context.Background(), onlyUnlock, func() error { return nil }); !errors.Is(err, unlockErr) {
		t.Errorf("fn 成功时应返回释放错误, 实际 %v", err)
	}
}

func TestDoReleasesOnPanic(t *testing.T) {
	mu, err := New(Config{Key: "job:panic"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer mu.Close()

	boom := errors.New("临界区炸了")
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("调用方自己的 panic 应原样抛回, 不被 Do 吞掉")
			}
		}()
		_ = Do(context.Background(), mu, func() error { panic(boom) })
	}()

	if mu.Held() || mu.Depth() != 0 {
		t.Errorf("panic 路径也必须归还锁, Held=%v Depth=%d", mu.Held(), mu.Depth())
	}
	if err := mu.TryLock(context.Background()); err != nil {
		t.Errorf("panic 后他人应能获得锁: %v", err)
	}
}

func TestDoNestedIsReentrant(t *testing.T) {
	mu, err := New(Config{Key: "job:nested"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer mu.Close()
	ctx := context.Background()

	var innerDepth int
	var depthAfterInner int
	err = Do(ctx, mu, func() error {
		// 同 goroutine 嵌套:内层 Do 直接重入, 不能自锁
		if err := Do(ctx, mu, func() error {
			innerDepth = mu.Depth()
			return nil
		}); err != nil {
			return err
		}
		depthAfterInner = mu.Depth() // 内层只减一层, 外层仍持有
		return nil
	})
	if err != nil {
		t.Fatalf("嵌套 Do: %v", err)
	}
	if innerDepth != 2 {
		t.Errorf("内层深度应为 2, 实际 %d", innerDepth)
	}
	if depthAfterInner != 1 {
		t.Errorf("内层出栈后外层应仍持有(深度 1), 实际 %d", depthAfterInner)
	}
	if mu.Depth() != 0 {
		t.Errorf("两层都出栈后深度应归零, 实际 %d", mu.Depth())
	}
}
