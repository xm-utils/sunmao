// Package lock_test 从包外使用者视角演示 lock 的用法。
//
// 每个示例都带 // Output: 注释,go test 会把它们当测试用例执行并比对标准输出,
// 因此文档与行为同源:API 变了示例就会失败,不会留下过期写法。
// 示例一律用进程内 Local(零依赖);跨进程场景见 redisstore/consulstore 同名示例文件。
package lock_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xm-utils/sunmao/lock"
)

// newLocal 示例辅助:示例正文只留用法主干,构造失败交给测试框架暴露。
// 生产代码请按 err != nil 分支处理,不要照抄这里的 panic。
func newLocal(cfg lock.Config) *lock.Local {
	mu, err := lock.New(cfg)
	if err != nil {
		panic("示例配置非法: " + err.Error())
	}
	return mu
}

// Example 最小闭环:Lock 进入临界区,Unlock 归还。
// Unlock 即使 ctx 已取消也会执行释放——资源必须归还,不因取消而泄漏。
func Example() {
	mu := newLocal(lock.Config{Key: "sharding:ddl:orders"})
	defer mu.Close()

	ctx := context.Background()
	if err := mu.Lock(ctx); err != nil {
		fmt.Println("获取失败:", err)
		return
	}
	fmt.Println("临界区内 Held =", mu.Held())
	if err := mu.Unlock(ctx); err != nil {
		fmt.Println("释放失败:", err)
		return
	}
	fmt.Println("释放后 Held =", mu.Held(), "Depth =", mu.Depth())
	// Output:
	// 临界区内 Held = true
	// 释放后 Held = false Depth = 0
}

// ExampleTryLock 定时任务的「抢不到就跳过本轮」:TryLock 不排队,撞锁报 ErrBusy。
// 多副本 ticker 用它替代 lock.Do(Do 内部是 Lock,会排队而非跳过)。
func ExampleLocal_TryLock() {
	mu := newLocal(lock.Config{Key: "deadletter:recover:order-pay"})
	defer mu.Close()

	// 模拟另一副本已在处理:另一个 goroutine 拿到锁后不释放
	occupied := make(chan struct{})
	go func() {
		_ = mu.Lock(context.Background())
		close(occupied)
	}() // 它的持有由 defer mu.Close() 兜底归还

	<-occupied
	rounds := 0
	skipped := 0
	for i := 0; i < 3; i++ {
		rounds++
		err := mu.TryLock(context.Background())
		if errors.Is(err, lock.ErrBusy) {
			skipped++ // 本轮跳过,不阻塞 ticker
			continue
		}
		if err != nil {
			fmt.Println("真正的故障要单独处理:", err)
			break
		}
		_ = mu.Unlock(context.Background())
	}
	fmt.Printf("跑了 %d 轮,跳过 %d 轮\n", rounds, skipped)
	// Output:
	// 跑了 3 轮,跳过 3 轮
}

// Example_reentrant 默认 Owner 按 goroutine 绑定:同 goroutine 递归加锁只加深深度,
// 必须成对 Unlock,最后一层才真正释放。
func ExampleLocal_reentrant() {
	mu := newLocal(lock.Config{Key: "sharding:ddl:orders"})
	defer mu.Close()
	ctx := context.Background()

	_ = mu.Lock(ctx)
	_ = mu.Lock(ctx) // 递归进入同一临界区
	fmt.Println("递归后 Depth =", mu.Depth())

	_ = mu.Unlock(ctx) // 出一层,锁仍在手上
	fmt.Println("单层释放后 Depth =", mu.Depth(), "Held =", mu.Held())

	_ = mu.Unlock(ctx) // 深度归零才真正释放
	fmt.Println("全部释放后 Depth =", mu.Depth(), "Held =", mu.Held())
	// Output:
	// 递归后 Depth = 2
	// 单层释放后 Depth = 1 Held = true
	// 全部释放后 Depth = 0 Held = false
}

// Example_owner 显式固定 Owner 后按「同 Owner 即同持有者」处理,不再做 goroutine 隔离。
// 跨实例共享同一 Owner 才有意义(Local 的互斥范围只限同一实例,跨实例请用 redisstore)。
func ExampleLocal_ownerStrategy() {
	mu := newLocal(lock.Config{Key: "job:cleanup", Owner: "order-svc-3"})
	defer mu.Close()
	ctx := context.Background()

	_ = mu.Lock(ctx)
	inner := make(chan error, 1)
	go func() { inner <- mu.TryLock(ctx) }()
	fmt.Println("同 Owner 跨 goroutine 视作重入, err =", <-inner)
	fmt.Println("Depth =", mu.Depth())

	_ = mu.Unlock(ctx)
	_ = mu.Unlock(ctx)
	// Output:
	// 同 Owner 跨 goroutine 视作重入, err = <nil>
	// Depth = 2
}

// ExampleDo 把「获取-执行-释放」收敛成一次调用:错误分支与 panic 都不会把锁漏在半路。
func ExampleDo() {
	mu := newLocal(lock.Config{Key: "job:daily-report"})
	defer mu.Close()
	ctx := context.Background()

	err := lock.Do(ctx, mu, func() error {
		fmt.Println("Do 内部已持锁, Held =", mu.Held())
		return nil
	})
	fmt.Println("err =", err, "返回后 Held =", mu.Held())

	// 业务错误原样返回(内部用 errors.Join 合并释放错误,不覆盖业务错误)
	bizErr := errors.New("下游超时")
	err = lock.Do(ctx, mu, func() error { return bizErr })
	fmt.Println("业务错误保留 =", errors.Is(err, bizErr), "Held =", mu.Held())

	// 临界区 panic:先归还锁,再把 panic 原样抛给调用方
	func() {
		defer func() {
			fmt.Println("panic 已抛出 =", recover() != nil, "Held =", mu.Held())
		}()
		_ = lock.Do(ctx, mu, func() error { panic("boom") })
	}()
	// Output:
	// Do 内部已持锁, Held = true
	// err = <nil> 返回后 Held = false
	// 业务错误保留 = true Held = false
	// panic 已抛出 = true Held = false
}

// Example_waitTimeout WaitTimeout 到点仍未拿到锁 → 包装 ErrTimeout,而不是无限等待。
func ExampleLocal_waitTimeout() {
	mu := newLocal(lock.Config{
		Key: "job:report", TTL: 5 * time.Second,
		WaitTimeout: 30 * time.Millisecond, PollInterval: time.Millisecond,
	})
	defer mu.Close()
	ctx := context.Background()

	held := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		_ = mu.Lock(ctx)
		close(held)
		<-stop
		_ = mu.Unlock(ctx)
	}()
	<-held

	err := mu.Lock(ctx)
	fmt.Println("等到上限报 ErrTimeout =", errors.Is(err, lock.ErrTimeout))
	fmt.Println("快照里 timeouts 累计 =", mu.Stats().Timeouts)
	close(stop)
	// Output:
	// 等到上限报 ErrTimeout = true
	// 快照里 timeouts 累计 = 1
}

// Example_contextCanceled 父 ctx 自身取消/超时时错误原样透传,不伪装成 ErrTimeout——
// 调用方可以区分「我没等到」与「没等到」。
func ExampleLocal_contextCanceled() {
	mu := newLocal(lock.Config{Key: "job:report"})
	defer mu.Close()
	ctx := context.Background()

	held := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		_ = mu.Lock(ctx)
		close(held)
		<-stop
		_ = mu.Unlock(ctx)
	}()
	<-held

	cctx, cancel := context.WithCancel(ctx)
	cancel() // 已取消的 ctx 会让等待立即终止
	err := mu.Lock(cctx)
	s := mu.Stats()
	fmt.Println("透传 context.Canceled =", errors.Is(err, context.Canceled))
	fmt.Println("计入 canceled 而非 timeouts =", s.Canceled == 1 && s.Timeouts == 0)
	close(stop)
	// Output:
	// 透传 context.Canceled = true
	// 计入 canceled 而非 timeouts = true
}

// Example_fencer 栅栏值(fencing token)每轮首次获取单调递增,重入不变,空闲为 0。
// 把它随写入一起下发,下游即可拒掉旧持有者的迟到写入。
func ExampleLocal_Fence() {
	mu := newLocal(lock.Config{Key: "sharding:ddl:orders"})
	defer mu.Close()
	ctx := context.Background()

	fmt.Println("未持有时 Fence =", mu.Fence())
	_ = mu.Lock(ctx)
	first := mu.Fence()
	_ = mu.Lock(ctx)
	fmt.Println("重入不推进栅栏 =", first == mu.Fence())
	_ = mu.Unlock(ctx)
	_ = mu.Unlock(ctx)

	_ = mu.Lock(ctx)
	second := mu.Fence()
	fmt.Println("新一轮严格递增 =", second > first)
	fmt.Printf("写入携带 fence=%d\n", second)
	_ = mu.Unlock(ctx)
	fmt.Println("释放后归零 =", mu.Fence())
	// Output:
	// 未持有时 Fence = 0
	// 重入不推进栅栏 = true
	// 新一轮严格递增 = true
	// 写入携带 fence=2
	// 释放后归零 = 0
}

// Example_closed Close 会归还本实例仍持有的锁;之后再使用报 ErrClosed。
func ExampleLocal_Close() {
	mu := newLocal(lock.Config{Key: "job:one-shot"})
	ctx := context.Background()

	_ = mu.Lock(ctx)
	if err := mu.Close(); err != nil {
		fmt.Println("Close:", err)
		return
	}
	fmt.Println("Close 顺带释放, Held =", mu.Held())
	fmt.Println("关闭后 Lock 报 ErrClosed =", errors.Is(mu.Lock(ctx), lock.ErrClosed))
	fmt.Println("重复 Close 幂等, err =", mu.Close())
	// Output:
	// Close 顺带释放, Held = false
	// 关闭后 Lock 报 ErrClosed = true
	// 重复 Close 幂等, err = <nil>
}

// Example_stats 拉取式快照:计数字段自实例创建起累计,适合自检与日志巡检。
// 时长类字段(AvgWait/AvgHold)依赖真实时钟,示例不做断言。
func ExampleLocal_Stats() {
	mu := newLocal(lock.Config{Key: "deadletter:recover:order-pay"})
	ctx := context.Background()

	_ = mu.Lock(ctx)
	_ = mu.Lock(ctx) // 重入
	_ = mu.Unlock(ctx)
	_ = mu.Unlock(ctx) // 深度归零,完整释放
	_ = mu.TryLock(ctx)
	_ = mu.Unlock(ctx)
	_ = mu.Unlock(ctx) // 未持有就释放 → 记一次被拒

	s := mu.Stats()
	fmt.Printf("backend=%s attempts=%d acquired=%d reentered=%d\n", s.Backend, s.Attempts, s.Acquired, s.Reentered)
	fmt.Printf("released=%d decreased=%d release_misses=%d busy=%d\n", s.Released, s.Decreased, s.ReleaseMisses, s.Busy)
	fmt.Println("争用率 =", s.ContentionRatio())
	fmt.Println("已复位为未持有 =", !s.Holding && s.Depth == 0 && s.Fence == 0)
	_ = mu.Close()
	// Output:
	// backend=local attempts=3 acquired=2 reentered=1
	// released=2 decreased=1 release_misses=1 busy=0
	// 争用率 = 0
	// 已复位为未持有 = true
}

// customStatser 自定义后端复用 lock.Recorder 后即可实现 Statser(示意用)。
type customStatser struct{}

func (customStatser) Stats() lock.Stats {
	var rec lock.Recorder
	rec.NoteAttempt()
	rec.NoteBusy()
	rec.NoteAttempt()
	rec.NoteAcquired(true)
	return rec.Snapshot(lock.Stats{Key: "sharding:ddl:users", Backend: "custom"})
}

// Example_statser 装配层只依赖 lock.Statser 契约取数,不关心后端是哪一个实现;
// metrics/adapter/lock 也用同样的方式接收实例或自定义快照源。
func ExampleStatser() {
	local := newLocal(lock.Config{Key: "sharding:ddl:orders"})
	defer local.Close()
	_ = local.Lock(context.Background())

	registry := map[string]lock.Statser{
		"sharding:ddl:orders": local,
		"sharding:ddl:users":  customStatser{},
	}
	for _, key := range []string{"sharding:ddl:orders", "sharding:ddl:users"} {
		s := registry[key].Stats()
		fmt.Printf("%s[%s] attempts=%d busy=%d holding=%v fence=%d\n",
			s.Key, s.Backend, s.Attempts, s.Busy, s.Holding, s.Fence)
	}
	// Output:
	// sharding:ddl:orders[local] attempts=1 busy=0 holding=true fence=1
	// sharding:ddl:users[custom] attempts=2 busy=1 holding=false fence=0
}
