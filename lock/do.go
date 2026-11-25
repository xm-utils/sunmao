package lock

import (
	"context"
	"errors"
	"fmt"
)

// Do 获取锁、执行 fn、再释放锁,把「谁加锁谁解锁」收敛成一次调用,
// 消除最常见的泄漏形态——错误分支忘了 Unlock。
//
// 语义:
//   - 获取失败(Lock 返回 error)不执行 fn,错误原样返回,调用方仍可
//     errors.Is 分出 ErrTimeout / ctx 取消 / 后端故障;
//   - fn 的错误就是 Do 的错误;释放锁的错误在 fn 已成功时直接返回,
//     fn 已失败时用 errors.Join 合并,绝不把业务错误覆盖成释放错误;
//   - fn panic 时先归还锁再把 panic 原样抛回:这里 panic 的是调用方自己的
//     临界区代码,不是后端异常,资源归还优先——组件「对外 API 不 panic」的约定
//     针对的是锁自身的故障路径,不承诺吞掉用户函数的 panic。
//
// 重入安全:同一 goroutine 里嵌套 Do 只是加深一层深度,内层 Do 释放后
// 外层仍持有(见各后端 Unlock 的深度语义)。
//
// 需要「抢不到就本轮跳过」的定时任务不要用它——那种语义该用 TryLock
// 配合 errors.Is(err, ErrBusy) 判分支,Do 会排队等待而非跳过。
func Do(ctx context.Context, mu Mutex, fn func() error) error {
	if mu == nil {
		return fmt.Errorf("%w: mu 不能为 nil", ErrInvalidConfig)
	}
	if fn == nil {
		return fmt.Errorf("%w: fn 不能为 nil", ErrInvalidConfig)
	}
	if err := mu.Lock(ctx); err != nil {
		return err
	}

	var fnErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				// 临界区炸了也要把锁还回去,否则他人得等满租期
				_ = mu.Unlock(ctx)
				panic(r)
			}
		}()
		fnErr = fn()
	}()

	unlockErr := mu.Unlock(ctx)
	if fnErr != nil {
		return errors.Join(fnErr, unlockErr)
	}
	return unlockErr
}
