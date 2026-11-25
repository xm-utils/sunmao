package redisstore_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/sunmao/lock"
	"github.com/xm-utils/sunmao/lock/redisstore"
)

const demoKey = "sharding:ddl:orders"

// hashKeyOf 锁状态 Hash 的服务端键名(前缀由后端拼接,{tag} 保证与栅栏 Key 同 slot)
func hashKeyOf(key string) string { return "xm_lock:{" + key + "}" }

// mustMutex 示例辅助:统一补 Owner,其余配置由调用方给定。
// 示例里把 RenewInterval 设为 -1 关掉看门狗,免得异步续期干扰可读性;
// 生产代码用默认值(TTL/3)即可,无需手动 Refresh。
func mustMutex(client redisstore.Client, owner string, cfg lock.Config) *redisstore.Mutex {
	cfg.Key = demoKey
	cfg.Owner = owner
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = -1
	}
	mu, err := redisstore.New(redisstore.Config{Client: client, Config: cfg})
	if err != nil {
		panic("示例配置非法: " + err.Error())
	}
	return mu
}

// newMiniredis 起一个内存 Redis 并交出对应客户端:示例无需真实 Redis,
// 但可以像 redis-cli 一样直接读服务端状态(HGet/Exists/FastForward)。
func newMiniredis() (*miniredis.Miniredis, *redis.Client) {
	mr, err := miniredis.Run()
	if err != nil {
		panic("启动内存 Redis 失败: " + err.Error())
	}
	return mr, redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

// acceptWrite 下游写入的门禁示意:只接受不低于当前栅栏值的持有者。
func acceptWrite(f lock.Fencer, want uint64) bool { return f.Fence() >= want }

// Example Redis 后端的多副本互斥:属主与深度存在服务端,因此两个进程、
// 两台机器上的实例抢的是同一把锁。这里用 miniredis 代替真实 Redis,便于直接读服务端状态。
func Example() {
	mr, client := newMiniredis()
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	a := mustMutex(client, "replica-a", lock.Config{TTL: 10 * time.Second})
	defer a.Close()
	b := mustMutex(client, "replica-b", lock.Config{TTL: 10 * time.Second})
	defer b.Close()

	fmt.Println("a 获取 err =", a.TryLock(ctx))
	fmt.Println("b 撞锁是 ErrBusy =", errors.Is(b.TryLock(ctx), lock.ErrBusy))
	fmt.Printf("服务端视图: owner=%s depth=%s\n",
		mr.HGet(hashKeyOf(demoKey), "owner"), mr.HGet(hashKeyOf(demoKey), "depth"))

	// 跨持有者释放被拒绝:非属主不能把别人的临界区提前打开
	fmt.Println("b 释放他人锁是 ErrNotHeld =", errors.Is(b.Unlock(ctx), lock.ErrNotHeld))

	fmt.Println("a 释放 err =", a.Unlock(ctx))
	fmt.Println("释放后服务端键已消失 =", !mr.Exists(hashKeyOf(demoKey)))
	fmt.Println("b 再获取 err =", b.TryLock(ctx))
	// Output:
	// a 获取 err = <nil>
	// b 撞锁是 ErrBusy = true
	// 服务端视图: owner=replica-a depth=1
	// b 释放他人锁是 ErrNotHeld = true
	// a 释放 err = <nil>
	// 释放后服务端键已消失 = true
	// b 再获取 err = <nil>
}

// ExampleMutex_Fence 栅栏值(fencing token)由服务端 INCR 得出,跨实例单调递增,
// 且独立于锁 Key 存在——释放锁不会让它退回旧值,旧持有者的迟到写入因此可被下游拒绝。
func ExampleMutex_Fence() {
	mr, client := newMiniredis()
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	a := mustMutex(client, "replica-a", lock.Config{TTL: 10 * time.Second})
	defer a.Close()
	b := mustMutex(client, "replica-b", lock.Config{TTL: 10 * time.Second})
	defer b.Close()

	_ = a.Lock(ctx)
	fmt.Println("a 本轮 fence =", a.Fence())
	_ = a.Lock(ctx) // 同 Owner 重入不推进栅栏
	fmt.Println("重入后栅栏不变 =", a.Fence() == 1, "Depth =", a.Depth())
	_ = a.Unlock(ctx)

	// 下游只需接受 lock.Fencer 契约,不必知道背后是 Redis 还是进程内 Local
	fmt.Println("可按 lock.Fencer 交付写入 =", acceptWrite(a, 1))

	_ = a.Unlock(ctx)
	_ = b.Lock(ctx)
	fmt.Println("换人后 b 的栅栏更大 =", b.Fence() > 1, "b.Fence =", b.Fence())
	fmt.Println("栅栏 Key 不随释放删除 =", mr.Exists(hashKeyOf(demoKey)+":fence"))
	fmt.Printf("旧持有者拿 fence=%d 再写,会被 fence=%d 的当前持有者拒掉\n", 1, b.Fence())
	// Output:
	// a 本轮 fence = 1
	// 重入后栅栏不变 = true Depth = 2
	// 可按 lock.Fencer 交付写入 = true
	// 换人后 b 的栅栏更大 = true b.Fence = 2
	// 栅栏 Key 不随释放删除 = true
	// 旧持有者拿 fence=1 再写,会被 fence=2 的当前持有者拒掉
}

// ExampleMutex_Refresh 关闭看门狗后必须自己续期:Refresh 把租期重置回 TTL;
// 一旦租期到点被他人抢占,下一次操作报 lock.ErrLost——意味着临界区可能已被并发执行。
func ExampleMutex_Refresh() {
	mr, client := newMiniredis()
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	a := mustMutex(client, "replica-a", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	defer a.Close()
	b := mustMutex(client, "replica-b", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	defer b.Close()

	if err := a.Lock(ctx); err != nil {
		fmt.Println("获取失败:", err)
		return
	}
	mr.FastForward(8 * time.Second) // 时间流逝,租期只剩 2s
	left, err := a.RemainingTTL(ctx)
	fmt.Println("临近到期仍能读到剩余租期 =", left > 0, "err =", err)
	fmt.Println("此时 b 仍抢不到 =", errors.Is(b.TryLock(ctx), lock.ErrBusy))

	if err := a.Refresh(ctx); err != nil { // 手动续期,租期回到 TTL
		fmt.Println("续期失败:", err)
		return
	}
	renewed, _ := a.RemainingTTL(ctx)
	fmt.Println("续期后租期回升 =", renewed > left)

	mr.FastForward(11 * time.Second) // 再无人续期,租期到点
	fmt.Println("过期后 b 可抢占 =", b.TryLock(ctx) == nil)
	fmt.Println("旧持有者释放报 ErrLost =", errors.Is(a.Unlock(ctx), lock.ErrLost))
	fmt.Println("快照 lost =", a.Stats().Lost)
	// Output:
	// 临近到期仍能读到剩余租期 = true err = <nil>
	// 此时 b 仍抢不到 = true
	// 续期后租期回升 = true
	// 过期后 b 可抢占 = true
	// 旧持有者释放报 ErrLost = true
	// 快照 lost = 1
}
