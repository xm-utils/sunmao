//go:build integration

// 真实 Redis 集成用例:go test -tags integration ./...
//
// 与默认用例的分工:miniredis 覆盖全部判定分支与确定性时序(单测与示例),
// 本文件只验那些「内存服务器兜不住」的东西——真服务端对 Lua/PEXPIRE/{tag}
// 的处理、看门狗在真实 RTT 下的续期节奏、租期到点后的真实抢占与 ErrLost。
//
// 环境不可用时逐条 t.Skip 并给出补救提示,不判失败——仓库约定是「不带标签的
// go test 永远绿」,CI 没有 Redis 也不会被这些用例拖红。
//
// 环境变量:
//
//	XM_LOCK_TEST_REDIS_ADDR            单机地址,缺省 127.0.0.1:6379
//	XM_LOCK_TEST_REDIS_PASSWORD        密码,缺省空
//	XM_LOCK_TEST_REDIS_DB              库号,缺省 0
//	XM_LOCK_TEST_REDIS_CLUSTER_ADDRS   Cluster 节点(逗号分隔),空则跳过 Cluster 用例
//
// 用例只在自建键(xmlock_it:*)上动作,结束时删除,不碰库里的其他键。
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/sunmao/lock"
)

// itHashKey/itFenceKey 与 New 里的构造口径一致({tag} 保证同 slot)
func itHashKey(key string) string  { return "xm_lock:{" + key + "}" }
func itFenceKey(key string) string { return itHashKey(key) + ":fence" }

// itClient 连真实 Redis 并 PING 探活;连不上即 Skip。
// 关闭动作挂在 t.Cleanup 上,失败路径也不会遗留连接。
func itClient(t *testing.T) *redis.Client {
	t.Helper()
	var db int
	if v := os.Getenv("XM_LOCK_TEST_REDIS_DB"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &db); err != nil {
			t.Fatalf("XM_LOCK_TEST_REDIS_DB 不是整数: %q", v)
		}
	}
	c := redis.NewClient(&redis.Options{
		Addr:     itEnv("XM_LOCK_TEST_REDIS_ADDR", "127.0.0.1:6379"),
		Password: os.Getenv("XM_LOCK_TEST_REDIS_PASSWORD"),
		DB:       db,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		t.Skipf("未检测到可用 Redis(%s): %v;起一个再跑,或去掉 -tags integration", c.Options().Addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// itEnv 读环境变量,空值取缺省
func itEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// itMutexAt 在指定逻辑键上造一把属于某副本的锁,并登记键清理。
// cfg 由调用方给全(TTL/RenewInterval),这里只钉 Key/Owner。
func itMutexAt(t *testing.T, client *redis.Client, key, owner string, cfg lock.Config) *Mutex {
	t.Helper()
	c := cfg
	c.Key = key
	c.Owner = owner
	mu, err := New(Config{Client: client, Config: c})
	if err != nil {
		t.Fatalf("构造 redisstore.Mutex(%s) 失败: %v", owner, err)
	}
	// 先记下键名:Close 之后本地字段仍可读,但清理不依赖它更稳
	hashKey, fenceKey := mu.hashKey, mu.fenceKey
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Del(ctx, hashKey, fenceKey).Err()
	})
	return mu
}

// itPair 造出同一逻辑键下的两把锁(模拟两个副本),返回键名便于直读服务端
func itPair(t *testing.T, client *redis.Client, cfg lock.Config) (a, b *Mutex, key string) {
	t.Helper()
	key = "xmlock_it:" + strings.ReplaceAll(t.Name(), "/", ":")
	return itMutexAt(t, client, key, "replica-a", cfg), itMutexAt(t, client, key, "replica-b", cfg), key
}

// TestIntegrationExclusiveAndRelease 真服务端上的互斥与归还:
// 属主/深度落在 Hash 里(与 miniredis 视图同构),非持有者释放被拒。
func TestIntegrationExclusiveAndRelease(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	a, b, key := itPair(t, client, lock.Config{TTL: 10 * time.Second, RenewInterval: -1})

	if err := a.TryLock(ctx); err != nil {
		t.Fatalf("a 首次获取应成功: %v", err)
	}
	if err := b.TryLock(ctx); !errors.Is(err, lock.ErrBusy) {
		t.Errorf("b 应撞到 ErrBusy, 得 %v", err)
	}
	// 服务端是唯一事实来源:owner/depth 必须能从 Redis 直接读到
	if got := client.HGet(ctx, itHashKey(key), "owner").Val(); got != "replica-a" {
		t.Errorf("服务端 owner 期望 replica-a, 得 %q", got)
	}
	if got := client.HGet(ctx, itHashKey(key), "depth").Val(); got != "1" {
		t.Errorf("服务端 depth 期望 1, 得 %q", got)
	}
	if err := b.Unlock(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("b 释放他人锁应报 ErrNotHeld, 得 %v", err)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a 释放应成功: %v", err)
	}
	if client.Exists(ctx, itHashKey(key)).Val() != 0 {
		t.Errorf("彻底释放后锁 Hash 应已删除")
	}
	if err := b.TryLock(ctx); err != nil {
		t.Errorf("释放后 b 应能获取: %v", err)
	}
}

// TestIntegrationRealTTLExpiryAndLost 租期真的到点时会发生什么:
// 持有者还活着但没人续期 → 他人可抢占,旧持有者的操作报 ErrLost。
// miniredis 靠 FastForward 拨时钟,这里用真实时钟,确认 PEXPIRE 的语义没被我们误读。
func TestIntegrationRealTTLExpiryAndLost(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	a, b, _ := itPair(t, client, lock.Config{TTL: time.Second, RenewInterval: -1})

	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	time.Sleep(1300 * time.Millisecond) // 超过 TTL,服务端已过期

	if left, err := a.RemainingTTL(ctx); err != nil || left != 0 {
		t.Errorf("过期后 RemainingTTL 期望 (0, nil), 得 (%v, %v)", left, err)
	}
	if err := b.TryLock(ctx); err != nil {
		t.Fatalf("过期后 b 应能抢占: %v", err)
	}
	// a 本地还以为持有:释放必须报 ErrLost(而非 ErrNotHeld),意味着临界区可能已被并发执行
	if err := a.Unlock(ctx); !errors.Is(err, lock.ErrLost) {
		t.Errorf("旧持有者释放应报 ErrLost, 得 %v", err)
	}
	st := a.Stats()
	if st.Lost != 1 {
		t.Errorf("快照 Lost 期望 1, 得 %d", st.Lost)
	}
	if st.HoldNanos != 0 {
		t.Errorf("丢锁那一轮不该计入持有时长, HoldNanos 期望 0, 得 %d", st.HoldNanos)
	}
	if err := b.Unlock(ctx); err != nil {
		t.Errorf("b 收尾释放失败: %v", err)
	}
}

// TestIntegrationWatchdogKeepsLease 看门狗在真实 RTT 下必须顶住租期:
// TTL 2s 的锁持有 5s(跨约 7 个续期周期)不应被抢占,续期计数落在快照里。
func TestIntegrationWatchdogKeepsLease(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	// 不显式给 RenewInterval,取默认 TTL/3 ≈ 666ms
	a, b, _ := itPair(t, client, lock.Config{TTL: 2 * time.Second})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := b.TryLock(ctx); !errors.Is(err, lock.ErrBusy) {
			t.Fatalf("持有期间 b 应始终撞锁, 得 %v", err)
		}
		if left, err := a.RemainingTTL(ctx); err != nil || left <= 0 {
			t.Fatalf("续期后剩余租期异常: (%v, %v)", left, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	st := a.Stats()
	if st.Renewals < 3 {
		t.Errorf("5s 内至少完成 3 次续期, 得 Renewals=%d", st.Renewals)
	}
	if st.RenewFailures != 0 {
		t.Errorf("不该有续期失败, 得 %d", st.RenewFailures)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a 释放失败: %v", err)
	}
	if err := b.TryLock(ctx); err != nil {
		t.Errorf("释放后 b 应能获取: %v", err)
	}
}

// TestIntegrationFenceMonotonicAcrossCycles 栅栏值跨轮次、跨实例单调递增:
// 这是「旧持有者的迟到写入被下游拒绝」的前提,必须由服务端 INCR 而非本地计数保证。
func TestIntegrationFenceMonotonicAcrossCycles(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	var last uint64
	for round := 1; round <= 3; round++ {
		a, b, key := itPair(t, client, lock.Config{TTL: 5 * time.Second, RenewInterval: -1})
		if err := a.Lock(ctx); err != nil {
			t.Fatalf("第 %d 轮 a 获取失败: %v", round, err)
		}
		fence := a.Fence()
		if fence <= last {
			t.Fatalf("第 %d 轮栅栏 %d 未超过上一轮 %d", round, fence, last)
		}
		if err := a.Unlock(ctx); err != nil {
			t.Fatalf("第 %d 轮 a 释放失败: %v", round, err)
		}
		if err := b.Lock(ctx); err != nil {
			t.Fatalf("第 %d 轮 b 获取失败: %v", round, err)
		}
		if b.Fence() <= fence {
			t.Fatalf("第 %d 轮换人后栅栏应继续增长, b=%d 而上一持有者=%d", round, b.Fence(), fence)
		}
		// 释放锁不回收栅栏计数:否则旧持有者的迟到写入会被误接受
		if client.Exists(ctx, itFenceKey(key)).Val() == 0 {
			t.Fatalf("第 %d 轮: 栅栏 Key 应独立于锁 Key 存在", round)
		}
		last = b.Fence()
		if err := b.Unlock(ctx); err != nil {
			t.Fatalf("第 %d 轮 b 释放失败: %v", round, err)
		}
	}
}

// TestIntegrationConcurrentHoldersSerialised 多副本抢同一把锁的真实互斥性:
// 临界区里「读计数 → 睡眠 → 写回」不是原子的,只有锁真互斥才不会串台。
func TestIntegrationConcurrentHoldersSerialised(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	key := "xmlock_it:" + t.Name()

	var inside, maxInside, done int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 每个副本一把实例级 Mutex:Owner 不同,互相不可重入
			lm := itMutexAt(t, client, key, fmt.Sprintf("worker-%d", i), lock.Config{
				TTL: 10 * time.Second, RenewInterval: -1, WaitTimeout: 30 * time.Second,
			})
			// lock.Do 保证释放:错误分支与 panic 都不会把锁漏在半路
			err := lock.Do(ctx, lm, func() error {
				cur := atomic.AddInt32(&inside, 1)
				// 无锁记录峰值并发:CAS 回环,失败就重读
				for {
					old := atomic.LoadInt32(&maxInside)
					if cur <= old || atomic.CompareAndSwapInt32(&maxInside, old, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&inside, -1)
				atomic.AddInt32(&done, 1)
				return nil
			})
			if err != nil {
				t.Errorf("worker-%d 执行失败: %v", i, err)
			}
			_ = lm.Close()
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxInside); got != 1 {
		t.Errorf("临界区最大并发期望 1, 得 %d(互斥失效)", got)
	}
	if got := atomic.LoadInt32(&done); got != 8 {
		t.Errorf("8 个副本应全部完成, 得 %d", got)
	}
}

// TestIntegrationDoReleasesOnPanic panic 穿过 lock.Do 时锁必须归还服务端,
// 否则一次崩溃会让整条 DDL 流程等到租期自然到期。
func TestIntegrationDoReleasesOnPanic(t *testing.T) {
	client := itClient(t)
	ctx := context.Background()
	a, b, key := itPair(t, client, lock.Config{TTL: 30 * time.Second, RenewInterval: -1})

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("lock.Do 应把业务 panic 原样抛出")
			}
		}()
		_ = lock.Do(ctx, a, func() error { panic("boom") })
	}()

	if client.Exists(ctx, itHashKey(key)).Val() != 0 {
		t.Errorf("panic 后锁应已归还服务端")
	}
	if err := b.TryLock(ctx); err != nil {
		t.Errorf("panic 后其他副本应能拿锁: %v", err)
	}
}

// TestIntegrationClusterScriptsAndSameSlot Cluster 部署下的关键承诺:
// 锁 Hash 与栅栏 Key 同用 {tag},一轮脚本不会跨槽(CROSSSLOT 会直接失败)。
// 未配 XM_LOCK_TEST_REDIS_CLUSTER_ADDRS 时跳过。
func TestIntegrationClusterScriptsAndSameSlot(t *testing.T) {
	addrs := os.Getenv("XM_LOCK_TEST_REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("未设置 XM_LOCK_TEST_REDIS_CLUSTER_ADDRS, 跳过 Cluster 用例")
	}
	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(addrs, ","),
		Password: os.Getenv("XM_LOCK_TEST_REDIS_PASSWORD"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cc.Ping(ctx).Err(); err != nil {
		_ = cc.Close()
		t.Skipf("Cluster 不可用(%s): %v", addrs, err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	key := "xmlock_it:" + t.Name()
	hashKey, fenceKey := itHashKey(key), itFenceKey(key)
	slot := func(k string) int64 {
		v, err := cc.Do(ctx, redis.NewCmd(ctx, "cluster", "keyslot", k)).Int64()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %s 失败: %v", k, err)
		}
		return v
	}
	if hs, fs := slot(hashKey), slot(fenceKey); hs != fs {
		t.Fatalf("{tag} 未生效: 锁 Hash 槽 %d 与栅栏槽 %d 不同, 脚本必然 CROSSSLOT", hs, fs)
	}

	mu, err := New(Config{Client: cc, Config: lock.Config{
		Key: key, Owner: "cluster-a", TTL: 5 * time.Second, RenewInterval: -1,
	}})
	if err != nil {
		t.Fatalf("Cluster 下构造失败: %v", err)
	}
	t.Cleanup(func() {
		_ = mu.Close()
		_ = cc.Del(context.Background(), hashKey, fenceKey).Err()
	})

	if err := mu.Lock(ctx); err != nil {
		t.Fatalf("Cluster 下获取失败(EVALSHA 跨槽即在此暴露): %v", err)
	}
	if mu.Fence() == 0 {
		t.Errorf("Cluster 下栅栏值应由服务端 INCR 得出, 得 0")
	}
	if left, err := mu.RemainingTTL(ctx); err != nil || left <= 0 {
		t.Errorf("Cluster 下 RemainingTTL 应可读, 得 (%v, %v)", left, err)
	}
	if err := mu.Unlock(ctx); err != nil {
		t.Fatalf("Cluster 下释放失败: %v", err)
	}
	if cc.Exists(ctx, hashKey).Val() != 0 {
		t.Errorf("Cluster 释放后锁 Hash 应已删除")
	}
}
