package consulstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	api "github.com/hashicorp/consul/api"

	"github.com/xm-utils/sunmao/lock"
	"github.com/xm-utils/sunmao/lock/consulstore"
)

// stubBackend Consul 能力集的内存实现,只为让示例不必起一个 Consul。
// 真实装配直接把 consul 组件的客户端(*consul.Client)交给 Backend 即可——
// 它的方法签名与本实现逐一对应(见 consulstore 文件末尾的编译期断言)。
// 自己实现这一层(按 namespace 包装、加本地缓存、做测试替身)同样是允许的。
type stubBackend struct {
	mu        sync.Mutex
	live      map[string]bool   // 存活的 Session
	owner     map[string]string // KV 键 → 占用它的 Session
	nextID    int
	Destroyed []string // 记录销毁顺序,便于示例里核对
}

func newStubBackend() *stubBackend {
	return &stubBackend{live: make(map[string]bool), owner: make(map[string]string)}
}

func (b *stubBackend) CreateSession(name string, ttl time.Duration) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := fmt.Sprintf("sess-%d", b.nextID)
	b.live[id] = true
	return id, nil
}

func (b *stubBackend) DestroySession(sessionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.live, sessionID)
	for key, owner := range b.owner {
		if owner == sessionID {
			delete(b.owner, key) // Consul 的行为:会话没了,它占的 KV 一并释放
		}
	}
	b.Destroyed = append(b.Destroyed, sessionID)
	return nil
}

func (b *stubBackend) RenewSession(sessionID string) (*api.SessionEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.live[sessionID] {
		return nil, fmt.Errorf("session %s 已被回收", sessionID)
	}
	return &api.SessionEntry{ID: sessionID, TTL: "10s"}, nil
}

func (b *stubBackend) AcquireLock(key, sessionID string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.live[sessionID] {
		return false, fmt.Errorf("session %s 已被回收", sessionID)
	}
	if cur, ok := b.owner[key]; ok && cur != sessionID {
		return false, nil // 他人持有,Consul 返回 CAS false
	}
	b.owner[key] = sessionID
	return true, nil
}

func (b *stubBackend) ReleaseLock(key, sessionID string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.owner[key]; !ok || cur != sessionID {
		return false, nil
	}
	delete(b.owner, key)
	return true, nil
}

// mustMutex 示例辅助:固定 Owner 便于跨副本叙述,关掉看门狗免得异步续期干扰输出。
func mustMutex(b consulstore.Backend, owner string, cfg lock.Config) *consulstore.Mutex {
	cfg.Key = "deadletter:recover:order-pay"
	cfg.Owner = owner
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = -1
	}
	mu, err := consulstore.New(consulstore.Config{Backend: b, Config: cfg})
	if err != nil {
		panic("示例配置非法: " + err.Error())
	}
	return mu
}

// Example Consul 后端的多副本互斥:谁持有锁取决于 Session 是否占着那个 KV,
// 因此持有者进程崩溃后会话 TTL 到点即被 Consul 自动回收,他人可抢占。
func Example() {
	backend := newStubBackend()
	ctx := context.Background()

	a := mustMutex(backend, "replica-a", lock.Config{TTL: 10 * time.Second})
	defer a.Close()
	b := mustMutex(backend, "replica-b", lock.Config{TTL: 10 * time.Second, WaitTimeout: 20 * time.Millisecond})
	defer b.Close()

	fmt.Println("a 获取 err =", a.TryLock(ctx))
	fmt.Println("a 的 KV 键 =", a.KVKey())
	fmt.Println("a 已建会话 =", a.SessionID() != "")
	fmt.Println("b 撞锁是 ErrBusy =", errors.Is(b.TryLock(ctx), lock.ErrBusy))
	fmt.Println("b 阻塞等到上限是 ErrTimeout =", errors.Is(b.Lock(ctx), lock.ErrTimeout))

	fmt.Println("b 释放他人锁是 ErrNotHeld =", errors.Is(b.Unlock(ctx), lock.ErrNotHeld))
	session := a.SessionID()
	fmt.Println("a 释放 err =", a.Unlock(ctx))
	fmt.Println("a 释放后会话已销毁 =", !backend.isLive(session))
	fmt.Println("此时 b 可获取 err =", b.TryLock(ctx))
	// Output:
	// a 获取 err = <nil>
	// a 的 KV 键 = xm_lock/deadletter:recover:order-pay
	// a 已建会话 = true
	// b 撞锁是 ErrBusy = true
	// b 阻塞等到上限是 ErrTimeout = true
	// b 释放他人锁是 ErrNotHeld = true
	// a 释放 err = <nil>
	// a 释放后会话已销毁 = true
	// 此时 b 可获取 err = <nil>
}

// ExampleMutex_Refresh 租约由 Session 表达:关闭看门狗后必须自己周期 Refresh。
// 会话被 Consul 回收(示例里手工 DestroySession 模拟 TTL 到期)即判为丢锁,
// 报 lock.ErrLost——临界区可能已被并发执行,调用方应放弃本轮而非重试补偿。
func ExampleMutex_Refresh() {
	backend := newStubBackend()
	ctx := context.Background()

	a := mustMutex(backend, "replica-a", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	defer a.Close()
	b := mustMutex(backend, "replica-b", lock.Config{TTL: 10 * time.Second, RenewInterval: -1})
	defer b.Close()

	if err := a.Lock(ctx); err != nil {
		fmt.Println("获取失败:", err)
		return
	}
	fmt.Println("手动续期 err =", a.Refresh(ctx))
	fmt.Println("续期期间 b 抢不到 =", errors.Is(b.TryLock(ctx), lock.ErrBusy))

	// 模拟 Consul 侧会话被回收:本地无从得知,只能从下一次操作的返回码判定
	_ = backend.DestroySession(a.SessionID())
	fmt.Println("会话被回收后 Refresh 报 ErrLost =", errors.Is(a.Refresh(ctx), lock.ErrLost))
	fmt.Println("本实例视图已作废, Held =", a.Held())
	fmt.Println("抢占者 b 获取 err =", b.TryLock(ctx))
	fmt.Println("旧持有者再释放不再影响新持有者, err 是 ErrNotHeld =",
		errors.Is(a.Unlock(ctx), lock.ErrNotHeld))
	fmt.Println("快照 lost =", a.Stats().Lost, "renew_failures =", a.Stats().RenewFailures)
	// Output:
	// 手动续期 err = <nil>
	// 续期期间 b 抢不到 = true
	// 会话被回收后 Refresh 报 ErrLost = true
	// 本实例视图已作废, Held = false
	// 抢占者 b 获取 err = <nil>
	// 旧持有者再释放不再影响新持有者, err 是 ErrNotHeld = true
	// 快照 lost = 1 renew_failures = 1
}

// Example_noFenceSupport 后端差异要在契约层可判定:consulstore 不透出 KV 的
// ModifyIndex,因此没有实现 lock.Fencer。按可选接口断言即可在不支持时退化处理,
// 需要栅栏 token 的场景换用 redisstore(装配代码不必改动业务逻辑)。
func Example_noFenceSupport() {
	backend := newStubBackend()
	mu := mustMutex(backend, "replica-a", lock.Config{TTL: 10 * time.Second})
	defer mu.Close()

	if err := mu.TryLock(context.Background()); err != nil {
		fmt.Println("获取失败:", err)
		return
	}
	// 能力探测:契约层用可选接口判断后端支持什么,不支持时退化处理而非猜字段
	_, isRefresher := any(mu).(lock.Refresher)
	_, isFencer := any(mu).(lock.Fencer)
	fmt.Println("实现 lock.Refresher(可手动续期) =", isRefresher)
	fmt.Println("实现 lock.Fencer(可取栅栏值) =", isFencer)
	fmt.Println("重入深度与后端标识仍可用 =", mu.Depth(), mu.Stats().Backend)
	// Output:
	// 实现 lock.Refresher(可手动续期) = true
	// 实现 lock.Fencer(可取栅栏值) = false
	// 重入深度与后端标识仍可用 = 1 consul
}

// isLive 报告会话是否仍在 Consul 侧存活(示例校验用:释放后应为 false)。
func (b *stubBackend) isLive(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.live[sessionID]
}
