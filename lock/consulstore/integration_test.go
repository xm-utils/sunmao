//go:build integration

// 真实 Consul 集成用例:go test -tags integration ./...
//
// 本文件只验内存替身(stubBackend/fakeBackend)兜不住的部分:
// 会话是否真随释放被销毁、KV 是否真被解除占用、Consul 抬升后的
// session_ttl_min 是否影响崩溃回收节奏、真实 Raft 写下的互斥判定。
//
// 实测到的三条服务端事实(Consul 1.22.7 + consul/api v1.24)已直接写进断言:
//   - KV Release 只解除会话绑定、不删键,释放后锁键以「Session 为空的墓碑」留下,
//     故不能用「键是否存在」判锁是否被持有;
//   - 会话被 TTL 回收(非显式释放)后,Consul 对该键施加 lock_delay(默认 15s),
//     期间他人 Acquire 返回 false——过期用例因此轮询等待而非立刻断言;
//   - Session().Renew 对已失效会话返回 (nil, nil) 而不是 error,
//     判失效统一走 itSessionGone,不能只看 err。
//
// 环境不可用时 t.Skip,不判失败:不带 integration 标签的 go test 不依赖外部 Consul。
//
// 环境变量:
//
//	XM_LOCK_TEST_CONSUL_ADDR  缺省 127.0.0.1:8500
//	XM_LOCK_TEST_CONSUL_TOKEN ACL Token,缺省空
//
// 注意耗时:过期与续期两条用例要跨过「会话实际生效 TTL」,而 Consul 默认把它
// 抬到 session_ttl_min=10s,单条约 12~15s。若 agent 的 session_ttl_min 大于
// XM_LOCK_TEST_CONSUL_MAXTTL(缺省 30s),这两条自动跳过并在日志里说明原因。
// 过期用例还要再等一个 lock_delay(缺省 15s,可用 XM_LOCK_TEST_CONSUL_LOCKDELAY 覆盖)。
package consulstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	api "github.com/hashicorp/consul/api"

	xmconsul "github.com/xm-utils/sunmao/consul"
	"github.com/xm-utils/sunmao/lock"
)

// itBackend 连接真实 Consul 并探活;不可用即 Skip。
// 组件的 NewClient 自带一次 /v1/agent/self 探活,连不上同样走 Skip 而非失败——
// 集成用例的约定是「环境缺失就跳过」,不能把没起 Consul 的机器判红。
// 返回组件客户端(它本身就是 Backend),另附原生 api.Client 便于直读 KV。
func itBackend(t *testing.T) (*xmconsul.Client, *api.Client) {
	t.Helper()
	addr := itEnv("XM_LOCK_TEST_CONSUL_ADDR", "127.0.0.1:8500")
	c, err := xmconsul.NewClient(&xmconsul.Config{
		Address: addr,
		Token:   os.Getenv("XM_LOCK_TEST_CONSUL_TOKEN"),
		Timeout: 5,
	})
	if err != nil {
		t.Skipf("未检测到可用 Consul(%s): %v;起一个 agent 再跑,或去掉 -tags integration", addr, err)
	}
	raw := c.GetConsulClient()
	leader, err := raw.Status().Leader()
	if err != nil || leader == "" {
		t.Skipf("未检测到可用 Consul(%s): err=%v leader=%q;起一个 agent 再跑,或去掉 -tags integration", addr, err, leader)
	}
	return c, raw
}

// itEnv 读环境变量,空值取缺省
func itEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// itEffectiveTTL 探出「会话实际生效的最小租期」:建一个 1s 会话并续期,
// 服务端返回的 TTL 就是被 session_ttl_min 抬高后的值。
// 探测完立刻销毁会话,不留垃圾。
func itEffectiveTTL(t *testing.T, backend Backend) time.Duration {
	t.Helper()
	session, err := backend.CreateSession("xmlock_it:probe", time.Second)
	if err != nil {
		t.Skipf("创建探测会话失败: %v", err)
	}
	t.Cleanup(func() { _ = backend.DestroySession(session) })
	entry, err := backend.RenewSession(session)
	if err != nil {
		t.Skipf("续期探测会话失败: %v", err)
	}
	ttl, err := parseConsulTTL(entry.TTL)
	if err != nil {
		t.Skipf("无法解析会话 TTL %q: %v", entry.TTL, err)
	}
	return ttl
}

// parseConsulTTL 解析 Consul 的 "10s"/"1m15s" 形态 TTL
func parseConsulTTL(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("空 TTL")
	}
	return time.ParseDuration(s)
}

// itMaxTTL 过期/续期两条用例能接受的最大会话租期,超过则跳过(避免 CI 长尾)
func itMaxTTL(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("XM_LOCK_TEST_CONSUL_MAXTTL"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("XM_LOCK_TEST_CONSUL_MAXTTL 应为秒数, 当前 %q", v)
		}
		return time.Duration(d) * time.Second
	}
	return 30 * time.Second
}

// itLockDelay 会话被 TTL 回收后 Consul 施加的交接延迟,缺省与服务端默认一致(15s)。
// 过期用例按它设轮询上限:agent 改过 session 默认锁延迟时,用 env 对齐即可。
func itLockDelay(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("XM_LOCK_TEST_CONSUL_LOCKDELAY"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("XM_LOCK_TEST_CONSUL_LOCKDELAY 应为秒数, 当前 %q", v)
		}
		return time.Duration(d) * time.Second
	}
	return 15 * time.Second
}

// itSessionGone 判断会话是否已失效:consul/api 对不存在/已销毁的会话返回 (nil, nil),
// 自实现的 Backend 也可能改回 error,两种形态都算已回收。
func itSessionGone(backend Backend, id string) bool {
	entry, err := backend.RenewSession(id)
	return err != nil || entry == nil
}

// itKVOf 直读锁键(仅用于核对服务端视图)
func itKVOf(t *testing.T, raw *api.Client, key string) *api.KVPair {
	t.Helper()
	pair, _, err := raw.KV().Get(key, nil)
	if err != nil {
		t.Fatalf("读取 KV %s 失败: %v", key, err)
	}
	return pair
}

// itCleanKV 删除用例留下的墓碑键:Release 不删键,不清就会攒垃圾
func itCleanKV(t *testing.T, raw *api.Client, key string) {
	t.Helper()
	if _, err := raw.KV().Delete(key, nil); err != nil {
		t.Logf("清理 KV %s 失败(不影响断言): %v", key, err)
	}
}

// itMutex 造一把属于本用例某副本的锁,并登记会话与 KV 的清理
func itMutex(t *testing.T, backend Backend, key, owner string, cfg lock.Config) *Mutex {
	t.Helper()
	c := cfg
	c.Key = key
	c.Owner = owner
	mu, err := New(Config{Backend: backend, Config: c})
	if err != nil {
		t.Fatalf("构造 consulstore.Mutex(%s) 失败: %v", owner, err)
	}
	t.Cleanup(func() {
		// Close:在持有时以记录属主归还 KV 并销毁会话,未持有时只停看门狗
		_ = mu.Close()
	})
	return mu
}

// TestIntegrationSessionLifecycle 会话与 KV 的真实生命周期:
// 持锁期间 KV 被本会话占住(他人会话抢不动),释放后会话销毁、KV 解除绑定。
// 注意 Release 不删键:锁键以「Session 为空的墓碑」留下,断言看的是 Session 字段。
func TestIntegrationSessionLifecycle(t *testing.T) {
	backend, raw := itBackend(t)
	key := "xmlock_it/" + t.Name()
	cfg := lock.Config{TTL: 15 * time.Second}
	a := itMutex(t, backend, key, "replica-a", cfg)
	b := itMutex(t, backend, key, "replica-b", cfg)
	kvKey := a.KVKey()
	t.Cleanup(func() { itCleanKV(t, raw, kvKey) })

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	if a.SessionID() == "" {
		t.Fatalf("获取后应已创建会话")
	}
	pair := itKVOf(t, raw, kvKey)
	if pair == nil {
		t.Fatalf("持锁期间 KV 应存在")
	}
	// 部分 Consul 版本/配置不在读响应里回 Session,只在能读到时期望一致
	if pair.Session != "" && pair.Session != a.SessionID() {
		t.Errorf("服务端报告的持有会话应为 %s, 得 %q", a.SessionID(), pair.Session)
	}
	// 服务端侧的归属证据:另一个会话抢不动这个键(CAS 语义)
	foreign, err := backend.CreateSession(kvKey+":foreign", 15*time.Second)
	if err != nil {
		t.Fatalf("创建旁路会话失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = backend.ReleaseLock(kvKey, foreign)
		_ = backend.DestroySession(foreign)
	})
	if ok, err := backend.AcquireLock(kvKey, foreign); err != nil || ok {
		t.Errorf("持锁期间旁路会话不应获得 KV, 得 (%v, %v)", ok, err)
	}
	if err := b.TryLock(context.Background()); !errors.Is(err, lock.ErrBusy) {
		t.Errorf("b 应撞到 ErrBusy, 得 %v", err)
	}
	if err := b.Unlock(context.Background()); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("b 释放他人锁应报 ErrNotHeld, 得 %v", err)
	}

	session := a.SessionID()
	if err := a.Unlock(context.Background()); err != nil {
		t.Fatalf("a 释放失败: %v", err)
	}
	if left := itKVOf(t, raw, kvKey); left != nil && left.Session != "" {
		t.Errorf("释放后 KV 不应再绑定会话, 得 session=%q", left.Session)
	}
	if !itSessionGone(backend, session) {
		t.Errorf("释放后会话 %s 应已失效(续期报错或返空条目), 却仍可续", session)
	}
	// 占用解除后,原先被拒的旁路会话现在能拿到(显式释放不触发 lock_delay)
	if ok, err := backend.AcquireLock(kvKey, foreign); err != nil || !ok {
		t.Fatalf("释放后旁路会话应能获得 KV, 得 (%v, %v)", ok, err)
	}
	if ok, err := backend.ReleaseLock(kvKey, foreign); err != nil || !ok {
		t.Fatalf("归还旁路会话占的 KV 失败, 得 (%v, %v)", ok, err)
	}
	if err := b.TryLock(context.Background()); err != nil {
		t.Errorf("释放后 b 应能获取: %v", err)
	}
}

// TestIntegrationExpiryPreemption 持有者不再续期时,会话到点被 Consul 回收:
// 旧持有者的操作报 ErrLost(consul/api 对失效会话给 (nil,nil),只看 error 会漏判);
// 他人可以抢占,但要再等一个 lock_delay(缺省 15s)——这是 Consul 防时钟偏移的设计。
func TestIntegrationExpiryPreemption(t *testing.T) {
	backend, raw := itBackend(t)
	effective := itEffectiveTTL(t, backend)
	delay := itLockDelay(t)
	if effective > itMaxTTL(t) {
		t.Skipf("会话生效租期 %v 超过可接受上限 %v, 跳过(把 agent 的 session_ttl_min 调小再跑)",
			effective, itMaxTTL(t))
	}
	key := "xmlock_it/" + t.Name()
	cfg := lock.Config{TTL: effective, RenewInterval: -1} // 关看门狗,让租期自然走完
	a := itMutex(t, backend, key, "replica-a", cfg)
	b := itMutex(t, backend, key, "replica-b", cfg)
	kvKey := a.KVKey()
	t.Cleanup(func() { itCleanKV(t, raw, kvKey) })

	if err := a.Lock(context.Background()); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	sid := a.SessionID()
	t.Logf("等待会话过期: 生效租期 %v, 过期后还要跨 lock_delay %v", effective, delay)
	time.Sleep(effective + 2*time.Second)

	// 服务端事实:会话已被回收,而 Renew 不报错、只返空条目——这正是
	// consulstore 要拿 entry==nil 判丢锁的原因(只看 error 会以为续期成功)。
	if !itSessionGone(backend, sid) {
		t.Errorf("会话 %s 应已随 TTL 失效, 续期却返回有效会话", sid)
	}

	// a 本地还以为持有:此时任何操作都应判为丢锁,而不是静默成功
	if err := a.Refresh(context.Background()); !errors.Is(err, lock.ErrLost) {
		t.Errorf("过期后 Refresh 应报 ErrLost, 得 %v", err)
	}
	if a.Held() || a.Depth() != 0 {
		t.Errorf("判为丢锁后本地视图应复位, Held=%v Depth=%d", a.Held(), a.Depth())
	}
	if err := a.Unlock(context.Background()); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("作废后重复释放应报 ErrNotHeld, 得 %v", err)
	}
	if a.Stats().Lost == 0 {
		t.Errorf("快照应记录丢锁, 得 %+v", a.Stats())
	}
	// 会话失效后 KV 仍在,但不再绑定会话(Behavior=release 的默认语义)
	if left := itKVOf(t, raw, kvKey); left != nil && left.Session != "" {
		t.Errorf("会话回收后 KV 不应再绑定会话, 得 session=%q", left.Session)
	}

	// lock_delay 期间拿不到、到点后必须能拿到:轮询而不是事前一刀切断言
	start := time.Now()
	deadline := start.Add(delay + 5*time.Second)
	for {
		err := b.TryLock(context.Background())
		if err == nil {
			t.Logf("b 在会话回收后 %v 拿到锁", time.Since(start).Round(100*time.Millisecond))
			break
		}
		if !errors.Is(err, lock.ErrBusy) {
			t.Fatalf("抢占应只撞 ErrBusy, 得 %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("等了 %v 仍未能抢占(lock_delay 值不匹配?可调 XM_LOCK_TEST_CONSUL_LOCKDELAY)", delay+5*time.Second)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := b.Unlock(context.Background()); err != nil {
		t.Errorf("b 收尾释放失败: %v", err)
	}
}

// TestIntegrationWatchdogHoldsLease 看门狗必须在真实 Raft 往返下顶住租期:
// 持有 2×生效租期 + 2s,期间他人始终抢不到,续期计数为零失败。
func TestIntegrationWatchdogHoldsLease(t *testing.T) {
	backend, raw := itBackend(t)
	effective := itEffectiveTTL(t, backend)
	if effective > itMaxTTL(t) {
		t.Skipf("会话生效租期 %v 超过可接受上限 %v, 跳过", effective, itMaxTTL(t))
	}
	key := "xmlock_it/" + t.Name()
	// 不给 RenewInterval,取默认 TTL/3
	a := itMutex(t, backend, key, "replica-a", lock.Config{TTL: effective})
	b := itMutex(t, backend, key, "replica-b", lock.Config{TTL: effective, RenewInterval: -1})
	t.Cleanup(func() { itCleanKV(t, raw, a.KVKey()) })

	if err := a.Lock(context.Background()); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	deadline := time.Now().Add(2*effective + 2*time.Second)
	for time.Now().Before(deadline) {
		if err := b.TryLock(context.Background()); !errors.Is(err, lock.ErrBusy) {
			t.Fatalf("持有期间 b 应始终撞锁, 得 %v", err)
		}
		if !a.Held() {
			t.Fatalf("本实例视图不应因续期失败而作废")
		}
		time.Sleep(time.Second)
	}
	st := a.Stats()
	if st.Renewals < 2 {
		t.Errorf("跨两个租期至少续期 2 次, 得 Renewals=%d", st.Renewals)
	}
	if st.RenewFailures != 0 {
		t.Errorf("不该有续期失败, 得 %d", st.RenewFailures)
	}
}

// TestIntegrationBackendRejectsForeignRelease 直接走 Backend 核对 Consul 侧事实:
// 别的会话 ReleaseLock 解不开本会话占的 KV(CAS 语义,不依赖我们的本地校验)。
func TestIntegrationBackendRejectsForeignRelease(t *testing.T) {
	backend, raw := itBackend(t)
	key := "xmlock_it/" + t.Name()
	mine, err := backend.CreateSession(key+":mine", 15*time.Second)
	if err != nil {
		t.Fatalf("创建会话失败: %v", err)
	}
	theirs, err := backend.CreateSession(key+":theirs", 15*time.Second)
	if err != nil {
		_ = backend.DestroySession(mine)
		t.Fatalf("创建会话失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = backend.ReleaseLock(key, mine)
		_ = backend.DestroySession(mine)
		_ = backend.DestroySession(theirs)
		itCleanKV(t, raw, key)
	})

	if ok, err := backend.AcquireLock(key, mine); err != nil || !ok {
		t.Fatalf("首轮 AcquireLock 应成功, 得 (%v, %v)", ok, err)
	}
	if ok, err := backend.AcquireLock(key, theirs); err != nil || ok {
		t.Errorf("他人 AcquireLock 应被拒, 得 (%v, %v)", ok, err)
	}
	if ok, err := backend.ReleaseLock(key, theirs); err != nil || ok {
		t.Errorf("非占用者 ReleaseLock 应返回 false, 得 (%v, %v)", ok, err)
	}
	if ok, err := backend.ReleaseLock(key, mine); err != nil || !ok {
		t.Errorf("占用者 ReleaseLock 应成功, 得 (%v, %v)", ok, err)
	}
}
