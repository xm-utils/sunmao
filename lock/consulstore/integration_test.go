//go:build integration

// 真实 Consul 集成用例:go test -tags integration ./...
//
// 本文件只验内存替身(stubBackend/fakeBackend)兜不住的部分:
// 会话是否真随释放被销毁、KV 是否真被解除占用、Consul 抬升后的
// session_ttl_min 是否影响崩溃回收节奏、真实 Raft 写下的互斥判定。
//
// 环境不可用时 t.Skip,不判失败(仓库约定:不带标签的 go test 永远绿)。
//
// 环境变量:
//
//	XM_LOCK_TEST_CONSUL_ADDR  缺省 127.0.0.1:8500
//	XM_LOCK_TEST_CONSUL_TOKEN ACL Token,缺省空
//
// 注意耗时:过期与续期两条用例要跨过「会话实际生效 TTL」,而 Consul 默认把它
// 抬到 session_ttl_min=10s,单条约 12~15s。若 agent 的 session_ttl_min 大于
// XM_LOCK_TEST_CONSUL_MAXTTL(缺省 30s),这两条自动跳过并在日志里说明原因。
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
// 持锁期间 KV 被本会话占住(他人会话既抢不到也解不开),释放后占用解除、会话销毁。
func TestIntegrationSessionLifecycle(t *testing.T) {
	backend, raw := itBackend(t)
	key := "xmlock_it/" + t.Name()
	cfg := lock.Config{TTL: 15 * time.Second}
	a := itMutex(t, backend, key, "replica-a", cfg)
	b := itMutex(t, backend, key, "replica-b", cfg)

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	if a.SessionID() == "" {
		t.Fatalf("获取后应已创建会话")
	}
	pair, _, err := raw.KV().Get(a.KVKey(), nil)
	if err != nil {
		t.Fatalf("读取 KV 失败: %v", err)
	}
	if pair == nil {
		t.Fatalf("持锁期间 KV 应存在")
	}
	// 服务端侧的归属证据:另一个会话抢不动这个键(CAS 语义)
	foreign, err := backend.CreateSession(a.KVKey()+":foreign", 15*time.Second)
	if err != nil {
		t.Fatalf("创建旁路会话失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = backend.ReleaseLock(a.KVKey(), foreign)
		_ = backend.DestroySession(foreign)
	})
	if ok, err := backend.AcquireLock(a.KVKey(), foreign); err != nil || ok {
		t.Errorf("持锁期间旁路会话不应获得 KV, 得 (%v, %v)", ok, err)
	}
	// 部分 Consul 版本/配置不在读响应里回 Session,只在能读到时期望一致
	if pair.Session != "" && pair.Session != a.SessionID() {
		t.Errorf("服务端报告的持有会话应为 %s, 得 %q", a.SessionID(), pair.Session)
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
	if pair, _, err := raw.KV().Get(a.KVKey(), nil); err != nil || pair != nil {
		t.Errorf("释放后 KV 应已不存在, 得 pair=%v err=%v", pair, err)
	}
	if _, err := backend.RenewSession(session); err == nil {
		t.Errorf("释放后会话 %s 应已销毁, 续期却仍成功", session)
	}
	// 占用解除后,原先被拒的旁路会话现在能拿到
	if ok, err := backend.AcquireLock(a.KVKey(), foreign); err != nil || !ok {
		t.Errorf("释放后旁路会话应能获得 KV, 得 (%v, %v)", ok, err)
	}
	if err := b.TryLock(context.Background()); err != nil {
		t.Errorf("释放后 b 应能获取: %v", err)
	}
}

// TestIntegrationExpiryPreemption 持有者不再续期时,会话到点被 Consul 回收:
// 他人可抢占,旧持有者的操作报 ErrLost。等待时长按实际生效租期计算,
// 因为 agent 会把我们传的 TTL 抬到 session_ttl_min。
func TestIntegrationExpiryPreemption(t *testing.T) {
	backend, _ := itBackend(t)
	effective := itEffectiveTTL(t, backend)
	if effective > itMaxTTL(t) {
		t.Skipf("会话生效租期 %v 超过可接受上限 %v, 跳过(把 agent 的 session_ttl_min 调小再跑)",
			effective, itMaxTTL(t))
	}
	key := "xmlock_it/" + t.Name()
	cfg := lock.Config{TTL: effective, RenewInterval: -1} // 关看门狗,让租期自然走完
	a := itMutex(t, backend, key, "replica-a", cfg)
	b := itMutex(t, backend, key, "replica-b", cfg)

	if err := a.Lock(context.Background()); err != nil {
		t.Fatalf("a 获取失败: %v", err)
	}
	t.Logf("等待会话过期: 生效租期 %v", effective)
	time.Sleep(effective + 3*time.Second)

	if err := b.TryLock(context.Background()); err != nil {
		t.Fatalf("会话回收后 b 应能抢占: %v", err)
	}
	// a 本地还以为持有:此时任何操作都应判为丢锁,而不是静默成功
	if err := a.Refresh(context.Background()); !errors.Is(err, lock.ErrLost) {
		t.Errorf("过期后 Refresh 应报 ErrLost, 得 %v", err)
	}
	if err := a.Unlock(context.Background()); !errors.Is(err, lock.ErrNotHeld) {
		t.Errorf("作废后重复释放应报 ErrNotHeld, 得 %v", err)
	}
	if a.Stats().Lost == 0 {
		t.Errorf("快照应记录丢锁, 得 %+v", a.Stats())
	}
}

// TestIntegrationWatchdogHoldsLease 看门狗必须在真实 Raft 往返下顶住租期:
// 持有 2×生效租期 + 2s,期间他人始终抢不到,续期计数为零失败。
func TestIntegrationWatchdogHoldsLease(t *testing.T) {
	backend, _ := itBackend(t)
	effective := itEffectiveTTL(t, backend)
	if effective > itMaxTTL(t) {
		t.Skipf("会话生效租期 %v 超过可接受上限 %v, 跳过", effective, itMaxTTL(t))
	}
	key := "xmlock_it/" + t.Name()
	// 不给 RenewInterval,取默认 TTL/3
	a := itMutex(t, backend, key, "replica-a", lock.Config{TTL: effective})
	b := itMutex(t, backend, key, "replica-b", lock.Config{TTL: effective, RenewInterval: -1})

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
	backend, _ := itBackend(t)
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
