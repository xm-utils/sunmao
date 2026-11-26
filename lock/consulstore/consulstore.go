// Package consulstore 提供 lock 的 Consul 后端(ROADMAP 3.3.1):
// Session + KV 锁——以 Session 存活作为租约,持有者进程崩溃后会话 TTL 到期,
// Consul 自动释放其占用的 KV,他人即可抢占。
//
// 复用仓库 consul 组件的方式:Backend 接口的方法集与 *consul.Client 完全一致
// (见文件末尾的编译期断言),直接把组件客户端传入即可;抽象出一层接口是为
// 让测试无需真实 Consul 也能覆盖全部判定分支。
//
// 与 redisstore 的两处语义差异(其余契约完全相同):
//
//   - 重入深度由本地计数决定,不下发服务端:Consul 的属主是 Session,
//     同 Session 重复 Acquire 本就幂等,再由本地 Owner 校验拦住跨 goroutine 误重入;
//   - 不实现 lock.Fencer:consul 组件的 AcquireLock 未透出 KV 的 ModifyIndex,
//     需要栅栏 token 的场景请用 redisstore。
//
// 租期以 Session TTL 表达,但 Consul 服务端会把它抬到 session_ttl_min(默认 10s):
// 把 TTL 配得比 10s 更小不会加快崩溃后的回收,真要更快需调 Consul 服务端参数。
// 等待期间的每轮尝试都会创建并销毁一份会话(一次 Raft 写),分钟级等待请上调 PollInterval。
//
// 真实服务端的另两条行为(Consul 1.22 实测,集成用例有覆盖):
//
//   - 正常 Unlock 走 KV Release,只解除 Session 绑定、不删键:锁键会作为墓碑留下,
//     值仍是上一个持有者的会话 ID(便于排障)。判断「是否被持有」要看 KV 的 Session
//     字段,不能看键是否存在;显式释放后他人可立刻接手,不受下述延迟约束;
//   - 持有者崩溃后会话被 TTL 回收时,Consul 对该键施加会话的 lock_delay(默认 15s),
//     期间他人 Acquire 一律返回 false——崩溃后的实际交接耗时约 TTL + 15s,
//     等待方要按这个量级设 WaitTimeout,不能只看 TTL。
package consulstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	api "github.com/hashicorp/consul/api"

	xmconsul "github.com/xm-utils/sunmao/consul"
	"github.com/xm-utils/sunmao/lock"
)

// defaultPrefix KV 键前缀;Consul 以 "/" 分层,前缀便于在 UI/CLI 里按目录聚合查看
const defaultPrefix = "xm_lock"

// Backend lock 所需的最小 Consul 能力集,由 consul 组件 Client 提供:
//   - CreateSession/DestroySession/RenewSession:租约生命周期
//   - AcquireLock/ReleaseLock:带 Session 的 KV 独占写与释放
//
// 方法签名与 *consul.Client 逐一对应,故可直接传入组件客户端实例;
// 自实现(如按 namespace 包装、测试替身)亦可行。
type Backend interface {
	// CreateSession 创建 TTL 会话,返回 sessionID
	CreateSession(name string, ttl time.Duration) (string, error)
	// DestroySession 销毁会话(其占用的 KV 一并释放)
	DestroySession(sessionID string) error
	// RenewSession 续期会话。会话已失效时真实 consul/api 返回 (nil, nil) 而非 error,
	// 故本包把 entry==nil 同样当作丢锁信号;自实现的 Backend 返回 error 亦可。
	RenewSession(sessionID string) (*api.SessionEntry, error)
	// AcquireLock 以 session 身份写锁 Key,返回是否获得
	AcquireLock(key string, sessionID string) (bool, error)
	// ReleaseLock 释放本 session 持有的锁 Key,返回是否释放成功
	ReleaseLock(key string, sessionID string) (bool, error)
}

// Config Consul 后端锁配置。内嵌的 lock.Config 字段与本结构同层参与
// yaml 反序列化,键位见 ../config.example.yaml。
type Config struct {
	// Backend Consul 能力集,通常直接传 *consul.Client,必填
	Backend Backend
	// Prefix KV 键前缀,空值取 "xm_lock";实际键为 <Prefix>/<Key>
	Prefix string
	lock.Config
}

// Mutex Consul 后端分布式锁,实现 lock.Mutex 与 lock.Holder。
// 会话在首次获取时惰性创建、彻底释放或 Close 时销毁,避免遗留空会话占用 TTL 配额。
type Mutex struct {
	backend Backend
	cfg     lock.Config
	kvKey   string
	rec     lock.Recorder // 运行计数,供 Stats() 拉取

	wd *lock.Watchdog

	mu        sync.Mutex
	sessionID string // 本轮持有的会话,空表示未创建/已销毁
	heldOwner string
	depth     int
	closed    bool
}

var (
	_ lock.Mutex     = (*Mutex)(nil)
	_ lock.Holder    = (*Mutex)(nil)
	_ lock.Refresher = (*Mutex)(nil)
	_ lock.Statser   = (*Mutex)(nil)
	// 不实现 lock.Fencer:consul 组件未透出 KV 的 ModifyIndex,无单调栅栏值可用
	// (Stats().Fence 因此恒为 0)
)

// New 创建 Consul 锁;配置非法返回包装 lock.ErrInvalidConfig 的 error,不 panic
func New(cfg Config) (*Mutex, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("%w: Backend 不能为 nil(可直接传 *consul.Client)", lock.ErrInvalidConfig)
	}
	c := cfg.Config
	if err := c.Normalize(); err != nil {
		return nil, err
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}

	m := &Mutex{
		backend: cfg.Backend,
		cfg:     c,
		kvKey:   prefix + "/" + c.Key,
	}
	wd, err := lock.NewWatchdog(&m.cfg, m.renew)
	if err != nil {
		return nil, err
	}
	m.wd = wd
	return m, nil
}

// Key 返回锁标识
func (m *Mutex) Key() string { return m.cfg.Key }

// KVKey 返回锁在 Consul 中的实际 KV 键,便于运维定位与手工干预
func (m *Mutex) KVKey() string { return m.kvKey }

// Lock 阻塞获取,直到成功、ctx 取消/超时、WaitTimeout 到点或后端故障。
// 等待为轮询:consul 组件的 AcquireLock 不阻塞,而其 blocking query(WatchKV)
// 不接受 ctx,取消语义会失真,故统一走带抖动的轮询。
func (m *Mutex) Lock(ctx context.Context) error {
	start := time.Now()
	err := lock.WaitAcquire(ctx, &m.cfg, m.attempt)
	// 等待时长无条件累计:等到超时/被取消同样花了时间
	m.rec.NoteWait(time.Since(start))
	if err != nil {
		m.rec.NoteFailure(err)
		return err
	}
	return nil
}

// TryLock 非阻塞获取;被占用返回包装 lock.ErrBusy 的 error
func (m *Mutex) TryLock(ctx context.Context) error {
	acquired, busy, err := m.attempt(ctx)
	if err != nil {
		m.rec.NoteFailure(err)
		return err
	}
	if acquired {
		return nil
	}
	if !busy {
		return m.noteAbnormal(fmt.Errorf("%w: key=%s 获取尝试返回异常状态", lock.ErrInvalidConfig, m.cfg.Key))
	}
	return fmt.Errorf("%w: key=%s 正被持有", lock.ErrBusy, m.cfg.Key)
}

// noteAbnormal 统一「尝试返回异常状态」的记账与返回
func (m *Mutex) noteAbnormal(err error) error {
	m.rec.NoteFailure(err)
	return err
}

// Unlock 释放锁:可重入时只减一层深度,减到 0 才释放 KV 并销毁会话。
// 仅持有者可释放:调用方 Owner 与记录属主不符(默认按 goroutine 绑定)时直接返回
// 包装 lock.ErrNotHeld 的 error,不发起任何后端调用。
// 本地未持有直接返回包装 lock.ErrNotHeld 的 error(不浪费 RTT);
// 服务端拒绝释放说明会话已被 Consul 回收,错误同时包装 lock.ErrLost。
//
// 注意:consul 组件客户端方法不接受 context,取消/超时无法传导到释放请求本身
// (与 redisstore 不同,那里用 context.Background 兜底保证资源归还)。
func (m *Mutex) Unlock(_ context.Context) error {
	return m.unlockAs(m.cfg.CurrentOwner())
}

// unlockAs 以 caller 身份释放。普通调用方传自己的 Owner;Close 传记录属主,
// 保证在持有 goroutine 之外收尾时会话与 KV 仍能归还,而非等到 TTL 自然回收。
func (m *Mutex) unlockAs(caller string) error {
	m.mu.Lock()
	if m.depth == 0 {
		m.mu.Unlock()
		return m.noteReleaseRejected(fmt.Errorf("%w: key=%s 本实例未持有", lock.ErrNotHeld, m.cfg.Key))
	}
	if caller != m.heldOwner {
		m.mu.Unlock()
		return m.noteReleaseRejected(fmt.Errorf("%w: key=%s 释放者 %q 非当前持有者 %q", lock.ErrNotHeld, m.cfg.Key, caller, m.heldOwner))
	}
	if m.depth > 1 {
		m.depth--
		m.mu.Unlock()
		m.rec.NoteReleased(false)
		return nil
	}
	session := m.sessionID
	m.mu.Unlock()

	// 顺序:先 Release(明确解除 KV 占用),再 Destroy(回收会话)。
	// Destroy 失败不影响锁已释放的事实,仅记录到返回错误中。
	released, err := m.release(session)
	if err != nil {
		m.rec.NoteFailure(err)
		return fmt.Errorf("lock/consulstore: 释放失败: %w", err)
	}
	if !released {
		m.forfeit()
		return m.noteReleaseRejected(fmt.Errorf("%w: key=%s 属主已失, 释放被 Consul 拒绝(会话可能已过期): %w",
			lock.ErrLost, m.cfg.Key, lock.ErrNotHeld))
	}

	m.mu.Lock()
	m.clearLocked()
	m.mu.Unlock()
	m.wd.Stop()
	m.rec.NoteReleased(true)

	if derr := m.backend.DestroySession(session); derr != nil {
		return fmt.Errorf("lock/consulstore: 锁已释放但会话销毁失败, 该会话将在 TTL %v 后自动回收: %w",
			m.cfg.TTL, derr)
	}
	return nil
}

// noteReleaseRejected 统一「释放被拒」的记账与返回
func (m *Mutex) noteReleaseRejected(err error) error {
	m.rec.NoteReleaseRejected(err)
	return err
}

// errRenewEmptyEntry Backend 返回 (nil, nil) 时补的锚点错误:consul/api 的 Renew 对
// 已销毁/已过期的会话不给 error,只给空条目,只看 err 会把丢锁读成续期成功。
var errRenewEmptyEntry = errors.New("consul: 会话已不存在(Renew 返回空条目)")

// renewErr 归一化续期结果:优先透传后端错误,再把「会话不存在」的空条目折算成错误。
func renewErr(entry *api.SessionEntry, err error) error {
	if err != nil {
		return err
	}
	if entry == nil {
		return errRenewEmptyEntry
	}
	return nil
}

// Close 停止看门狗、释放仍持有的锁并销毁会话。本地复位与看门狗停止总会完成,
// 释放/销毁中的错误经 errors.Join 合并返回;可重复调用。关闭后继续获取返回
// 包装 lock.ErrClosed 的 error。只影响本实例的会话,不触碰他人持锁状态。
func (m *Mutex) Close() error {
	m.mu.Lock()
	held := m.depth > 0
	m.mu.Unlock()

	var errs []error
	if held {
		// 以记录属主释放:Close 常发生在持有 goroutine 之外
		errs = append(errs, m.unlockAs(m.heldOwnerOf()))
	}

	m.mu.Lock()
	m.closed = true
	session := m.sessionID
	if session != "" {
		m.sessionID = ""
	}
	m.mu.Unlock()

	// 兜底销毁:Unlock 路径已销毁时此处 session 为空
	if session != "" {
		if err := m.backend.DestroySession(session); err != nil {
			errs = append(errs, fmt.Errorf("lock/consulstore: 会话销毁失败, 将在 TTL %v 后自动回收: %w", m.cfg.TTL, err))
		}
	}
	m.wd.Stop()
	return errors.Join(errs...)
}

// Held 报告本实例视图下是否持有(深度 > 0 且属主为本调用方)
func (m *Mutex) Held() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depth > 0 && m.heldOwner == m.cfg.CurrentOwner()
}

// Depth 返回本地重入深度(观测用)
func (m *Mutex) Depth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depth
}

// Stats 返回本实例的运行计数快照(实现 lock.Statser)。
// Fence 恒为 0(consulstore 不提供栅栏);重入时顺带的 RenewSession 同样计入续期计数,
// 因为它确实向 Consul 发了一次会话续期。
func (m *Mutex) Stats() lock.Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rec.Snapshot(lock.Stats{
		Key:     m.cfg.Key,
		Backend: "consul",
		TTL:     m.cfg.TTL,
		Depth:   m.depth,
		Holding: m.depth > 0,
		Closed:  m.closed,
	})
}

// heldOwnerOf 返回记录属主(供 Close 以持有者身份归还 KV 与会话)
func (m *Mutex) heldOwnerOf() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heldOwner
}

// SessionID 返回本轮持有的会话 ID,未持有时为空——供排障时在 Consul 里查会话状态
func (m *Mutex) SessionID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessionID
}

// Refresh 手动续期一次本次持有的会话。
// 默认看门狗已按 RenewInterval(缺省 TTL/3)自动续期,无需调用;
// 配置 RenewInterval=-1 关闭后,长任务必须自行周期调用本方法。
// 未持有返回包装 lock.ErrNotHeld 的 error;会话已被回收时报 lock.ErrLost。
func (m *Mutex) Refresh(ctx context.Context) error {
	return m.renew(ctx)
}

// attempt 一次获取尝试:先按本地 Owner 拦掉跨 goroutine 误重入,
// 首次获得时建会话并 Acquire KV,重入只加深度并顺带续期。
func (m *Mutex) attempt(ctx context.Context) (acquired, busy bool, err error) {
	// 设计约定二:Backend 实现(包括传入的 *consul.Client)不得以 panic 形式
	// 逃逸给调用方——TryLock 直接调用本方法,故在此兜住。
	defer func() {
		if r := recover(); r != nil {
			acquired, busy, err = false, false, fmt.Errorf("lock/consulstore: 获取尝试 panic: %v", r)
		}
	}()

	if err := ctx.Err(); err != nil {
		return false, false, err
	}

	owner := m.cfg.CurrentOwner()
	if owner == "" {
		return false, false, fmt.Errorf("%w: OwnerFunc 返回空串, 无法标识持有者", lock.ErrInvalidConfig)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false, false, fmt.Errorf("%w: key=%s", lock.ErrClosed, m.cfg.Key)
	}
	m.rec.NoteAttempt()
	if m.depth > 0 {
		// 已有持有者:同 Owner 视为重入,他人直接判 busy(不查 Consul)
		if m.heldOwner != owner {
			m.mu.Unlock()
			m.rec.NoteBusy()
			return false, true, nil
		}
		m.depth++
		session := m.sessionID
		m.mu.Unlock()
		// 重入顺带续一次租期,避免深层递归长时间不返回而临近过期
		entry, rerr := m.backend.RenewSession(session)
		rerr = renewErr(entry, rerr)
		m.rec.NoteRenew(rerr)
		if rerr != nil {
			m.mu.Lock()
			m.depth--
			m.mu.Unlock()
			return false, false, fmt.Errorf("lock/consulstore: 重入续期失败: %w", rerr)
		}
		m.rec.NoteAcquired(false)
		return true, false, nil
	}
	m.mu.Unlock()

	session, err := m.backend.CreateSession(m.sessionName(owner), m.cfg.TTL)
	if err != nil {
		return false, false, fmt.Errorf("lock/consulstore: 创建会话失败: %w", err)
	}

	ok, err := m.backend.AcquireLock(m.kvKey, session)
	if err != nil {
		// 未取得就作废会话,否则白占一份 TTL 会话
		if derr := m.backend.DestroySession(session); derr != nil {
			return false, false, fmt.Errorf("lock/consulstore: 获取锁失败: %w (会话销毁同样失败: %v)", err, derr)
		}
		return false, false, fmt.Errorf("lock/consulstore: 获取锁失败: %w", err)
	}
	if !ok {
		if derr := m.backend.DestroySession(session); derr != nil {
			m.rec.NoteBusy()
			return false, true, fmt.Errorf("lock/consulstore: 锁被占用且会话销毁失败, 等待 TTL 回收: %w", derr)
		}
		m.rec.NoteBusy()
		return false, true, nil
	}

	m.mu.Lock()
	m.sessionID = session
	m.heldOwner = owner
	m.depth = 1
	m.mu.Unlock()
	m.rec.NoteAcquired(true)
	m.wd.Start()
	return true, false, nil
}

// renew 看门狗续期入口:看门狗与手动 Refresh 同一口径记账。
func (m *Mutex) renew(ctx context.Context) error {
	err := m.renewOnce(ctx)
	m.rec.NoteRenew(err)
	return err
}

// renewOnce 看门狗续期:以本轮会话调用 RenewSession。
// 会话被 Consul 回收(过期/失效)时报 ErrLost,看门狗据此停止空转。
func (m *Mutex) renewOnce(ctx context.Context) error {
	m.mu.Lock()
	session, holding := m.sessionID, m.depth > 0
	m.mu.Unlock()
	if !holding || session == "" {
		return fmt.Errorf("%w: key=%s 本实例未持有", lock.ErrNotHeld, m.cfg.Key)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	entry, err := m.backend.RenewSession(session)
	if err = renewErr(entry, err); err != nil {
		m.forfeit()
		return fmt.Errorf("%w: key=%s 会话续期失败(会话可能已被回收), 本地深度已作废: %w",
			lock.ErrLost, m.cfg.Key, err)
	}
	return nil
}

// sessionName 会话名,便于在 Consul UI/`consul session list` 里按锁与持有者识别
func (m *Mutex) sessionName(owner string) string {
	return fmt.Sprintf("%s:%s:%s", defaultPrefix, m.cfg.Key, owner)
}

// release 调后端释放,隔离 panic 并把传输层错误包装清楚
func (m *Mutex) release(session string) (released bool, err error) {
	if session == "" {
		return false, fmt.Errorf("%w: key=%s 本地无会话记录, 无法释放", lock.ErrNotHeld, m.cfg.Key)
	}
	defer func() {
		if r := recover(); r != nil {
			released, err = false, fmt.Errorf("lock/consulstore: 释放 panic: %v", r)
		}
	}()
	return m.backend.ReleaseLock(m.kvKey, session)
}

// forfeit 属主已失时的本地复位:清状态并停看门狗,不再尝试释放
func (m *Mutex) forfeit() {
	m.mu.Lock()
	m.clearLocked()
	m.mu.Unlock()
	m.wd.Stop()
}

// clearLocked 复位本地持有视图(会话留给调用方销毁,避免在锁内做 IO)
func (m *Mutex) clearLocked() {
	m.depth = 0
	m.heldOwner = ""
	m.sessionID = ""
}

// 编译期断言:consul 组件客户端必须能直接作为 Backend 传入。
// 一旦 consul 侧改掉这几个方法签名,此处即编译失败,而不是等到运行期才发现。
var _ Backend = (*xmconsul.Client)(nil)
