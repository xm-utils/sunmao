// Package redisstore 提供 lock 的 Redis 后端(ROADMAP 3.3.1):
// SetNX 语义的互斥锁 + Owner 属主校验 + 服务端深度计数,多实例共享同一把锁。
//
// 复用仓库 redis 组件的方式与 ratelimit/redisstore 一致:连接管理沿用其
// InitRedisCache(配置/连接池/健康检查归组件),本包接收其管理的 *redis.Client
// 直接执行脚本——组件全局方法存在 KEYS 前缀等问题,不走全局函数。
//
// 三个动作(获取/释放/续期)全部由 Lua 脚本在服务端原子执行,消除
// 「读到属主 → 判断 → 写回」的竞态窗口;临界区判定完全依赖脚本返回值。
//
// 集群友好:锁状态与栅栏计数同处一个 {tag},Cluster 下 EVALSHA 与脚本内命令
// 命中同一 slot,不会跨槽。
//
// 后端故障一律返回 error,绝不降级为「获得锁」(fail-closed)——锁的语义宁可让
// 任务不执行,也不能让两个持有者同时执行。
package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/sunmao/lock"
)

// 返回码约定:脚本用整数码表达判定结果,避免字符串比较与 nil 歧义。
const (
	codeAcquired   int64 = 1  // 获得或重入成功
	codeBusy       int64 = 0  // 他人持有
	codeNoHolder   int64 = -1 // 释放/续期时锁不存在(租期已过或从未持有)
	codeOtherOwner int64 = -2 // 释放/续期时属主不是自己
)

// acquireScript 获取(或重入)锁。
// KEYS[1]=锁 Hash, KEYS[2]=栅栏计数 Key(无 TTL,保证栅栏值单调)。
// ARGV: 1=owner 2=租期(ms)。
// 返回 {码, 深度, 栅栏值}:空闲时置 owner/depth=1/fence=INCR 并 PEXPIRE;
// 同 owner 重入只加深不换栅栏;他人持有返回 {0,0,0}。
const acquireScript = `
local key = KEYS[1]
local fenceKey = KEYS[2]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])

local cur = redis.call('HGET', key, 'owner')
if not cur then
  local fence = redis.call('INCR', fenceKey)
  redis.call('HSET', key, 'owner', owner, 'depth', 1, 'fence', fence)
  redis.call('PEXPIRE', key, ttl)
  return {1, 1, fence}
end

if cur == owner then
  local depth = redis.call('HINCRBY', key, 'depth', 1)
  redis.call('PEXPIRE', key, ttl)
  local f = redis.call('HGET', key, 'fence')
  return {1, depth, tonumber(f) or 0}
end

return {0, 0, 0}
`

// releaseScript 释放一层深度,减到 0 才删 Key(不碰栅栏计数,保持单调)。
// 返回 {码, 剩余深度}: -1 锁不存在, -2 属主不匹配, 1 仍有残留深度, 0 已彻底释放。
const releaseScript = `
local key = KEYS[1]
local owner = ARGV[1]

local cur = redis.call('HGET', key, 'owner')
if not cur then
  return {-1, 0}
end
if cur ~= owner then
  return {-2, 0}
end

local depth = redis.call('HINCRBY', key, 'depth', -1)
if depth > 0 then
  return {1, depth}
end
redis.call('DEL', key)
return {0, 0}
`

// renewScript 看门狗续期:属主校验通过才重置租期。
// 返回 -1(锁已消失,即租期到点被他人抢走前的空窗)、-2(属主已易主)、1(续期成功)。
const renewScript = `
local key = KEYS[1]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])

local cur = redis.call('HGET', key, 'owner')
if not cur then
  return -1
end
if cur ~= owner then
  return -2
end
redis.call('PEXPIRE', key, ttl)
return 1
`

// 脚本单例:Run 乐观走 EVALSHA,NOSCRIPT 时自动回落 EVAL(v8 未导出 NoScriptError)
var (
	acquireLua = redis.NewScript(acquireScript)
	releaseLua = redis.NewScript(releaseScript)
	renewLua   = redis.NewScript(renewScript)
)

// Config Redis 后端锁配置。内嵌的 lock.Config 字段(Key/TTL/Owner 等)
// 与本结构同层参与 yaml 反序列化,键位见 ../config.example.yaml。
type Config struct {
	// Client 由 redis 组件 InitRedisCache 建立(或同等来源)的客户端,必填
	Client *redis.Client
	lock.Config
}

// Mutex Redis 后端分布式锁,实现 lock.Mutex(附 lock.Fencer / lock.Holder)。
// 本地只缓存属主视图(深度、栅栏值)以支持 Held/Fence 零 RTT,判定一律以服务端为准。
type Mutex struct {
	client   *redis.Client
	cfg      lock.Config
	hashKey  string
	fenceKey string

	wd *lock.Watchdog

	mu        sync.Mutex
	heldOwner string // 本轮持有的属主,续期由其他 goroutine 驱动时必须用它
	depth     int
	fence     uint64
	closed    bool
}

var (
	_ lock.Mutex     = (*Mutex)(nil)
	_ lock.Fencer    = (*Mutex)(nil)
	_ lock.Holder    = (*Mutex)(nil)
	_ lock.Refresher = (*Mutex)(nil)
)

// New 创建 Redis 锁;配置非法返回包装 lock.ErrInvalidConfig 的 error,不 panic。
// Client 必填,故不做参数 Option,统一在构造期校验。
func New(cfg Config) (*Mutex, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("%w: Client 不能为 nil", lock.ErrInvalidConfig)
	}
	// 拷贝一份再规范化,避免改写调用方的 cfg
	c := cfg.Config
	if err := c.Normalize(); err != nil {
		return nil, err
	}

	m := &Mutex{
		client: cfg.Client,
		cfg:    c,
		// {tag} 保证两个 Key 在 Cluster 下同 slot
		hashKey:  "xm_lock:{" + c.Key + "}",
		fenceKey: "xm_lock:{" + c.Key + "}:fence",
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

// Lock 阻塞获取,直到成功、ctx 取消/超时、WaitTimeout 到点或后端故障。
// 等待为服务端轮询(多进程无共享内存,无法像进程内锁那样释放即唤醒)。
func (m *Mutex) Lock(ctx context.Context) error {
	return lock.WaitAcquire(ctx, &m.cfg, m.attempt)
}

// TryLock 非阻塞获取;被占用返回包装 lock.ErrBusy 的 error
func (m *Mutex) TryLock(ctx context.Context) error {
	acquired, busy, err := m.attempt(ctx)
	if err != nil {
		return err
	}
	if acquired {
		return nil
	}
	if !busy {
		return fmt.Errorf("%w: key=%s 获取尝试返回异常状态", lock.ErrInvalidConfig, m.cfg.Key)
	}
	return fmt.Errorf("%w: key=%s 正被持有", lock.ErrBusy, m.cfg.Key)
}

// Unlock 释放锁:可重入时只减一层深度,减到 0 才真正删除服务端状态。
// 仅持有者可释放:调用方 Owner 与记录属主不符(默认按 goroutine 绑定)时
// 直接返回包装 lock.ErrNotHeld 的 error 且不发起 RTT——
// 跨持有者释放会把别人的临界区提前打开。本地未持有同样报 ErrNotHeld;
// 服务端已不存在该锁返回包装 lock.ErrNotHeld 的 error(租期已过);
// 若本地仍认为持有而服务端已易主,错误同时包装 lock.ErrLost——
// 意味着临界区可能已被并发执行,调用方不应重试补偿。
// ctx 已取消仍会尝试释放(用 context.Background 兜底),避免租期外长期泄漏。
func (m *Mutex) Unlock(ctx context.Context) error {
	return m.unlockAs(ctx, m.cfg.CurrentOwner())
}

// unlockAs 以 caller 身份执行释放。普通调用方传自己的 Owner,与记录属主不符即拒;
// Close 传记录属主,保证在持有 goroutine 之外收尾时租约仍能归还而非白等到 TTL。
func (m *Mutex) unlockAs(ctx context.Context, caller string) error {
	m.mu.Lock()
	localDepth := m.depth
	held := m.heldOwner
	m.mu.Unlock()
	if localDepth == 0 {
		return fmt.Errorf("%w: key=%s 本实例未持有", lock.ErrNotHeld, m.cfg.Key)
	}
	if caller != held {
		return fmt.Errorf("%w: key=%s 释放者 %q 非当前持有者 %q", lock.ErrNotHeld, m.cfg.Key, caller, held)
	}

	runCtx := ctx
	if ctx.Err() != nil {
		// 调用方 ctx 已取消:资源仍需归还,改用无取消背景执行
		runCtx = context.Background()
	}

	arr, err := m.eval(runCtx, releaseLua, nil, held)
	if err != nil {
		return fmt.Errorf("lock/redisstore: 释放失败: %w", err)
	}
	code, depth, err := codeDepth(arr)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	switch code {
	case 1: // 仍有残留深度
		m.depth = int(depth)
		return nil
	case 0: // 彻底释放
		m.clearLocked()
		m.wd.Stop()
		return nil
	case codeNoHolder:
		// 服务端已过期:本地状态一并清掉,不再空转续期
		m.clearLocked()
		m.wd.Stop()
		return fmt.Errorf("%w: key=%s 锁已不存在(租期 %v 已过)",
			lock.ErrNotHeld, m.cfg.Key, m.cfg.TTL)
	case codeOtherOwner:
		m.clearLocked()
		m.wd.Stop()
		return fmt.Errorf("%w: key=%s 属主已易主, 本地深度 %d: %w",
			lock.ErrLost, m.cfg.Key, localDepth, lock.ErrNotHeld)
	default:
		return fmt.Errorf("%w: 释放脚本返回未知码 %d", lock.ErrInvalidConfig, code)
	}
}

// Close 停止看门狗并释放仍持有的锁。本地复位与看门狗停止总会完成,释放中的错误
// 原样返回;可重复调用。关闭后 Lock/TryLock 返回包装 lock.ErrClosed 的 error。
// 只影响本实例,Redis 中其他实例的状态不受影响。
func (m *Mutex) Close() error {
	m.mu.Lock()
	held := m.depth > 0
	m.mu.Unlock()

	var err error
	if held {
		// 用无取消 ctx:调用方 ctx 已结束不能成为锁泄漏的理由;
		// 以记录属主释放:Close 常发生在持有 goroutine 之外
		err = m.unlockAs(context.Background(), m.heldOwnerOf())
	}

	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.wd.Stop()
	return err
}

// Held 报告本实例视图下是否仍持有(重入深度 > 0 且属主为本调用方)。
// 只反映本地缓存,服务端租期是否已被抢占以下一次操作结果为准。
func (m *Mutex) Held() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depth > 0 && m.heldOwner == m.cfg.CurrentOwner()
}

// Fence 返回本轮持锁的栅栏值(单调递增),未持有时为 0。
// 下游写入可带上门禁条件以拒绝旧持有者。
func (m *Mutex) Fence() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.depth == 0 {
		return 0
	}
	return m.fence
}

// Depth 返回本地缓存的重入深度(观测用)
func (m *Mutex) Depth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depth
}

// Refresh 手动续期一次本次持有的租期。
// 默认看门狗已按 RenewInterval(缺省 TTL/3)自动续期,无需调用;
// 配置 RenewInterval=-1 关闭后,长任务必须自行周期调用本方法。
// 未持有返回包装 lock.ErrNotHeld 的 error;属主已失时报 lock.ErrLost。
func (m *Mutex) Refresh(ctx context.Context) error {
	return m.renew(ctx)
}

// RemainingTTL 返回服务端剩余租期。Key 不存在返回 (0, nil);
// Redis 报「无 TTL」(-1ms)时同样归零。仅用于观测与巡检。
func (m *Mutex) RemainingTTL(ctx context.Context) (time.Duration, error) {
	left, err := m.client.PTTL(ctx, m.hashKey).Result()
	if err != nil {
		return 0, fmt.Errorf("lock/redisstore: 读取剩余租期失败: %w", err)
	}
	if left < 0 {
		return 0, nil
	}
	return left, nil
}

// attempt 一次服务端获取尝试:执行脚本并按返回码更新本地属主视图,
// 首次真正获得时启动看门狗。
func (m *Mutex) attempt(ctx context.Context) (acquired, busy bool, err error) {
	// 设计约定二:后端异常不得以 panic 形式逃逸。TryLock 直接调用本方法,
	// 故在此兜住;Lock 路径另有 WaitAcquire 一层的隔离。
	defer func() {
		if r := recover(); r != nil {
			acquired, busy, err = false, false, fmt.Errorf("lock/redisstore: 获取尝试 panic: %v", r)
		}
	}()

	if err := ctx.Err(); err != nil {
		return false, false, err
	}

	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return false, false, fmt.Errorf("%w: key=%s", lock.ErrClosed, m.cfg.Key)
	}

	owner := m.cfg.CurrentOwner()
	if owner == "" {
		return false, false, fmt.Errorf("%w: OwnerFunc 返回空串, 无法标识持有者", lock.ErrInvalidConfig)
	}

	arr, err := m.eval(ctx, acquireLua, []string{m.fenceKey}, owner, m.cfg.TTL.Milliseconds())
	if err != nil {
		return false, false, fmt.Errorf("lock/redisstore: 获取失败: %w", err)
	}
	if len(arr) < 3 {
		return false, false, fmt.Errorf("lock/redisstore: 获取脚本返回值格式异常: %#v", arr)
	}
	code, err := toInt64(arr[0])
	if err != nil {
		return false, false, err
	}
	depth, err := toInt64(arr[1])
	if err != nil {
		return false, false, err
	}
	fence, err := toInt64(arr[2])
	if err != nil {
		return false, false, err
	}

	switch code {
	case codeAcquired:
		m.mu.Lock()
		first := m.depth == 0
		m.depth = int(depth)
		m.heldOwner = owner
		m.fence = uint64(fence)
		m.mu.Unlock()
		if first {
			m.wd.Start()
		}
		return true, false, nil
	case codeBusy:
		return false, true, nil
	default:
		return false, false, fmt.Errorf("%w: 获取脚本返回未知码 %d", lock.ErrInvalidConfig, code)
	}
}

// renew 看门狗续期:以本轮记录的属主为准(续期由独立 goroutine 驱动,
// 其 goroutine 标识与持锁方不同,不能用 CurrentOwner 现取)。
func (m *Mutex) renew(ctx context.Context) error {
	m.mu.Lock()
	owner, holding := m.heldOwner, m.depth > 0
	m.mu.Unlock()
	if !holding {
		return fmt.Errorf("%w: key=%s 本实例未持有", lock.ErrNotHeld, m.cfg.Key)
	}

	code, err := m.evalInt(ctx, renewLua, owner, m.cfg.TTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("lock/redisstore: 续期失败: %w", err)
	}
	switch code {
	case 1:
		return nil
	case codeNoHolder:
		m.mu.Lock()
		m.clearLocked()
		m.mu.Unlock()
		return fmt.Errorf("%w: key=%s 租期 %v 已到期, 锁已被释放",
			lock.ErrLost, m.cfg.Key, m.cfg.TTL)
	case codeOtherOwner:
		m.mu.Lock()
		m.clearLocked()
		m.mu.Unlock()
		return fmt.Errorf("%w: key=%s 属主已易主(本地记录 %q 已无效)", lock.ErrLost, m.cfg.Key, owner)
	default:
		return fmt.Errorf("%w: 续期脚本返回未知码 %d", lock.ErrInvalidConfig, code)
	}
}

// heldOwnerOf 返回记录属主(供 Close 以持有者身份归还服务端状态)
func (m *Mutex) heldOwnerOf() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heldOwner
}

// clearLocked 复位本地属主视图,调用方须持锁
func (m *Mutex) clearLocked() {
	m.depth = 0
	m.heldOwner = ""
	m.fence = 0
}

// eval 执行脚本并取回数组返回值;keys 为空时只传主 Hash Key
func (m *Mutex) eval(ctx context.Context, script *redis.Script, extraKeys []string, args ...interface{}) ([]interface{}, error) {
	keys := append([]string{m.hashKey}, extraKeys...)
	return script.Run(ctx, m.client, keys, args...).Slice()
}

// evalInt 执行返回单个整数的脚本(复用脚本单例,走 EVALSHA)
func (m *Mutex) evalInt(ctx context.Context, script *redis.Script, args ...interface{}) (int64, error) {
	return script.Run(ctx, m.client, []string{m.hashKey}, args...).Int64()
}

func codeDepth(arr []interface{}) (code, depth int64, err error) {
	if len(arr) < 2 {
		return 0, 0, fmt.Errorf("lock/redisstore: 脚本返回值格式异常: %#v", arr)
	}
	if code, err = toInt64(arr[0]); err != nil {
		return 0, 0, err
	}
	if depth, err = toInt64(arr[1]); err != nil {
		return 0, 0, err
	}
	return code, depth, nil
}

func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("lock/redisstore: 脚本返回元素类型异常: %#v", v)
	}
}
