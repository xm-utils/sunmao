package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// LocalConfig 本地内存后端配置
type LocalConfig struct {
	// Rate 新建限流器的默认回填速率(个/秒),必须 > 0
	Rate float64
	// Burst 新建限流器的默认桶容量,必须 > 0
	Burst float64
	// Clock 时间源,nil 时使用 SystemClock
	Clock Clock
}

// Local 本地内存限流后端(单实例):按 Key 惰性创建并复用 TokenBucket,
// 天然支撑接口级/服务级/租户级等多维限流(Key 由使用方拼接,如 "api:order:create")。
// 判定结果按 Key 累计计数,经 Snapshots 供观测(metrics 适配器与告警巡检拉取)。
type Local struct {
	mu      sync.RWMutex
	cfg     LocalConfig
	buckets map[string]*TokenBucket
	counts  map[string]*keyCount
	closed  bool
}

// keyCount 单 Key 判定计数(与 buckets 同锁保护)
type keyCount struct {
	total    int64
	rejected int64
}

// KeySnapshot 单 Key 的观测快照:累计计数 + 当前令牌水位。
// 计数为进程启动以来累计值(非滑动窗口值),速率类判定请对两次采样做差分。
type KeySnapshot struct {
	Key       string
	Total     int64   // 累计判定次数(放行+拒绝,不含配置错误/后端故障/ctx 取消)
	Rejected  int64   // 累计被限流拒绝次数
	Remaining float64 // 当前可用令牌数(Key 已移除时为 0)
	Limit     float64 // 当前速率(个/秒)
}

// NewLocal 创建本地内存后端;配置非法返回包装 ErrInvalidConfig 的 error,不 panic。
func NewLocal(cfg LocalConfig) (*Local, error) {
	probe := TokenBucketConfig{Rate: cfg.Rate, Burst: cfg.Burst, Clock: cfg.Clock}
	if _, err := NewTokenBucket(probe); err != nil {
		return nil, err
	}
	return &Local{
		cfg:     cfg,
		buckets: make(map[string]*TokenBucket),
		counts:  make(map[string]*keyCount),
	}, nil
}

// Limiter 获取(或按默认配置惰性创建)指定 Key 的限流器;
// Key 为空或后端已关闭时返回包装对应哨兵的 error
func (l *Local) Limiter(key string) (Limiter, error) {
	if key == "" {
		return nil, fmt.Errorf("%w: Key 不能为空", ErrInvalidConfig)
	}

	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return nil, ErrClosed
	}
	if b, ok := l.buckets[key]; ok {
		l.mu.RUnlock()
		return b, nil
	}
	l.mu.RUnlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	// 双检:拿写锁期间可能已被其他 goroutine 创建,或后端已关闭
	if l.closed {
		return nil, ErrClosed
	}
	if b, ok := l.buckets[key]; ok {
		return b, nil
	}
	b, err := NewTokenBucket(TokenBucketConfig{
		Rate:  l.cfg.Rate,
		Burst: l.cfg.Burst,
		Clock: l.cfg.Clock,
	})
	if err != nil {
		// NewLocal 已校验过配置,此处仅防御性透传
		return nil, err
	}
	l.buckets[key] = b
	return b, nil
}

// Allow 指定 Key 的单次放行判定
func (l *Local) Allow(ctx context.Context, key string) error {
	b, err := l.Limiter(key)
	if err != nil {
		return err
	}
	err = b.Allow(ctx)
	// 仅对真实判定记账:ctx 取消/后端故障等非判定结果不计入,避免污染拒绝率差分
	if err == nil || errors.Is(err, ErrRateLimited) {
		l.record(key, errors.Is(err, ErrRateLimited))
	}
	return err
}

// AllowN 指定 Key 的一次消耗 n 个令牌判定
func (l *Local) AllowN(ctx context.Context, key string, n int) (bool, error) {
	b, err := l.Limiter(key)
	if err != nil {
		return false, err
	}
	ok, err := b.AllowN(ctx, n)
	// AllowN 被拒为 (false, nil),按结果记账;后端/参数错误不记
	if err == nil {
		l.record(key, !ok)
	}
	return ok, err
}

// record 按 Key 累计判定计数;仅对真实判定(放行或限流拒绝)记账,
// 后端错误/ctx 取消不计入,避免污染拒绝率差分
func (l *Local) record(key string, rejected bool) {
	l.mu.Lock()
	c, ok := l.counts[key]
	if !ok {
		c = &keyCount{}
		l.counts[key] = c
	}
	c.total++
	if rejected {
		c.rejected++
	}
	l.mu.Unlock()
}

// Update 热更新指定 Key 的速率与容量(规则变更的应用端点;
// 配置源监听与广播由使用方装配,与 logging 调级同一手法)。
// Key 不存在时按新参数创建;非法参数保持原状并返回 error。
func (l *Local) Update(key string, rate, burst float64) error {
	if key == "" {
		return fmt.Errorf("%w: Key 不能为空", ErrInvalidConfig)
	}

	l.mu.RLock()
	b, ok := l.buckets[key]
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if ok {
		return b.SetParams(rate, burst)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if exist, ok := l.buckets[key]; ok {
		return exist.SetParams(rate, burst)
	}
	nb, err := NewTokenBucket(TokenBucketConfig{Rate: rate, Burst: burst, Clock: l.cfg.Clock})
	if err != nil {
		return err
	}
	l.buckets[key] = nb
	return nil
}

// Remove 移除指定 Key 的限流器(规则删除场景);Key 不存在视为已移除,返回 nil。
// 计数快照保留(供事后审计),不随 Remove 清除。
func (l *Local) Remove(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.buckets[key]; ok {
		delete(l.buckets, key)
		return b.Close()
	}
	return nil
}

// Stats 返回指定 Key 的当前令牌快照;Key 不存在时 OK=false、Remaining=0
func (l *Local) Stats(key string) Result {
	l.mu.RLock()
	b, ok := l.buckets[key]
	l.mu.RUnlock()
	if !ok {
		return Result{}
	}
	return b.Peek()
}

// Snapshots 返回全部有判定记录 Key 的观测快照(计数 + 令牌水位),
// 供 metrics 适配器与告警巡检拉取;后端已关闭时返回 nil 不报错。
func (l *Local) Snapshots() map[string]KeySnapshot {
	l.mu.RLock()
	out := make(map[string]KeySnapshot, len(l.counts))
	for key, c := range l.counts {
		s := KeySnapshot{Key: key, Total: c.total, Rejected: c.rejected}
		if b, ok := l.buckets[key]; ok {
			res := b.Peek()
			s.Remaining, s.Limit = res.Remaining, res.Limit
		}
		out[key] = s
	}
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		return nil
	}
	return out
}

// Close 关闭后端并释放全部限流器,幂等;关闭后新建与判定一律返回 ErrClosed
func (l *Local) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	for key, b := range l.buckets {
		_ = b.Close()
		delete(l.buckets, key)
	}
	return nil
}
