// Package lock 把 lock 组件的实例快照(lock.Statser.Stats)适配为统一注册表
// 可收录的 prometheus.Collector,输出 xmt_lock_* 指标。
//
// 拉取式设计:每次抓取时逐个调用实例的 Stats(),不改动、不包装锁本体;
// lock 组件零依赖本包(适配器模式优先,与 metrics/adapter/{breaker,deadletter,kafka,ratelimit} 同手法)。
// panic 安全:provider 或某个实例取数 panic 一律兜成抓取错误(InvalidMetric),
// 单个实例坏掉不会让其余锁的指标一起消失。
package lock

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	xmlock "github.com/xm-utils/sunmao/lock"
	"github.com/xm-utils/sunmao/metrics"
)

const component = "lock"

// Provider 快照源,map 的键约定为锁 Key(快照内 Key 非空时以它为准)。
// 装配层自持锁注册表(多后端聚合、按租户分片)时用它,只有若干实例时用 New。
type Provider func() map[string]xmlock.Stats

// Collector 实现 prometheus.Collector,输出五组指标族, label 统一为 (key, backend):
//
//	xmt_lock_operations_total{key,backend,op,result}  op=acquire|release|renew
//	xmt_lock_errors_total{key,backend}                后端故障与误用(跨三条路径汇总)
//	xmt_lock_duration_seconds_total{key,backend,phase} phase=wait|hold
//	xmt_lock_contention_ratio{key,backend}            累计撞锁比例
//	xmt_lock_state{key,backend,field}                 field=holding|depth|fence|closed
//
// 注册方式:metrics.Register(c) 或 metrics.RegisterExporter(c)。
type Collector struct {
	provider  Provider
	instances []xmlock.Statser

	opsDesc        *prometheus.Desc
	errorsDesc     *prometheus.Desc
	durationDesc   *prometheus.Desc
	contentionDesc *prometheus.Desc
	stateDesc      *prometheus.Desc
	errDesc        *prometheus.Desc // 抓取失败占位(InvalidMetric 用)
}

var _ prometheus.Collector = (*Collector)(nil)

// New 为若干锁实例建立采集器。参数类型是 lock.Statser,因此 Local 与
// redisstore/consulstore 的 Mutex 可直接传入;自定义后端只要实现 Stats() 也能进。
// 每次抓取实时拉取,实例的持锁状态变化会立刻反映到指标上。
func New(instances ...xmlock.Statser) *Collector {
	return &Collector{
		instances:      instances,
		opsDesc:        desc("operations_total", "锁操作累计计数, 按 op(acquire/release/renew) 与 result 区分", "key", "backend", "op", "result"),
		errorsDesc:     desc("errors_total", "后端故障与误用累计计数(跨获取/释放/续期汇总, 无法按路径拆分)", "key", "backend"),
		durationDesc:   desc("duration_seconds_total", "累计耗时(秒): phase=wait 为阻塞等待, phase=hold 为完整持有的临界区", "key", "backend", "phase"),
		contentionDesc: desc("contention_ratio", "累计撞锁比例(Busy/Attempts, 0~1);需区间竞争率请对 operations_total 做 rate() 推导", "key", "backend"),
		stateDesc:      desc("state", "当前状态: holding(0/1)、depth(重入深度)、fence(栅栏值, consulstore 恒 0)、closed(0/1)", "key", "backend", "field"),
		errDesc:        descNoLabels("collect_error", "抓取失败占位指标, 失败时经 InvalidMetric 上报"),
	}
}

// NewWithProvider 以自定义快照源建立采集器(装配层已把锁收进注册表的场景)。
func NewWithProvider(provider Provider) *Collector {
	c := New()
	c.provider = provider
	c.instances = nil
	return c
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(mustQualifiedName(name), help, labels, nil)
}

func descNoLabels(name, help string) *prometheus.Desc {
	return prometheus.NewDesc(mustQualifiedName(name), help, nil, nil)
}

func mustQualifiedName(name string) string {
	fq, err := metrics.QualifiedName(component, name)
	if err != nil {
		fq = metrics.DefaultNamespace + "_" + component + "_invalid"
	}
	return fq
}

// MetricsCollectors 满足 metrics.Exporter 约定,便于 RegisterExporter 收录。
func (c *Collector) MetricsCollectors() []prometheus.Collector {
	return []prometheus.Collector{c}
}

// Describe 导出全部指标描述符。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.opsDesc
	ch <- c.errorsDesc
	ch <- c.durationDesc
	ch <- c.contentionDesc
	ch <- c.stateDesc
}

// Collect 实时拉取快照并产出样本;取数失败转为 InvalidMetric,不击穿抓取方。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snaps, errs := c.pull()
	for _, err := range errs {
		ch <- prometheus.NewInvalidMetric(c.errDesc, err)
	}
	for key, s := range snaps {
		if s.Key != "" {
			key = s.Key
		}
		c.emit(ch, key, s)
	}
}

// pull 汇总快照来源。两类来源可同时使用(provider 优先返回的键会被实例覆盖),
// 但常规装配只用其一。单个实例取数失败不影响其余实例。
func (c *Collector) pull() (map[string]xmlock.Stats, []error) {
	var errs []error
	out := make(map[string]xmlock.Stats, len(c.instances))

	if c.provider != nil {
		snaps, err := safeProvider(c.provider)
		if err != nil {
			errs = append(errs, err)
		}
		for k, s := range snaps {
			out[k] = s
		}
	}

	for i, m := range c.instances {
		s, err := safeStats(m)
		if err != nil {
			errs = append(errs, fmt.Errorf("metrics/adapter/lock: 第 %d 个实例取快照失败: %w", i, err))
			continue
		}
		key := s.Key
		if key == "" {
			key = fmt.Sprintf("instance-%d", i)
		}
		out[key] = s
	}
	return out, errs
}

// emit 产出单个锁实例的全部样本。
func (c *Collector) emit(ch chan<- prometheus.Metric, key string, s xmlock.Stats) {
	backend := s.Backend
	if backend == "" {
		backend = "unknown"
	}
	labels := []string{key, backend}

	// 计数族一律产出(含 0 值),便于 rate() 推导;比例与时长族仅在确有数据时产出。
	acquire := []struct {
		result string
		value  uint64
	}{
		{"acquired", s.Acquired},
		{"reentered", s.Reentered},
		{"busy", s.Busy},
		{"timeout", s.Timeouts},
		{"lost", s.Lost},
		{"canceled", s.Canceled},
	}
	for _, r := range acquire {
		ch <- prometheus.MustNewConstMetric(c.opsDesc, prometheus.CounterValue, float64(r.value), append(labels, "acquire", r.result)...)
	}
	for _, r := range []struct {
		result string
		value  uint64
	}{
		{"full", s.Released},
		{"decreased", s.Decreased},
		{"rejected", s.ReleaseMisses},
	} {
		ch <- prometheus.MustNewConstMetric(c.opsDesc, prometheus.CounterValue, float64(r.value), append(labels, "release", r.result)...)
	}
	for _, r := range []struct {
		result string
		value  uint64
	}{
		{"ok", s.Renewals},
		{"failed", s.RenewFailures},
	} {
		ch <- prometheus.MustNewConstMetric(c.opsDesc, prometheus.CounterValue, float64(r.value), append(labels, "renew", r.result)...)
	}

	ch <- prometheus.MustNewConstMetric(c.errorsDesc, prometheus.CounterValue, float64(s.Errors), labels...)

	// 外部 provider 可能交出负时长, 夹持后再产出(负值会让 ConstMetric 构造 panic)
	ch <- prometheus.MustNewConstMetric(c.durationDesc, prometheus.CounterValue, nanosToSeconds(s.WaitNanos), append(labels, "wait")...)
	ch <- prometheus.MustNewConstMetric(c.durationDesc, prometheus.CounterValue, nanosToSeconds(s.HoldNanos), append(labels, "hold")...)

	if s.Attempts > 0 {
		ch <- prometheus.MustNewConstMetric(c.contentionDesc, prometheus.GaugeValue, s.ContentionRatio(), labels...)
	}

	ch <- prometheus.MustNewConstMetric(c.stateDesc, prometheus.GaugeValue, b2f(s.Holding), append(labels, "holding")...)
	ch <- prometheus.MustNewConstMetric(c.stateDesc, prometheus.GaugeValue, float64(s.Depth), append(labels, "depth")...)
	ch <- prometheus.MustNewConstMetric(c.stateDesc, prometheus.GaugeValue, float64(s.Fence), append(labels, "fence")...)
	ch <- prometheus.MustNewConstMetric(c.stateDesc, prometheus.GaugeValue, b2f(s.Closed), append(labels, "closed")...)
}

func nanosToSeconds(n int64) float64 {
	if n < 0 {
		n = 0
	}
	return float64(n) / float64(time.Second)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// safeProvider 调用 provider 并把 panic/nil 转为 error。
func safeProvider(provider Provider) (snaps map[string]xmlock.Stats, err error) {
	if provider == nil {
		return nil, fmt.Errorf("metrics/adapter/lock: provider 为 nil")
	}
	defer func() {
		if r := recover(); r != nil {
			snaps, err = nil, fmt.Errorf("metrics/adapter/lock: provider panic: %v", r)
		}
	}()
	return provider(), nil
}

// safeStats 取单个实例的快照, 隔离 panic(含「接口里装了类型化空指针」这类误用)。
func safeStats(m xmlock.Statser) (s xmlock.Stats, err error) {
	if m == nil {
		return s, fmt.Errorf("实例为 nil")
	}
	defer func() {
		if r := recover(); r != nil {
			s, err = xmlock.Stats{}, fmt.Errorf("Stats() panic: %v", r)
		}
	}()
	return m.Stats(), nil
}
