// Package ratelimit 把 ratelimit 组件的私有观测快照（Local.Snapshots 的
// 按 Key 判定计数与令牌水位）适配为统一注册表可收录的 prometheus.Collector。
//
// 拉取式设计：每次抓取时实时读快照，不改动、不包装限流后端本体；
// ratelimit 组件零依赖本包（适配器模式，见路线图 3.1.2 兼容策略）。
// panic 安全：provider panic 兜底为抓取错误（InvalidMetric），不击穿抓取方。
package ratelimit

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xm-utils/sunmao/metrics"
	xmratelimit "github.com/xm-utils/sunmao/ratelimit"
)

const component = "ratelimit"

// Provider 快照提供方，通常传 local.Snapshots；自持限流器 map 的场景可自行组装。
type Provider func() map[string]xmratelimit.KeySnapshot

// Collector 实现 prometheus.Collector，输出 xmt_ratelimit_* 指标，
// key 维度为限流规则 Key（如 "api:order:create"）。
// 注册方式：metrics.Register(c) 或 metrics.RegisterExporter(c)。
type Collector struct {
	provider Provider

	requestsDesc *prometheus.Desc // xmt_ratelimit_requests_total{key,result}
	rateDesc     *prometheus.Desc // xmt_ratelimit_rejection_rate{key}
	tokensDesc   *prometheus.Desc // xmt_ratelimit_tokens_available{key}
	ruleDesc     *prometheus.Desc // xmt_ratelimit_rule_rate{key}
	errDesc      *prometheus.Desc // 抓取失败占位（InvalidMetric 用）
}

var _ prometheus.Collector = (*Collector)(nil)

// New 为本地内存后端建立采集器，快照实时拉取。
func New(l *xmratelimit.Local) *Collector {
	return NewWithProvider(l.Snapshots)
}

// NewWithProvider 以自定义快照源建立采集器（多后端聚合、自持 SlidingWindow/
// LeakyBucket 实例等场景）；provider 返回 map 的 key 与快照内 Key 字段不一致时，
// 以快照内 Key 为准。
func NewWithProvider(provider Provider) *Collector {
	return &Collector{
		provider:     provider,
		requestsDesc: desc("requests_total", "限流判定累计计数，按结果维度 allowed/rejected 区分（进程启动以来累计）", "key", "result"),
		rateDesc:     desc("rejection_rate", "累计拒绝率（0~1）；需区间拒绝率时对本指标与 requests_total 做 rate() 推导", "key"),
		tokensDesc:   desc("tokens_available", "当前可用令牌数（令牌桶类快照）", "key"),
		ruleDesc:     desc("rule_rate", "当前生效速率（个/秒）", "key"),
		errDesc:      descNoLabels("collect_error", "抓取失败占位指标，失败时经 InvalidMetric 上报"),
	}
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

// MetricsCollectors 满足 metrics.Exporter 约定，便于 RegisterExporter 收录。
func (c *Collector) MetricsCollectors() []prometheus.Collector {
	return []prometheus.Collector{c}
}

// Describe 导出全部指标描述符。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.requestsDesc
	ch <- c.rateDesc
	ch <- c.tokensDesc
	ch <- c.ruleDesc
}

// Collect 实时拉取快照并产出样本；nil/panic 转为 InvalidMetric。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snaps, err := safeSnapshots(c.provider)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.errDesc, err)
		return
	}

	for key, s := range snaps {
		if s.Key != "" {
			key = s.Key
		}
		// 自定义 provider 可能交出回退计数，夹持后再产出（负值会让 ConstMetric 构造 panic）
		if s.Rejected > s.Total {
			s.Total = s.Rejected
		}
		if s.Remaining < 0 {
			s.Remaining = 0
		}
		allowed := s.Total - s.Rejected
		ch <- prometheus.MustNewConstMetric(c.requestsDesc, prometheus.CounterValue, float64(allowed), key, "allowed")
		ch <- prometheus.MustNewConstMetric(c.requestsDesc, prometheus.CounterValue, float64(s.Rejected), key, "rejected")

		if s.Total > 0 {
			ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue,
				float64(s.Rejected)/float64(s.Total), key)
			ch <- prometheus.MustNewConstMetric(c.tokensDesc, prometheus.GaugeValue, s.Remaining, key)
		}
		if s.Limit > 0 {
			ch <- prometheus.MustNewConstMetric(c.ruleDesc, prometheus.GaugeValue, s.Limit, key)
		}
	}
}

// safeSnapshots 调用 provider 并把外部依赖引发的 panic 转为 error。
func safeSnapshots(provider Provider) (snaps map[string]xmratelimit.KeySnapshot, err error) {
	if provider == nil {
		return nil, fmt.Errorf("metrics/adapter/ratelimit: provider 为 nil")
	}
	defer func() {
		if r := recover(); r != nil {
			snaps, err = nil, fmt.Errorf("metrics/adapter/ratelimit: Snapshots panic: %v", r)
		}
	}()
	return provider(), nil
}
