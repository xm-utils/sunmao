package ratelimit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/xm-utils/sunmao/metrics"
	adapter "github.com/xm-utils/sunmao/metrics/adapter/ratelimit"
	xmratelimit "github.com/xm-utils/sunmao/ratelimit"
)

// gather 注册采集器到独立注册表并抓取，返回按指标全名索引的 MetricFamily。
func gather(t *testing.T, c prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("抓取失败: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

// findSample 在指定指标族中按 label 精确查找样本值。
func findSample(t *testing.T, mfs map[string]*dto.MetricFamily, family string, labels map[string]string) float64 {
	t.Helper()
	mf, ok := mfs[family]
	if !ok {
		t.Fatalf("缺少指标族 %s", family)
	}
	for _, m := range mf.GetMetric() {
		match := true
		for k, v := range labels {
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == k && lp.GetValue() == v {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("指标 %s 中未找到 label %v 的样本", family, labels)
	return 0
}

func TestCollectorFromLocal(t *testing.T) {
	local, err := xmratelimit.NewLocal(xmratelimit.LocalConfig{Rate: 10, Burst: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	ctx := context.Background()
	// 2 放行 + 2 拒绝 → 拒绝率 0.5
	for i := 0; i < 2; i++ {
		if err := local.Allow(ctx, "api:order:create"); err != nil {
			t.Fatalf("预置放行失败: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := local.Allow(ctx, "api:order:create"); !errors.Is(err, xmratelimit.ErrRateLimited) {
			t.Fatalf("应被限流, 得 %v", err)
		}
	}

	mfs := gather(t, adapter.New(local))
	key := map[string]string{"key": "api:order:create"}

	if v := findSample(t, mfs, "xmt_ratelimit_requests_total", map[string]string{"key": "api:order:create", "result": "allowed"}); v != 2 {
		t.Errorf("allowed 计数期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_ratelimit_requests_total", map[string]string{"key": "api:order:create", "result": "rejected"}); v != 2 {
		t.Errorf("rejected 计数期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_ratelimit_rejection_rate", key); v != 0.5 {
		t.Errorf("拒绝率期望 0.5, 得 %v", v)
	}
	// 令牌已耗尽
	if v := findSample(t, mfs, "xmt_ratelimit_tokens_available", key); v > 0.1 {
		t.Errorf("水位期望接近 0, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_ratelimit_rule_rate", key); v != 10 {
		t.Errorf("速率期望 10, 得 %v", v)
	}
}

func TestCollectorPullsLatestSnapshot(t *testing.T) {
	local, _ := xmratelimit.NewLocal(xmratelimit.LocalConfig{Rate: 10, Burst: 5})
	defer local.Close()
	c := adapter.New(local)
	ctx := context.Background()

	_ = local.Allow(ctx, "k")
	v1 := findSample(t, gather(t, c), "xmt_ratelimit_requests_total", map[string]string{"key": "k", "result": "allowed"})

	_ = local.Allow(ctx, "k")
	_ = local.Allow(ctx, "k")
	v2 := findSample(t, gather(t, c), "xmt_ratelimit_requests_total", map[string]string{"key": "k", "result": "allowed"})

	if v2 != v1+2 {
		t.Errorf("拉取式应反映最新计数, %v → %v", v1, v2)
	}
}

func TestCollectorNoJudgmentSkipsRate(t *testing.T) {
	// 从未判定过的 Key 不进入快照，也就无任何样本
	provider := func() map[string]xmratelimit.KeySnapshot { return nil }
	mfs := gather(t, adapter.NewWithProvider(provider))
	if len(mfs) != 0 {
		t.Errorf("空快照应无输出, 得 %d 个指标族", len(mfs))
	}
}

func TestCollectorZeroTotalKeepsCounters(t *testing.T) {
	provider := func() map[string]xmratelimit.KeySnapshot {
		return map[string]xmratelimit.KeySnapshot{"cold": {Key: "cold"}}
	}
	mfs := gather(t, adapter.NewWithProvider(provider))
	// 计数族保留零值（便于 rate() 推导），速率/水位类无数据时不产出
	if v := findSample(t, mfs, "xmt_ratelimit_requests_total", map[string]string{"key": "cold", "result": "rejected"}); v != 0 {
		t.Errorf("零值计数期望 0, 得 %v", v)
	}
	if _, ok := mfs["xmt_ratelimit_rejection_rate"]; ok {
		t.Error("Total=0 不应产出拒绝率")
	}
	if _, ok := mfs["xmt_ratelimit_tokens_available"]; ok {
		t.Error("Total=0 不应产出水位")
	}
	if _, ok := mfs["xmt_ratelimit_rule_rate"]; ok {
		t.Error("Limit=0 不应产出生效速率")
	}
}

func TestCollectorSnapshotKeyOverridesMapKey(t *testing.T) {
	provider := func() map[string]xmratelimit.KeySnapshot {
		return map[string]xmratelimit.KeySnapshot{"mapKey": {Key: "real:key", Total: 4, Rejected: 1}}
	}
	mfs := gather(t, adapter.NewWithProvider(provider))
	findSample(t, mfs, "xmt_ratelimit_requests_total", map[string]string{"key": "real:key", "result": "allowed"})
}

func TestCollectorClampsInconsistentSnapshot(t *testing.T) {
	// 自定义 provider 交出 Rejected > Total、Remaining < 0 时夹持为 0，不得 panic
	provider := func() map[string]xmratelimit.KeySnapshot {
		return map[string]xmratelimit.KeySnapshot{"bad": {Key: "bad", Total: 2, Rejected: 5, Remaining: -3, Limit: 1}}
	}
	mfs := gather(t, adapter.NewWithProvider(provider))
	if v := findSample(t, mfs, "xmt_ratelimit_requests_total", map[string]string{"key": "bad", "result": "allowed"}); v != 0 {
		t.Errorf("allowed 应夹持为 0, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_ratelimit_tokens_available", map[string]string{"key": "bad"}); v != 0 {
		t.Errorf("水位应夹持为 0, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_ratelimit_rejection_rate", map[string]string{"key": "bad"}); v != 1 {
		t.Errorf("拒绝率上限应为 1, 得 %v", v)
	}
}

func TestCollectorPanicSafety(t *testing.T) {
	// provider panic 转为抓取错误，不击穿抓取方
	reg := prometheus.NewRegistry()
	if err := reg.Register(adapter.NewWithProvider(func() map[string]xmratelimit.KeySnapshot {
		panic("snapshot boom")
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Gather(); err == nil {
		t.Fatal("provider panic 应转为抓取错误（Gather 返回 non-fatal error）")
	}
}

func TestMetricsCollectorsExporter(t *testing.T) {
	// 用稳定闭包而非会 Close 的 Local：-count>1 时全局注册表保留首次采集器，
	// 采集器必须仍可产出样本
	provider := func() map[string]xmratelimit.KeySnapshot {
		return map[string]xmratelimit.KeySnapshot{"exporter:key": {Key: "exporter:key", Total: 3, Rejected: 1, Remaining: 2, Limit: 10}}
	}
	c := adapter.NewWithProvider(provider)
	if got := c.MetricsCollectors(); len(got) != 1 || got[0] != prometheus.Collector(c) {
		t.Errorf("MetricsCollectors 应返回自身, 得 %v", got)
	}

	// 满足 Exporter 约定后可被统一注册表收录（-count>1 时重复注册视为通过）
	if err := metrics.RegisterExporter(c); err != nil {
		var dup prometheus.AlreadyRegisteredError
		if !errors.As(err, &dup) {
			t.Fatalf("RegisterExporter 失败: %v", err)
		}
	}
	mfs, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("统一注册表抓取失败: %v", err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "xmt_ratelimit_requests_total" && len(mf.GetMetric()) > 0 {
			found = true
		}
	}
	if !found {
		t.Error("统一注册表中缺少 xmt_ratelimit_requests_total 样本")
	}
}
