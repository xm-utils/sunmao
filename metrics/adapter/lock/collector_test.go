package lock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	xmlock "github.com/xm-utils/sunmao/lock"
	"github.com/xm-utils/sunmao/metrics"
	adapter "github.com/xm-utils/sunmao/metrics/adapter/lock"
)

// gather 注册采集器到独立注册表并抓取, 返回按指标全名索引的 MetricFamily。
func gather(t *testing.T, c prometheus.Collector) (map[string]*dto.MetricFamily, error) {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	mfs, err := reg.Gather()
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out, err
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

func hasSample(mfs map[string]*dto.MetricFamily, family string, labels map[string]string) bool {
	mf, ok := mfs[family]
	if !ok {
		return false
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
			return true
		}
	}
	return false
}

// exercisedLocal 造一把有故事的进程内锁:重入、撞锁、超时、误释放各有若干。
func exercisedLocal(t *testing.T) *xmlock.Local {
	t.Helper()
	l, err := xmlock.New(xmlock.Config{
		Key: "deadletter:recover:order-pay", TTL: 5 * time.Second,
		WaitTimeout: 20 * time.Millisecond, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	ctx := context.Background()
	if err := l.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := l.Lock(ctx); err != nil {
		t.Fatalf("重入 Lock: %v", err)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("内层 Unlock: %v", err)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("外层 Unlock: %v", err)
	}

	// 换一个 goroutine 长期持有, 让主 goroutine 撞锁 + 超时 + 误释放
	held := make(chan error, 1)
	release := make(chan struct{})
	go func() {
		held <- l.Lock(ctx)
		<-release
		_ = l.Unlock(ctx)
	}()
	if err := <-held; err != nil {
		t.Fatalf("另一 goroutine Lock: %v", err)
	}
	if err := l.TryLock(ctx); !errors.Is(err, xmlock.ErrBusy) {
		t.Fatalf("应报 ErrBusy: %v", err)
	}
	if err := l.Lock(ctx); !errors.Is(err, xmlock.ErrTimeout) {
		t.Fatalf("应报 ErrTimeout: %v", err)
	}
	if err := l.Unlock(ctx); !errors.Is(err, xmlock.ErrNotHeld) {
		t.Fatalf("非属主释放应报 ErrNotHeld: %v", err)
	}
	t.Cleanup(func() { close(release) })
	return l
}

func TestCollectorFromInstances(t *testing.T) {
	l := exercisedLocal(t)
	mfs, err := gather(t, adapter.New(l))
	if err != nil {
		t.Fatalf("抓取失败: %v", err)
	}

	key := map[string]string{"key": "deadletter:recover:order-pay", "backend": "local"}
	op := func(o, r string) map[string]string {
		m := map[string]string{"op": o, "result": r}
		for k, v := range key {
			m[k] = v
		}
		return m
	}

	if v := findSample(t, mfs, "xmt_lock_operations_total", op("acquire", "acquired")); v != 2 {
		t.Errorf("acquired 期望 2(两轮各一次), 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("acquire", "reentered")); v != 1 {
		t.Errorf("reentered 期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("acquire", "busy")); v < 2 {
		t.Errorf("busy 至少 2(TryLock 一次 + 轮询若干轮), 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("acquire", "timeout")); v != 1 {
		t.Errorf("timeout 期望 1, 得 %v", v)
	}
	// 另一 goroutine 仍持有, 它那次完整释放发生在 Cleanup(Close) 里, 不在本次抓取范围内
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("release", "full")); v != 1 {
		t.Errorf("release full 期望 1(主 goroutine 那一轮), 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("release", "decreased")); v != 1 {
		t.Errorf("release decreased 期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("release", "rejected")); v != 1 {
		t.Errorf("release rejected 期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_operations_total", op("renew", "ok")); v != 0 {
		t.Errorf("Local 无续期, 得 %v", v)
	}

	if v := findSample(t, mfs, "xmt_lock_errors_total", key); v != 0 {
		t.Errorf("无后端故障时 errors 应为 0, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_duration_seconds_total", map[string]string{"key": "deadletter:recover:order-pay", "backend": "local", "phase": "wait"}); v <= 0 {
		t.Errorf("等待超时的回次也应累计时长, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_duration_seconds_total", map[string]string{"key": "deadletter:recover:order-pay", "backend": "local", "phase": "hold"}); v < 0 {
		t.Errorf("持有时长不应为负, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_contention_ratio", key); v <= 0 || v > 1 {
		t.Errorf("竞争率应落在 (0,1], 得 %v", v)
	}
	for _, field := range []string{"holding", "depth", "fence", "closed"} {
		if !hasSample(mfs, "xmt_lock_state", map[string]string{"key": "deadletter:recover:order-pay", "backend": "local", "field": field}) {
			t.Errorf("状态族缺少 field=%s 样本", field)
		}
	}
	if v := findSample(t, mfs, "xmt_lock_state", map[string]string{"key": "deadletter:recover:order-pay", "backend": "local", "field": "holding"}); v != 1 {
		t.Errorf("另一 goroutine 仍在持有, holding 期望 1, 得 %v", v)
	}
}

func TestCollectorPullsLatestSnapshot(t *testing.T) {
	l, err := xmlock.New(xmlock.Config{Key: "job:pull"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	c := adapter.New(l)
	before := findSample(t, mustGather(t, c), "xmt_lock_operations_total",
		map[string]string{"key": "job:pull", "backend": "local", "op": "acquire", "result": "acquired"})

	_ = l.TryLock(context.Background())
	_ = l.Unlock(context.Background())
	_ = l.TryLock(context.Background())
	_ = l.Unlock(context.Background())

	after := findSample(t, mustGather(t, c), "xmt_lock_operations_total",
		map[string]string{"key": "job:pull", "backend": "local", "op": "acquire", "result": "acquired"})
	if after != before+2 {
		t.Errorf("拉取式应反映最新计数, %v → %v", before, after)
	}
}

func mustGather(t *testing.T, c prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := gather(t, c)
	if err != nil {
		t.Fatalf("抓取失败: %v", err)
	}
	return mfs
}

func TestCollectorZeroSnapshotKeepsCounters(t *testing.T) {
	l, err := xmlock.New(xmlock.Config{Key: "job:cold"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	mfs := mustGather(t, adapter.New(l))
	// 计数族保留零值(便于 rate() 推导), 比例类无数据时不产出
	if v := findSample(t, mfs, "xmt_lock_operations_total",
		map[string]string{"key": "job:cold", "backend": "local", "op": "acquire", "result": "busy"}); v != 0 {
		t.Errorf("零值计数期望 0, 得 %v", v)
	}
	if _, ok := mfs["xmt_lock_contention_ratio"]; ok {
		t.Error("Attempts=0 不应产出竞争率")
	}
	if v := findSample(t, mfs, "xmt_lock_state", map[string]string{"key": "job:cold", "backend": "local", "field": "closed"}); v != 0 {
		t.Errorf("未关闭时 closed 期望 0, 得 %v", v)
	}
}

func TestCollectorFromProvider(t *testing.T) {
	provider := func() map[string]xmlock.Stats {
		return map[string]xmlock.Stats{
			"mapKey": {Key: "sharding:ddl:orders", Backend: "redis", Attempts: 4, Acquired: 1,
				Busy: 2, Timeouts: 1, Released: 1, Renewals: 3, WaitNanos: int64(2 * time.Second),
				HoldNanos: int64(1500 * time.Millisecond), Holding: true, Depth: 1, Fence: 7},
			"no-key-backend-empty": {Attempts: 1, Busy: 1},
		}
	}
	mfs := mustGather(t, adapter.NewWithProvider(provider))

	// 快照内 Key 优先于 map 键
	if v := findSample(t, mfs, "xmt_lock_operations_total",
		map[string]string{"key": "sharding:ddl:orders", "backend": "redis", "op": "renew", "result": "ok"}); v != 3 {
		t.Errorf("renew ok 期望 3, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_state",
		map[string]string{"key": "sharding:ddl:orders", "backend": "redis", "field": "fence"}); v != 7 {
		t.Errorf("fence 期望 7, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_duration_seconds_total",
		map[string]string{"key": "sharding:ddl:orders", "backend": "redis", "phase": "wait"}); v != 2 {
		t.Errorf("等待时长应换算为秒, 期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_lock_contention_ratio",
		map[string]string{"key": "sharding:ddl:orders", "backend": "redis"}); v != 0.5 {
		t.Errorf("竞争率期望 0.5, 得 %v", v)
	}
	// backend 缺省填 unknown, 避免空 label 让样本无法区分
	if !hasSample(mfs, "xmt_lock_operations_total",
		map[string]string{"key": "no-key-backend-empty", "backend": "unknown", "op": "acquire", "result": "busy"}) {
		t.Error("空 backend 应回退为 unknown 并保留样本")
	}
}

func TestCollectorClampsNegativeDurations(t *testing.T) {
	provider := func() map[string]xmlock.Stats {
		return map[string]xmlock.Stats{
			"bad": {Key: "bad", Attempts: 2, Busy: 9, WaitNanos: -5, HoldNanos: -7},
		}
	}
	mfs := mustGather(t, adapter.NewWithProvider(provider))
	for _, phase := range []string{"wait", "hold"} {
		if v := findSample(t, mfs, "xmt_lock_duration_seconds_total",
			map[string]string{"key": "bad", "backend": "unknown", "phase": phase}); v != 0 {
			t.Errorf("%s 负时长应夹持为 0, 得 %v", phase, v)
		}
	}
	if v := findSample(t, mfs, "xmt_lock_contention_ratio", map[string]string{"key": "bad", "backend": "unknown"}); v != 1 {
		t.Errorf("竞争率上限应为 1, 得 %v", v)
	}
}

// panicStatser 模拟自定义后端 Stats() 直接 panic 的情形
type panicStatser struct{}

func (panicStatser) Stats() xmlock.Stats { panic("snapshot boom") }

// typedNilStatser 模拟「接口里装了类型化空指针」
type typedNilStatser struct{}

func (typedNilStatser) Stats() xmlock.Stats {
	var l *xmlock.Local // nil 接收者 → 取字段 panic
	return l.Stats()
}

func TestCollectorPanicSafety(t *testing.T) {
	t.Run("provider panic 转为抓取错误", func(t *testing.T) {
		_, err := gather(t, adapter.NewWithProvider(func() map[string]xmlock.Stats {
			panic("provider boom")
		}))
		if err == nil {
			t.Fatal("provider panic 应转为抓取错误")
		}
	})

	t.Run("单实例 panic 不带走其他锁", func(t *testing.T) {
		var typed *xmlock.Local
		good, err := xmlock.New(xmlock.Config{Key: "job:good"})
		if err != nil {
			t.Fatal(err)
		}
		defer good.Close()

		mfs, gerr := gather(t, adapter.New(panicStatser{}, typedNilStatser{}, typed, good))
		if gerr == nil {
			t.Fatal("坏实例应上报抓取错误")
		}
		if !hasSample(mfs, "xmt_lock_operations_total",
			map[string]string{"key": "job:good", "backend": "local", "op": "acquire", "result": "acquired"}) {
			t.Error("健康实例的样本必须仍然产出")
		}
	})
}

func TestCollectorNoSourceProducesNothing(t *testing.T) {
	if mfs := mustGather(t, adapter.New()); len(mfs) != 0 {
		t.Errorf("既无实例也无 provider 应无输出, 得 %d 个指标族", len(mfs))
	}
}

func TestMetricsCollectorsExporter(t *testing.T) {
	// 用稳定闭包而非会被 Close 的实例: -count>1 时全局注册表保留首次采集器
	provider := func() map[string]xmlock.Stats {
		return map[string]xmlock.Stats{"exporter:key": {Key: "exporter:key", Backend: "consul", Attempts: 3, Busy: 1}}
	}
	c := adapter.NewWithProvider(provider)
	if got := c.MetricsCollectors(); len(got) != 1 || got[0] != prometheus.Collector(c) {
		t.Errorf("MetricsCollectors 应返回自身, 得 %v", got)
	}

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
		if mf.GetName() == "xmt_lock_operations_total" && len(mf.GetMetric()) > 0 {
			found = true
		}
	}
	if !found {
		t.Error("统一注册表中缺少 xmt_lock_operations_total 样本")
	}
}
