package lock_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	xmlock "github.com/xm-utils/sunmao/lock"
	adapter "github.com/xm-utils/sunmao/metrics/adapter/lock"
)

// ExampleNew 把锁实例接进采集器:注册到 Prometheus 后,每次抓取实时拉一次 Stats()。
// 采集器只读 lock.Statser 契约,不包装、不改动锁本体,lock 组件对本包零依赖。
func ExampleNew() {
	mu, err := xmlock.New(xmlock.Config{Key: "deadletter:recover:order-pay"})
	if err != nil {
		fmt.Println("构造失败:", err)
		return
	}
	defer mu.Close()

	ctx := context.Background()
	_ = mu.Lock(ctx)
	_ = mu.Unlock(ctx)

	reg := prometheus.NewRegistry()
	if err := reg.Register(adapter.New(mu)); err != nil {
		fmt.Println("注册失败:", err)
		return
	}
	var families []string
	mfs, err := reg.Gather()
	if err != nil {
		fmt.Println("抓取失败:", err)
		return
	}
	for _, mf := range mfs {
		families = append(families, mf.GetName())
	}
	sort.Strings(families)
	fmt.Println(families)
	// Output:
	// [xmt_lock_contention_ratio xmt_lock_duration_seconds_total xmt_lock_errors_total xmt_lock_operations_total xmt_lock_state]
}
