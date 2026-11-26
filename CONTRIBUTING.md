# 开发规范（设计约定）

本文件是榫卯 sunmao 仓的**贡献者与开发约束**，不面向组件使用者。新增/扩展组件必须逐条对照执行；能力裁剪或启用须同步回写本文档与受影响组件的文档。

## 九条设计约定

1. **独立模块**：每个组件拥有独立 `go.mod`，可单独版本管理与引入，不强制拉取整个仓库；两级目录组件（如 `mq/*`）拆为同级独立模块，容器目录不设 `go.mod`。新建组件后同步加入仓根 `go.work`（该文件不提交）。
2. **panic 转 error**：对外 API 不向调用方抛 panic，内部 recover 后包装为 error 返回（`mq.InvokeHandler` 即标准手法）。
3. **三方库改动在调用方完成**：对 otel、prometheus 等 SDK 的适配写在本组件内，不 fork 三方库。
4. **适配器模式优先**：跨组件统一抽象一律「接口层新增 + 薄适配器」（`registry`、`mq`、`lock` 多后端皆此手法），不破坏被适配组件的既有 API。
5. **配置模板**：需要外部配置的组件提供 `config.example.yaml`，字段与结构体 yaml tag 一一对应（`httpclient` 属此类，可直接反序列化，时长写 `10s` 这类时长串）；`ratelimit`、`trace`、`lock` 根包不内置 YAML 解析（保持零仓外依赖或只依赖 otel），`grpcx` 只有 `ServiceConfig` 带 tag，其模板描述的是装配层同构 struct 的键位。
6. **文档配套**：README（完整 API 参考）+ QUICKSTART（5 分钟上手）；能力裁剪或启用须同步文档，不允许「代码已改、文档未动」合入。
7. **测试覆盖**：关键路径必须有单元测试，`go test -race ./...` 全绿方可合入；并发组件（`lock`、`ratelimit`、`pipeline`、`breaker`）必须含竞争与超时用例，且用例对时钟/调度是确定性的（不靠 `sleep` 赌结果）。
8. **向后兼容**：既有组件扩展只加 Option/接口，不改已发布函数签名；破坏性变更走版本号升级并在 README 标注迁移指南。
9. **发布与引用**：轻量 tag 命名 `<目录>/v<semver>`（两级目录同理，如 `mq/core/v0.0.2`）；仓内跨模块引用采用 pseudo-version、**不留 `replace`**，新模块接口冻结后尽快发正式 tag。

## 依赖策略

- 依赖方向只向下：设施 ← 契约 ← 适配 ← 治理 ← 入口，下层不得引用上层，同层不互依赖（唯一例外是 `metrics/adapter/*` 读取组件已公开的快照接口，组件本体零改动）。
- 外部 SDK 只进适配层子包：`lock/redisstore`、`lock/consulstore`、`ratelimit/redisstore`、`mq/redis`、`mq/kafka`、`registry/consul`、`registry/nacos`。契约层根包保持零第三方依赖。
- 抽象层刻意保持零/少依赖以避免模块互依赖与包级环——例如 `logging` 的调级配置源监听由**装配层**实现，不反向依赖 `registry/config`。
- 可观测性装配顺序：`trace` → `metrics` / `logging`（后两者仅依赖 `trace`）。

## 本地开发

```bash
git clone https://github.com/xm-utils/sunmao.git && cd sunmao
cd <模块目录>            # 各模块独立
go build ./... && go vet ./... && go test -race ./...
```

多模块联调时在仓根建 `go.work`（**不提交**）并 `use` 相关模块目录。合入前必须回到 `GOWORK=off` 再跑一遍 `build/vet/test`，确认该模块脱离工作区仍可独立构建——这是最容易漏的一步：工作区内 `replace` 生效，仓外引用却会编译失败。

## 需要真实环境的测试

依赖 Redis / Consul / Kafka / MySQL 的用例一律挂在构建标签下，环境缺失时 `t.Skip` 优雅跳过，绝不让默认 `go test ./...` 因为连不上外部服务而失败：

```go
//go:build integration
```

```bash
XM_LOCK_TEST_REDIS=127.0.0.1:6379 go test -tags integration -run TestIntegration ./...
```

## 跨模块引用刷新

`go mod tidy` **不会升级已有 requirement**（只要 import 路径在旧版本可解析就算满足），被依赖模块的新接口必须先推送远端，再在引用方显式升级：

```bash
GOWORK=off GOFLAGS=-mod=mod go get github.com/xm-utils/sunmao/<模块>@<tag 或 commit>
GOWORK=off GOFLAGS=-mod=mod go mod tidy && GOWORK=off go test -race ./...
```

- Proxy 对刚推送的分支 HEAD 有缓存时，用 `GOPRIVATE=<模块路径前缀> GOPROXY=direct` 直连 VCS 解析一次。
- 新发布的 tag 在校验库（sumdb）索引到位前 `go get` 会报 `verifying module: ... 404 Not Found`；此时加 `GONOSUMDB='<模块路径前缀>/*'` 让哈希直接落 `go.sum`（不要用 `GOPRIVATE`，它会连带关掉 proxy 通道）。`go.sum` 提交后，后续构建只比对 `go.sum`，不再依赖 sumdb。

## 文档站

文档唯一真源在本仓，站点页面全部是 `npm run sync` 生成物：

- 模块文档：改各模块的 `README.md` / `QUICKSTART.md` / `config.example.yaml`；
- 架构与组合：改仓根 `README.md` 的「架构设计 / 组件组合策略」两节；
- 新增模块：同步更新 `go.work` 与 `website/scripts/sync-docs.mjs` 顶部的族分组和描述映射。

本文件（开发规范）**只留在仓库内**，不同步到站点。
