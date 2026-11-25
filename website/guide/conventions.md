---
title: 设计约定
description: 榫卯（xm-utils/sunmao）仓库九条设计约束与依赖策略
---

# 设计约定

榫卯新增/扩展组件必须遵循的仓库既有约定（真源见 sunmao 仓 `ARCHITECTURE_ROADMAP.md` 第六章）：

1. **独立模块**：每个组件独立 `go.mod`，可单独 `go get`；新建后同步加入根目录 `go.work`（不提交）。两级目录组件（如 `mq/*`）为同级独立模块，容器目录不设 go.mod。
2. **panic 转 error**：对外 API 不向调用方抛 panic，内部 recover 后包装为 error 返回（`mq.InvokeHandler` 即标准手法）。
3. **三方库改动在调用方完成**：对 otel、prometheus 等 SDK 的适配写在本组件内，不 fork 三方库。
4. **适配器模式优先**：跨组件统一抽象一律「接口层新增 + 薄适配器」（registry、mq 皆此手法），不破坏被适配组件的既有 API。
5. **配置模板**：需要外部配置的组件提供 `config.example.yaml`，字段与结构体 yaml tag 一一对应。
6. **文档配套**：README（API 参考）+ QUICKSTART（5 分钟上手），能力裁剪/启用须同步文档。
7. **测试覆盖**：关键路径单测，`go test -race ./...` 全绿方可合入；并发组件（lock/scheduler/ratelimit）必须含竞争与超时用例。
8. **向后兼容**：既有组件扩展只加 Option/接口，不改已发布函数签名；破坏性变更走版本号升级并在 README 标注迁移指南。
9. **发布与引用**：轻量 tag 命名 `<目录>/v<semver>`（两级目录同理，如 `mq/core/v0.0.1`）；仓内跨模块引用采用 pseudo-version、不留 replace，新模块接口冻结后尽快发正式 tag。

## 依赖策略

- 仓内模块依赖关系见[仓内依赖关系表](/guide/dependencies)；
- 抽象层（`registry`、`mq/core`、`logging`）刻意保持零/少依赖，避免包级环与模块互依赖——如 `logging` 的调级配置源监听由装配层实现，不反向依赖 `registry/config`；
- 可观测性装配顺序：`trace` → `metrics` / `logging`（后两者仅依赖 trace）。
