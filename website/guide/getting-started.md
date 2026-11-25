---
title: 快速引入
description: 榫卯（xm-utils/sunmao）各模块的引入方式与版本策略
---

# 快速引入

榫卯各组件均为独立 Go Module（Go 1.24.2+），按需 `go get`，互不耦合：

```bash
go get github.com/xm-utils/sunmao/gorm/database
go get github.com/xm-utils/sunmao/redis
go get github.com/xm-utils/sunmao/mq/core
go get github.com/xm-utils/sunmao/mq/redis
```

> 模块路径统一前缀 `github.com/xm-utils/sunmao/<模块目录>`；`mq` 族为两级路径（`mq/core`、`mq/redis`、`mq/kafka`、`mq/pipequeue`），顶层 `mq/` 仅为容器目录、自身不是模块。

## tag 与 pseudo-version

发布采用轻量 tag，命名 `<目录>/v<semver>`（两级目录同理，如 `mq/core/v0.1.0`）：

- **已发 tag 的模块**：可直接 `go get github.com/xm-utils/sunmao/<目录>@v0.1.0`；
- **未发 tag 的模块**：`go get` 会解析到 main 分支 pseudo-version（如 `v0.0.0-2026xxxx...`），接口冻结前可用，生产建议锁 commit。

各模块当前版本状态见[模块总览](/guide/modules)与[更新日志](/changelog)。

## go.work 本地联调

业务仓如需同时改动榫卯多个模块，可在本地 `go.work` 中 `use` sunmao 仓的模块目录联调；**`go.work` 不提交**，最终引用仍以 tag / pseudo-version 为准。仓内跨模块引用采用 pseudo-version、不留 `replace`。

## 文档站维护

本站文档唯一真源在榫卯 sunmao 仓，页面内容均为 `npm run sync` 生成物：

1. 在 sunmao 仓修改模块的 `README.md` / `QUICKSTART.md` / `config.example.yaml`；
2. 回到 website 执行 `npm run sync`（`dev` / `build` 已自动前置）；
3. 新增模块时同步更新 sunmao 仓 `go.work` 与 `scripts/sync-docs.mjs` 顶部的族分组和描述映射。
