---
layout: home

hero:
  name: 榫卯
  text: Go 微服务基础设施组件集
  tagline: 可观测性三件套贯通 · 门面+适配器治理抽象 · 22 个独立模块按需引入
  actions:
    - theme: brand
      text: 开始使用
      link: /guide/getting-started
    - theme: alt
      text: 模块文档
      link: /docs/retry/
    - theme: alt
      text: GitHub
      link: https://github.com/xm-utils/sunmao.git

features:
  - title: 可观测性族
    details: trace（OTel 贯通）· metrics（xmt_ 统一出口）· logging（traceId 自动附加）。一次调用链全链路可见。
    link: /docs/trace/
  - title: 服务治理族
    details: ratelimit（三算法双后端）· breaker（熔断+降级）· retry（非阻塞重试）。入口防护与出口弹性齐备。
    link: /docs/ratelimit/
  - title: 通信与服务发现族
    details: grpcx（cmux 端口复用）· httpclient · registry（统一抽象）· consul / nacos 双适配器。
    link: /docs/registry/
  - title: 消息与异步编排族
    details: mq 门面四模块（core/kafka/redis/pipequeue）· pipeline 执行引擎 · deadletter 死信。零改动切换后端。
    link: /docs/mq/core/
  - title: 数据访问族
    details: gorm/database（含 sharding）· beego/database · redis 全数据结构 · mongodb。
    link: /docs/gorm/database/
  - title: 架构与组合
    details: 分层与依赖铁律 · 三条运行主线 · 五个典型装配配方与选型对照。拿来即用的组合策略。
    link: /guide/architecture
---
