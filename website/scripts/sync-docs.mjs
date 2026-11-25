#!/usr/bin/env node
// sync-docs：从 sunmao 仓同步文档到 VitePress 站点。
// 唯一真源在 sunmao 仓，本脚本只负责搬运与改写（契约见 ARCHITECTURE_ROADMAP.md 7.3）。
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = process.env.SUNMAO_REPO || process.env.TOOLS_REPO // TOOLS_REPO 为旧仓名环境变量，保留兼容
  ? path.resolve(process.env.SUNMAO_REPO || process.env.TOOLS_REPO)
  : path.resolve(siteRoot, '..');

const REPO = 'github.com/xm-utils/sunmao';
const BLOB = `https://${REPO}/blob/main`;
const INTERNAL_PREFIX = `${REPO}/`;

// —— 族分组（ARCHITECTURE_ROADMAP 7.4，合计 22 个模块，覆盖无遗漏）——
const GROUPS = [
  { name: '可观测性', modules: ['trace', 'metrics', 'logging'] },
  { name: '服务治理', modules: ['ratelimit', 'breaker', 'retry'] },
  { name: '通信与服务发现', modules: ['grpcx', 'httpclient', 'registry', 'consul', 'nacos'] },
  { name: '消息与异步编排', modules: ['mq/core', 'mq/kafka', 'mq/redis', 'mq/pipequeue', 'pipeline', 'deadletter'] },
  { name: '数据访问', modules: ['gorm/database', 'beego/database', 'redis', 'mongodb'] },
  { name: '基础工具', modules: ['common'] },
];

// —— 一句话描述：复用仓根 README.md 总览表；logging/metrics/trace 三行该表未收录、取自路线图 ——
const DESCRIPTIONS = {
  trace: 'OpenTelemetry 链路追踪核心封装，grpcx/httpclient/mq 族上下文贯通',
  metrics: '统一指标注册表：xmt_ 命名规范 + /metrics Handler + 组件拉取式适配器',
  logging: 'logrus JSON 统一日志：traceId 自动附加、文件轮转、可调级标准抽象',
  ratelimit: '限流与降级：令牌桶/滑动窗口/漏桶、本地与 Redis 双后端、gRPC/HTTP 入口中间件',
  breaker: '熔断器：滑动窗口指标、三态状态机、告警与 Fallback 降级钩子',
  retry: '非阻塞重试执行器：多策略（固定/指数/线性）、批量并发、监控回调',
  grpcx: 'gRPC 服务工具：Server/Client 快速构建、cmux 端口复用、优雅停机',
  httpclient: 'HTTP 客户端：连接池、JSON/表单/文件上传、请求追踪',
  registry: '注册/配置中心统一抽象：consul/nacos 双适配器、实例 Watch、配置热更新',
  consul: 'Consul SDK 封装：服务注册/发现、健康检查、KV 配置',
  nacos: 'Nacos SDK 封装：配置管理、服务发现（闭包模式独立客户端）',
  'mq/core': '消息队列统一门面（纯契约、零第三方依赖）：Message/Handler/Producer/Consumer',
  'mq/kafka': 'Kafka 侧适配器：手动 offset 生产/消费与额度内重投，含 client 客户端本体',
  'mq/redis': 'Redis 适配器四形态：list 安全队列 / zset 延迟队列 / stream 消费者组 / pubsub 广播',
  'mq/pipequeue': 'pipeline 引擎 × MQ 门面桥接队列：推送→拉取转换、有界缓冲背压',
  pipeline: '泛型消息消费执行引擎：步骤链编排、错误分级、步骤超时、有界停机',
  deadletter: '死信队列：自定义持久化（GORM）、自动恢复重试、实时监控告警',
  'gorm/database': 'GORM 数据库工具库：泛型 CRUD、读写分离、条件构建器、事务',
  'beego/database': 'Beego ORM 数据库工具库（兼容旧项目）',
  redis: 'Redis 客户端封装：全数据结构操作（String/Hash/List/Set/ZSet/Stream）',
  mongodb: 'MongoDB 客户端：连接管理、泛型 CRUD、聚合管道',
  common: '通用工具函数：JWT、RSA、密码哈希、统一响应体、时间/字符串工具',
};

const ALL_MODULE_DIRS = GROUPS.flatMap((g) => g.modules);
const groupOf = (dir) => GROUPS.find((g) => g.modules.includes(dir))?.name ?? '其他';

function git(args) {
  try {
    return execFileSync('git', ['-C', repoRoot, ...args], { encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
}

// —— 1. 模块清单：解析 go.work 的 use 块 ——
function collectModuleDirs() {
  const text = fs.readFileSync(path.join(repoRoot, 'go.work'), 'utf8');
  const dirs = new Set();
  const block = text.match(/use\s*\(([\s\S]*?)\)/);
  if (block) {
    for (const line of block[1].split('\n')) {
      const s = line.trim();
      if (s.startsWith('./')) dirs.add(s.slice(2));
    }
  }
  for (const m of text.matchAll(/^use\s+\.\/(\S+)\s*$/gm)) dirs.add(m[1]);
  return [...dirs];
}

// —— 2. 逐模块采集：go.mod（go 指令 + 仓内依赖）/ 文档存在性 / 轻量 tag ——
// 仓内模块路径匹配（如 github.com/xm-utils/sunmao/mq/core）
const MODULE_RE = new RegExp(REPO.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '/([A-Za-z0-9_/-]+)');

function parseGoMod(dir) {
  const file = path.join(repoRoot, dir, 'go.mod');
  const info = { goVersion: '', deps: [] };
  if (!fs.existsSync(file)) return info;
  const addDep = (dep) => {
    dep = dep.replace(/\/+$/, '');
    if (dep !== dir && ALL_MODULE_DIRS.includes(dep) && !info.deps.includes(dep)) info.deps.push(dep);
  };
  let inBlock = false;
  for (const raw of fs.readFileSync(file, 'utf8').split('\n')) {
    const line = raw.trim();
    if (/^go\s+\d/.test(line)) info.goVersion = line.split(/\s+/)[1];
    if (/^(require|replace)\s*\($/.test(line)) { inBlock = true; continue; }
    if (line === ')') { inBlock = false; continue; }
    // 单行 require/replace 与块内裸模块行统一按“出现即收集”处理
    const m = line.match(MODULE_RE);
    if (m) addDep(m[1]);
    else if (inBlock && /^\S+\s+github\.com\//.test(line)) {
      const t = line.match(MODULE_RE);
      if (t) addDep(t[1]);
    }
  }
  return info;
}

function collectModule(dir) {
  const root = path.join(repoRoot, dir);
  const has = (f) => fs.existsSync(path.join(root, f));
  const tagList = git(['tag', '-l', `${dir}/*`, '--sort=-v:refname']).split('\n').filter(Boolean);
  const latest = tagList[0] ?? '';
  const tags = tagList.map((t) => ({
    tag: t,
    version: t.slice(dir.length + 1),
    date: git(['log', '-1', '--format=%cs', `refs/tags/${t}`]),
  }));
  const go = parseGoMod(dir);
  return {
    dir,
    name: dir.split('/').pop(),
    path: `${INTERNAL_PREFIX}${dir}`,
    group: groupOf(dir),
    description: DESCRIPTIONS[dir] ?? '',
    goVersion: go.goVersion,
    deps: go.deps,
    version: latest ? tags[0].version : '未发布',
    tagDate: latest ? tags[0].date : null,
    unpublished: !latest,
    tags,
    hasReadme: has('README.md'),
    hasQuickstart: has('QUICKSTART.md'),
    hasConfig: has('config.example.yaml'),
  };
}

// —— 3. Markdown 链接改写（README.md→站内 index、QUICKSTART.md→quickstart、
//        config.example.yaml→config 页；站内不存在→blob；锚点与 http(s) 原样保留）——
// 返回新 url；undefined 表示原样保留
function rewriteLink(url, srcDir) {
  if (/^(https?:|mailto:|#|\/)/.test(url)) return undefined; // 原样保留
  const [rawPath, hash] = url.split('#');
  const anchor = hash ? `#${hash}` : '';
  if (!rawPath) return undefined;
  const abs = path.resolve(repoRoot, srcDir, rawPath);
  let rel = path.relative(repoRoot, abs).split(path.sep).join('/');
  if (rel.startsWith('..')) {
    // 指向仓外的相对路径（如 README 里的 ../../issues）→ 按仓库主页补全
    return `https://${REPO}/${rel.replace(/^(?:\.\.\/)+/, '')}${anchor}`;
  }

  if (rel === 'README.md' || rel.endsWith('/README.md')) {
    const m = rel.replace(/\/?README\.md$/, '');
    if (ALL_MODULE_DIRS.includes(m)) return `/docs/${m}/${anchor}`;
  }
  if (rel.endsWith('/QUICKSTART.md')) {
    const m = rel.replace(/\/QUICKSTART\.md$/, '');
    if (ALL_MODULE_DIRS.includes(m)) return `/docs/${m}/quickstart${anchor}`;
  }
  const cfg = rel.match(/^(.*)\/config\.example\.yaml$/);
  if (cfg && ALL_MODULE_DIRS.includes(cfg[1])) return `/docs/${cfg[1]}/config${anchor}`;

  return `${BLOB}/${rel}${anchor}`;
}

// 支持 URL 中的圆括号（badge 链接 [![x]( shields )](LICENSE) 的外层部分）与 title
const LINK_RE = /(!?\[(?:[^\[\]]|\[[^\]]*\])*\])\(((?:[^()\s]|\([^()]*\))+)(?:\s+"[^"]*")?\)/g;

function rewriteMarkdown(md, srcRelDir) {
  return md.replace(LINK_RE, (all, label, url) => {
    const next = rewriteLink(url, srcRelDir);
    return next === undefined ? all : `${label}(${next})`;
  });
}

function frontmatter(title, description) {
  return `---\ntitle: ${title}\ndescription: ${String(description).replace(/"/g, '')}\n---\n\n`;
}

function renderDoc(moduleDir, fileName, title, description) {
  const raw = fs.readFileSync(path.join(repoRoot, moduleDir, fileName), 'utf8');
  const body = raw.replace(/^\s*---\n[\s\S]*?\n---\n/, ''); // 去源 frontmatter（如有）
  return frontmatter(title, description) + rewriteMarkdown(body, moduleDir);
}

function placeholderPage(mod) {
  return (
    frontmatter(mod.name, `${mod.dir} 文档待回补`) +
    `# ${mod.dir}\n\n` +
    '::: warning 文档待回补\n该模块在 sunmao 仓中尚无 `README.md`，本页为 sync-docs 自动生成的占位页。\n:::\n\n' +
    `- **模块路径**：\`${mod.path}\`\n` +
    `- **所属族**：${mod.group}\n` +
    `- **简介**：${mod.description}\n` +
    `- **版本**：${mod.unpublished ? '未发布，走 pseudo-version' : `${mod.version}（${mod.tagDate}）`}\n\n` +
    `源码入口：[go.mod](${BLOB}/${mod.dir}/go.mod)\n`
  );
}

const CONFIG_PREAMBLE =
  '> 本模板字段与各组件配置结构体的 `yaml` tag 一一对应；零依赖组件（如 `ratelimit`）描述的是装配层同构 struct 的键位。\n' +
  '> 模板真源为 `config.example.yaml`，由 sync-docs 自动搬运，请勿在站点内直接修改。\n\n';

// —— 4. 生成 ——
function writeFile(rel, content) {
  const abs = path.join(siteRoot, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content.endsWith('\n') ? content : content + '\n');
}

if (!fs.existsSync(path.join(repoRoot, 'go.work'))) {
  console.error(`[sync] 未找到 sunmao 仓：${repoRoot}（可用 SUNMAO_REPO 环境变量指定）`);
  process.exit(1);
}

const dirs = collectModuleDirs();
const unknown = dirs.filter((d) => !ALL_MODULE_DIRS.includes(d));
if (unknown.length) console.warn('[sync] go.work 中存在未分组模块，请更新 GROUPS：', unknown);

const modules = dirs.map(collectModule);
const byDir = Object.fromEntries(modules.map((m) => [m.dir, m]));
const repoHead = git(['rev-parse', '--short', 'HEAD']);
let pageCount = 0;

fs.rmSync(path.join(siteRoot, 'docs'), { recursive: true, force: true });

for (const m of modules) {
  const outDir = `docs/${m.dir}`;
  if (m.hasReadme) {
    writeFile(`${outDir}/index.md`, renderDoc(m.dir, 'README.md', m.name, m.description));
  } else {
    writeFile(`${outDir}/index.md`, placeholderPage(m));
  }
  pageCount++;
  if (m.hasQuickstart) {
    writeFile(`${outDir}/quickstart.md`, renderDoc(m.dir, 'QUICKSTART.md', `${m.name} 快速开始`, `${m.dir} 5 分钟上手指南`));
    pageCount++;
  }
  if (m.hasConfig) {
    const yaml = fs.readFileSync(path.join(repoRoot, m.dir, 'config.example.yaml'), 'utf8');
    writeFile(
      `${outDir}/config.md`,
      frontmatter(`${m.name} 配置模板`, `${m.dir} config.example.yaml`) +
        `# ${m.name} 配置模板\n\n${CONFIG_PREAMBLE}\`\`\`yaml\n${yaml}\n\`\`\`\n`
    );
    pageCount++;
  }
}

// .vitepress/modules.json
writeFile('.vitepress/modules.json', JSON.stringify({
  generatedAt: new Date().toISOString(),
  repoHead,
  repo: `https://${REPO}`,
  groups: GROUPS.map((g) => ({ name: g.name, modules: g.modules.filter((d) => byDir[d]) })),
  modules,
}, null, 2));

// guide/modules.md
writeFile('guide/modules.md',
  frontmatter('模块总览', `全部模块清单与文档入口（真源：${REPO}）`) +
  '# 模块总览\n\n> 本页由 sync-docs 自动生成，勿手工编辑。\n\n' +
  '| 模块 | 族 | 说明 | 版本 | 更新时间 | Go | 文档 |\n|---|---|---|---|---|---|---|\n' +
  modules.map((m) =>
    `| \`${m.dir}\` | ${m.group} | ${m.description} | ${m.unpublished ? '未发布（pseudo-version）' : `\`${m.version}\``} | ${m.tagDate ?? '—'} | ${m.goVersion || '—'} | [README](/docs/${m.dir}/)${m.hasQuickstart ? ` · [快速开始](/docs/${m.dir}/quickstart)` : ''}${m.hasConfig ? ` · [配置](/docs/${m.dir}/config)` : ''} |`
  ).join('\n'));

// guide/dependencies.md
const depRows = modules.flatMap((m) =>
  m.deps.length ? m.deps.map((d) => `| \`${m.dir}\` | [\`${d}\`](/docs/${d}/) |`) : []
);
writeFile('guide/dependencies.md',
  frontmatter('仓内依赖关系', 'sunmao 仓各模块间的依赖矩阵') +
  '# 仓内依赖关系\n\n> 取自各模块 `go.mod` 的 require/replace（pseudo-version 与 `replace ../x` 均计入），由 sync-docs 自动生成。\n\n' +
  (depRows.length
    ? `| 模块 | 依赖（仓内） |\n|---|---|\n${depRows.join('\n')}\n\n### 无仓内依赖的模块\n\n${modules.filter((m) => !m.deps.length).map((m) => `\`${m.dir}\``).join('、')}\n`
    : '当前各模块 `go.mod` 未发现仓内交叉依赖。'));

// changelog.md
const changelogSections = modules
  .filter((m) => m.tags.length)
  .map((m) =>
    `## \`${m.dir}\`\n\n| 版本 | tag | 发布日期 |\n|---|---|---|\n` +
    m.tags.map((t) => `| ${t.version} | \`${t.tag}\` | ${t.date} |`).join('\n')
  );
const unpublished = modules.filter((m) => !m.tags.length).map((m) => `\`${m.dir}\``);
writeFile('changelog.md',
  frontmatter('更新日志', '按轻量 tag（<目录>/v<semver>）聚合的发布历史') +
  '# 更新日志\n\n> 按 sunmao 仓轻量 tag 聚合，由 sync-docs 自动生成。\n\n' +
  (changelogSections.length ? changelogSections.join('\n\n') + '\n' : 'sunmao 仓当前没有任何轻量 tag。\n') +
  (unpublished.length ? `\n### 未发布模块（走 pseudo-version）\n\n${unpublished.join('、')}\n` : ''));

// —— 5. 摘要 ——
const noReadme = modules.filter((m) => !m.hasReadme).map((m) => m.dir);
console.log(`[sync] sunmao 仓：${repoRoot}（HEAD ${repoHead || '未知'}）`);
console.log(`[sync] 模块 ${modules.length} 个，生成页面 ${pageCount} 篇`);
if (noReadme.length) console.log(`[sync] 缺 README（已生成占位页，回补 sunmao 仓第一批待办）：${noReadme.join('、')}`);
console.log('[sync] 完成 ✅');
