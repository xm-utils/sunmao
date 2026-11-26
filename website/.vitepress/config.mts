import { defineConfig } from 'vitepress';
import { readFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
// modules.json 由 scripts/sync-docs.mjs 生成；dev/build 均已前置 sync，正常情况下必然存在
const modules = existsSync(resolve(here, 'modules.json'))
  ? JSON.parse(readFileSync(resolve(here, 'modules.json'), 'utf-8'))
  : { groups: [], modules: [] };

const byDir = Object.fromEntries(modules.modules.map((m) => [m.dir, m]));

// 侧边栏：族为一级、模块为二级、README/QUICKSTART/配置模板为三级
function docsSidebar() {
  return modules.groups.map((g) => ({
    text: g.name,
    collapsed: false,
    items: g.modules.map((dir) => {
      const m = byDir[dir];
      const items = [{ text: '介绍', link: `/docs/${dir}/` }];
      if (m?.hasQuickstart) items.push({ text: '快速开始', link: `/docs/${dir}/quickstart` });
      if (m?.hasConfig) items.push({ text: '配置模板', link: `/docs/${dir}/config` });
      return { text: dir, collapsed: true, items };
    }),
  }));
}

export default defineConfig({
  base: '/sunmao/',
  lang: 'zh-CN',
  title: '榫卯',
  description: '榫卯（xm-utils/sunmao）—— Go 微服务基础设施组件集，可观测性、服务治理、消息队列、数据访问全家桶',
  lastUpdated: true,
  cleanUrls: true,
  head: [
    ['link', { rel: 'icon', type: 'image/png', href: '/favicon.png' }],
  ],
  ignoreDeadLinks: [
    // 教程示例中的本地服务端点（如 trace QUICKSTART 的 Jaeger UI），构建时必然不可达
    /^https?:\/\/(localhost|127\.0\.0\.1)/,
    // 存量 README 中指向尚未回补文档的相对链接（sunmao 仓补文档后收敛为精确列表）
    /^\/docs\/[^/]+\/(README|QUICKSTART)/,
  ],
  themeConfig: {
    logo: '/logo.png',
    nav: [
      { text: '首页', link: '/' },
      { text: '模块文档', link: `/docs/${modules.modules[0]?.dir ?? 'retry'}/` },
      { text: '架构与组合', link: '/guide/architecture' },
      { text: '指南', link: '/guide/getting-started' },
      { text: '更新日志', link: '/changelog' },
    ],
    sidebar: {
      '/docs/': docsSidebar(),
      '/guide/': [
        {
          text: '指南',
          items: [
            { text: '快速引入', link: '/guide/getting-started' },
            { text: '架构与组合', link: '/guide/architecture' },
            { text: '模块总览', link: '/guide/modules' },
            { text: '仓内依赖', link: '/guide/dependencies' },
          ],
        },
      ],
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/xm-utils/sunmao.git' }],
    search: { provider: 'local' },
    footer: {
      message: '基于 MIT 许可发布 · 要求 Go 1.24.2+',
      copyright: '© xm-utils · 备案号占位',
    },
    outline: { label: '本页目录' },
  },
});
