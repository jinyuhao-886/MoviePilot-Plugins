# 115网盘订阅追更魔改版

> MoviePilot 第三方插件，基于 [mrtian2016/MoviePilot-Plugins](https://github.com/mrtian2016/MoviePilot-Plugins) 的 `P115StrgmSub`，添加自定义特性。

## ✨ 与官方版的区别

| 特性 | 官方版 `P115StrgmSub` | 魔改版 |
|---|---|---|
| 默认 cron | `30 2,10,18 * * *`（每天 3 次） | `0 18-23 * * *`（每晚 6-11 点整点） |
| 最小调度间隔 | 强制 ≥ 8 小时 | 无限制 |
| DoVi 兜底排除 | ❌ 无 | ✅ `DoVi\|Dolby[\s.]?Vision\|DOVI\|杜比视界`（默认兜底，可在 UI 调） |
| 同步触发频率 | 每天 3 次 | 每晚 6 次（18:00 / 19:00 / 20:00 / 21:00 / 22:00 / 23:00） |

## 📦 安装

### 第 1 步：把本仓加到 `PLUGIN_MARKET`

编辑 MP 的 `docker-compose.yml` 或 `app.env`：

```bash
PLUGIN_MARKET='<你原有的仓地址>,https://github.com/jinyuhao-886/MoviePilot-Plugins/'
```

> **注意**：URL 末尾必须有斜杠 `/`，否则部分 MP 版本会解析失败。

### 第 2 步：重启 MP → 在插件市场搜「115网盘订阅追更魔改版」→ 安装

界面会显示：

- **名字**：115网盘订阅追更魔改版
- **版本**：1.5.3-modi.2
- **作者**：jinyuhao-886

## ⚙️ 配置项

### 🔴 必须填（否则插件无法工作）

| 字段 | 说明 | 例子 |
|---|---|---|
| `cookies` | 115 网盘登录 Cookie，没这个等于插件废了 | `UID=xxx; CID=xxx; SEID=xxx; KID=xxx` |
| `save_path` | 电视剧转存到 115 网盘的目录 | `/我的接收/TV` 或 `/pt/115订阅` |

### 🟡 可选填（按需启用）

| 字段 | 说明 |
|---|---|
| `pansou_url` | PanSou 搜索服务地址，默认 `https://so.252035.xyz`（公共） |
| `pansou_channels` | PanSou 频道列表，逗号分隔 |
| `movie_save_path` | 电影转存路径 |
| `notify` | 是否启用通知（默认 True） |
| `block_system_subscribe` | 是否屏蔽系统订阅，只走 115 网盘（默认 True） |
| `global_exclude` | 全局兜底 exclude 正则（默认 `DoVi\|Dolby[\s.]?Vision\|DOVI\|杜比视界`） |
| `hdhive_*` | HDHive 资源站配置，不用就 disabled |
| `nullbr_*` | NullBr 资源站配置，不用就 disabled |

### 🍪 `cookies` 怎么拿？

1. 浏览器登录 [115.com](https://115.com)
2. `F12` 打开 DevTools → `Network` 标签
3. 任意点一个请求 → 看 `Request Headers` 里的 `Cookie`
4. 复制完整的 `Cookie` 值（约 200-300 字符）

> 提示：如果同时装了 P115StrmHelper（115网盘STRM助手），**两边 cookie 共享**，填同一个就行。

## 🎬 DoVi 兜底怎么调？

魔改版默认内置"杜比视界硬拒绝"机制：

- 任何文件名包含 `DoVi`、`Dolby Vision`、`DOVI`、`杜比视界` 的资源**不会被转存**
- 可以在 MP 后台 → 插件配置 → 搜索「全局兜底排除」→ 改成你自己的正则

**为什么默认拒绝 DoVi？** DoVi 资源在 Emby/Jellyfin 上兼容性差，老大们通常从 PT 站下载 HDR 片源（质量更好），不依赖 115 网盘。

## 🔄 升级

插件市场 → 搜索「115网盘订阅追更魔改版」→ 看到新版本点升级即可。

⚠️ **升级不会丢配置**：本插件的 `plugin_id` 仍为 `P115StrgmSub`，所有 `user.db` 配置（48 字段）无缝迁移。

## 📊 版本历史

### v1.5.3-modi.2 (2026-06-21)

- cron 默认值改为 `0 18-23 * * *`（每晚 18-23 点整点执行）
- 删除 `8 小时最小间隔锁`
- `plugin_version` / `plugin_author` 改为 fork 仓标识

### v1.5.3-modi.1 (2026-06-21)

- 首次 fork（基于 mrtian2016 官方 `P115StrgmSub` 1.5.3）
- DoVi 兜底默认规则
- 4 文件 DoVi 改造（`utils/file_matcher.py` / `handlers/sync.py` / `__init__.py` / `ui/config.py`）

## 🙏 致谢

- 原作者 [mrtian2016](https://github.com/mrtian2016) 提供 `P115StrgmSub` 基础代码
- MoviePilot 框架 [jxxghp/MoviePilot](https://github.com/jxxghp/MoviePilot)

## 📜 License

本 fork 沿用原项目许可。