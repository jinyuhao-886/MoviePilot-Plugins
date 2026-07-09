# Doc911Subscribe — 金山文档订阅同步

> **MoviePilot 自研插件** — 从 KDocs（金山文档）xlsx 表格自动解析在播剧集并添加 MP 订阅。

**维护：** [jinyuhao-886](https://github.com/jinyuhao-886)

---

## 📋 功能

从内部共享的 KDocs 追剧表（xlsx）中，每天 21:01 自动拉取最新数据，解析出三个区段的**在播剧集**，添加到 MP 订阅：

| 区段 | 来源 |
|------|------|
| 🏮 本月更新【国产剧】 | KDocs 文档区段 |
| 🌍 本月更新【国外剧】 | KDocs 文档区段 |
| 🎪 本月更新【综艺】 | KDocs 文档区段 |

> 自动过滤已完结（"全\d+集"、"已完结"）和未开始的条目，只追**更新中**的剧集。

---

## ✨ 特性

| 特性 | 说明 |
|------|------|
| ⏰ 定时执行 | APScheduler CronTrigger(hour=21, minute=1)，每天自动 |
| 📥 xlsx 自动下载 | 通过 KDocs API 下载最新文档，本地缓存 |
| 🧩 智能区段解析 | openpyxl 解析合并单元格，三个区段独立过滤 |
| 🔍 在播识别 | 行内容含"更新"二字即视为在播，跳过已完结 |
| 🧠 AI 智能助手 | TMDB 匹配失败时调用 MP MCP 智能识别 |
| 📺 季数自动提取 | 剧名中的"第X季/Season X"自动提取，正确传参 |
| 🔧 别名映射（可选） | 可配置手动映射表做 fallback |

---

## ⚙️ 配置

### 插件设置项

| 字段 | 说明 |
|------|------|
| KDocs Cookie | 浏览器登录金山文档后复制的 Cookie（必填） |
| File ID | 金山文档的 file_id（默认已填好） |
| 仅执行一次 | 勾选后仅执行一轮，不注册定时任务 |
| 名称别名（JSON） | 可选的手动映射表，格式见下方说明 |

### 名称别名格式

```json
{
  "原剧名": "目标剧名",
  "乘风 (2026)": "乘风破浪的姐姐 第七季"
}
```

> 仅当 AI 智能助手也无法识别时，才会使用映射表做最后的 fallback。

---

## 🔄 执行流程

```
① 定时触发（每天 21:01）
         ↓
② 下载最新 xlsx（KDocs API + Cookie）
         ↓
③ openpyxl 解析三个区段（国产剧/国外剧/综艺）
         ↓
④ 过滤在播条目（包含"更新"关键词）
         ↓
⑤ 提取季数（"第X季"→ season=X）
         ↓
⑥ chain.add() 添加订阅
   └── 失败 → AI 智能助手（MCP）辅助识别
         ↓
⑦ 记录结果到日志
```

---

## 📜 版本历史

### v1.2.0
- 🎯 季数自动提取："第X季/第X期/Season X" → 正确传入 `chain.add(season=X)`
- ♻️ 从剧名中移除季节描述，避免干扰 TMDB 匹配
- 🧠 AI prompt 增加季数描述

### v1.1.1
- 🐛 修复 author 字段（`jyh` → `jinyuhao-886`）
- 🐛 修复 `SubscribeOper.list()` → `Subscribe.get_by_title()`
- ♻️ xlsx 改为本地文件存储（`self.get_data_path() / "doc.xlsx"`）

### v1.1.0
- 🧠 AI 智能助手集成（MCP search_media）
- 🔧 别名映射 fallback

### v1.0.0
- 🎉 初始版本：xlsx 下载 + 三区段解析 + chain.add 订阅

---

## 🔗 相关链接

- **KDocs 文档源：** 内部共享追剧表
- **[MoviePilot](https://github.com/jxxghp/MoviePilot)** - 本体项目
### v1.2.1
- 🎯 季数识别大升级：支持 `第1季`（阿拉伯数字）、`S01`、`Season 1` 格式
- 🐛 修复 TMDB 年份过滤问题：有季数的剧不传 year（避免续集搜不到），新剧才传 year 辅助匹配

### v1.2.2
- 🎯 策略调整：TMDB识别失败时直接跳过，不再调用AI助手硬猜
- 🔒 宁可不订也不乱订，避免错误订阅到错误的剧集
