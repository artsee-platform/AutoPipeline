# 艺术设计院校数据自动化流水线（`yjxauto`）

一个基于 Python 的多阶段数据流水线，用于：

- 从本地种子表导入艺术/设计院校基础信息；
- 使用 Claude + Tavily + 网页抓取补全院校资料；
- 自动匹配 QS 2026 排名并分层；
- 通过视频元数据补充标签与描述。

---

## 1. 项目结构

```text
yjxauto/
├── run_pipeline.py             # 总入口，按 stage 执行
├── config/settings.py          # .env 配置加载
├── pipeline/
│   ├── stage0_seed.py          # 从 Excel 导入 Supabase（初始状态 pending）
│   ├── stage1_web_enrich.py    # Claude + 官网抓取补全
│   ├── stage2_qs_rankings.py   # QS 排名匹配（含 LLM 兜底）
│   └── stage3_video.py         # yt-dlp + Claude 补充标签
├── db/
│   ├── supabase_client.py      # Supabase 读写
│   └── models.py               # School 字段定义
├── scrapers/
│   ├── claude_researcher.py    # Tavily 检索 + Claude 结构化抽取
│   └── website_scraper.py      # 官网 logo/campus 图片抓取
└── data/
    ├── schools.xlsx            # Stage 0 输入（需自行准备）
    ├── qs_data_subject.xlsx    # QS 学科排名
    ├── qs_data_metrics.csv     # QS 综合排名
    └── qs_aliases.json         # 院校别名映射（手工维护）
```

---

## 2. 流水线阶段与状态流转

### Stage 0：种子导入（Excel -> Supabase）
- 读取 `data/schools.xlsx`，清洗后写入 `schools_auto`；
- 新记录写入 `status="pending"`；
- 已存在记录仅更新种子字段（`name_zh/country/official_website`），不覆盖已富化内容。

### Stage 1：网页富化（Claude + 抓官网）
- 处理 `status="pending"` 的学校；
- 先标记 `processing` 避免重复并发处理；
- 完成后写回文本与媒体字段，状态变为 `enriched`；
- 失败标记 `error`。

### Stage 2：QS 排名匹配
- 处理 `status="enriched"`；
- 先用本地匹配器（`pipeline/qs_matcher.py`）对齐排名；
- 对 `qs_overall_rank` 缺失的学校，使用 Tavily + Claude 兜底；
- 写回各类 QS 字段和 `school_tier`，状态变为 `qs_done`；
- 低置信度结果会在日志提示手工维护 `data/qs_aliases.json`。

### Stage 3：视频元数据补充
- 处理 `status="qs_done"`；
- 通过 `yt-dlp` 拉取 YouTube/Bilibili 搜索结果；
- 用 Claude 从视频标题/描述提取新增标签与描述修订；
- 成功后状态变为 `done`，失败为 `error`。

### 全部状态
`pending -> processing -> enriched -> qs_done -> done`（任一阶段异常可进入 `error`）

---

## 3. 环境准备

### 3.1 Python 版本
建议 Python `3.10+`。

### 3.2 安装依赖

```bash
pip install -r requirements.txt
```

---

## 4. 配置 `.env`

先复制模板：

```bash
cp .env.example .env
```

然后填写以下变量：

```env
SUPABASE_URL=
SUPABASE_SERVICE_KEY=

ANTHROPIC_API_KEY=
TAVILY_API_KEY=

BATCH_SIZE=10
```

说明：
- `BATCH_SIZE` 是默认批量处理数量，可被命令行 `--batch` 覆盖；
- 启动时若缺关键变量会直接报错退出。

---

## 5. 数据文件准备

确保 `data/` 下至少具备以下文件：

- `schools.xlsx`：Stage 0 种子数据（需包含列：`name_en`, `name_zh`, `country_or_area`, `official_website`）；
- `qs_data_subject.xlsx`：QS 学科排名源；
- `qs_data_metrics.csv`：QS 综合排名源；
- `qs_aliases.json`：可选，建议维护（用于提升匹配准确率）。

---

## 6. 运行方式

入口脚本：`run_pipeline.py`

```bash
# 仅导入种子（stage 0）
python run_pipeline.py --stage 0

# 仅运行某一阶段
python run_pipeline.py --stage 1 --batch 10
python run_pipeline.py --stage 2 --batch 20
python run_pipeline.py --stage 3 --batch 10

# 连续运行多个阶段
python run_pipeline.py --stage 1-3 --batch 10
python run_pipeline.py --stage 0-3 --batch 10

# 不传 --stage 时：默认执行 0-3 全阶段
python run_pipeline.py

# 将 error 状态重置为 pending（便于重试）
python run_pipeline.py --retry-errors
```

---

## 7. Supabase 注意事项

### Supabase
- 当前表名固定为 `schools_auto`（见 `db/supabase_client.py`）；
- 需包含 `db/models.py` 里涉及的字段（至少保证各阶段会写入字段存在）。

---

## 8. 常见问题

### 1) 启动即报缺环境变量
- 检查 `.env` 是否存在、变量名是否完整且拼写一致。

### 2) Stage 2 匹配不准或丢失
- 查看日志中的 `MANUAL REVIEW` 提示；
- 在 `data/qs_aliases.json` 新增别名映射后重跑 Stage 2。

### 3) Stage 3 视频抓取结果少
- 目标学校公开视频本来可能较少；
- Bilibili 在部分网络环境可能不可用，属正常降级。

---

## 8. 开发建议

- 先小批量跑通：`--batch 3`；
- 建议按 `0 -> 1 -> 2 -> 3` 顺序逐步验证；
- 排查问题优先看每个 stage 的日志输出与 Supabase 状态字段。

