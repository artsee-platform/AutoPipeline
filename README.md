# 艺术设计院校数据自动化流水线（`yjxauto`）

一个基于 Python + Supabase 的多阶段数据流水线，用于采集、清洗和结构化艺术/设计院校数据。

它会从本地种子表开始，逐步补全学校基础资料、QS 排名、媒体资源、专业列表、专业费用/录取/评价、学校资源指标，并生成学校对比页可直接使用的汇总表。

## 项目结构

```text
yjxauto/
├── run_pipeline.py                 # 总入口，按 stage 执行
├── config/settings.py              # .env 配置加载
├── db/
│   ├── supabase_client.py          # Supabase 读写
│   ├── models.py                   # TypedDict 字段说明
│   └── migrate_*.sql               # Supabase schema 迁移
├── pipeline/
│   ├── stage0_seed.py              # Excel -> schools
│   ├── stage1_web_enrich.py        # 学校网页/LLM 富化
│   ├── stage2_qs_rankings.py       # QS 排名匹配
│   ├── stage3_video.py             # 视频元数据补标签
│   ├── stage4_programs.py          # 抽取 programs
│   ├── stage5_program_satellite.py # 抽取 fees/admissions/evaluations/categories
│   ├── stage6_school_resource_metrics.py
│   ├── stage7_school_comparison_rollups.py
│   ├── evidence.py                 # Tavily + 官网页面证据抓取
│   ├── qs_matcher.py               # QS 本地匹配器
│   ├── degree_normalizer.py        # 学位标签规范化
│   └── country_normalizer.py       # 国家/地区标签规范化
├── scrapers/
│   ├── claude_researcher.py        # Tavily + Claude 学校资料抽取
│   ├── website_scraper.py          # 官网 logo/campus 图片抓取
│   └── headless_image_scraper.py   # Playwright 图片候选采集
├── scripts/                        # 字典同步与回填脚本
├── data/
│   ├── schools.xlsx                # Stage 0 输入
│   ├── qs_data_subject.xlsx        # QS 学科排名
│   ├── qs_data_metrics.csv         # QS 综合排名
│   └── qs_aliases.json             # QS 别名映射
└── tests/                          # 轻量本地测试
```

## 环境准备

建议 Python `3.10+`。

```bash
pip install -r requirements.txt
```

如果要使用 Playwright fallback 或官网图片渲染抓取，还需要安装浏览器：

```bash
playwright install chromium
```

## 配置

复制模板并填写：

```bash
cp .env.example .env
```

必填变量：

```env
SUPABASE_URL=
SUPABASE_SERVICE_KEY=

ANTHROPIC_API_KEY=
TAVILY_API_KEY=

BATCH_SIZE=10
EVIDENCE_PLAYWRIGHT=1
```

说明：

- `BATCH_SIZE` 是默认批量大小，可被命令行 `--batch` 覆盖。
- `EVIDENCE_PLAYWRIGHT=1` 时，官网页面普通请求遇到 401/403/429/503 会尝试用 Playwright Chromium 重试，速度更慢但成功率更高。
- Stage 1/2/4/5/6 会调用 Anthropic/Tavily；Stage 3 会调用 `yt-dlp` 搜索视频平台。

## 数据文件

`data/` 下至少需要：

- `schools.xlsx`：Stage 0 输入，包含 `name_en`, `name_zh`, `country_or_area`, `official_website`。
- `qs_data_subject.xlsx`：QS 学科排名源。
- `qs_data_metrics.csv`：QS 综合排名源。
- `qs_aliases.json`：可选但建议维护，用于提高 QS 匹配准确率。

## Supabase 迁移

`db/` 下的迁移文件描述了当前 schema 的演进。新环境建议按文件名中的阶段顺序执行，尤其是：

- `migrate_p1_country_region.sql`
- `migrate_p2_schools_raw_country_rename.sql`
- `migrate_programs_id_to_uuid.sql`
- `fix_programs_school_id.sql`
- `migrate_programs_degree_type.sql`
- `migrate_degree_labels_dictionary.sql`
- `migrate_p3_currencies_fk.sql`
- `migrate_p4_application_difficulty_1_5.sql`
- `migrate_p5_competition_level_text.sql`
- `migrate_p6_program_evaluations_prose_columns.sql`
- `migrate_p7_schools_qs_rank.sql`
- `migrate_p8_school_resource_metrics.sql`
- `migrate_p9_school_comparison_rollups.sql`

国家、地区、学位字典可用脚本同步：

```bash
python -m scripts.sync_country_dictionaries
python -m scripts.backfill_country_and_region
python -m scripts.sync_degree_labels
python -m scripts.backfill_degree_normalization
```

## 流水线阶段

### Stage 0：种子导入

读取 `data/schools.xlsx`，写入/更新 `schools`。新记录状态为 `pending`；已有记录只更新种子字段，不覆盖富化内容。

```bash
python run_pipeline.py --stage 0
```

### Stage 1：学校网页富化

处理 `status="pending"` 的学校，使用 Tavily + Claude 抽取学校基础资料，并用官网抓取 logo/campus 图片。成功后状态变为 `enriched`。

```bash
python run_pipeline.py --stage 1 --batch 10
```

### Stage 2：QS 排名匹配

处理 `status="enriched"` 的学校。先用本地 QS 文件和 `pipeline/qs_matcher.py` 匹配；综合排名缺失时用 Tavily + Claude 兜底。成功后状态变为 `qs_done`。

```bash
python run_pipeline.py --stage 2 --batch 20
```

低置信度匹配会在日志中提示 `MANUAL REVIEW`，确认后可维护 `data/qs_aliases.json` 再重跑。

### Stage 3：视频元数据补充

处理 `status="qs_done"` 的学校，通过 `yt-dlp` 搜索 YouTube/Bilibili，再用 Claude 从标题和描述中提取补充标签。成功后状态变为 `done`。

```bash
python run_pipeline.py --stage 3 --batch 10
```

### Stage 4：专业列表抽取

遍历 `schools`，对每所学校补足最多 3 条 `programs`。证据来自 Tavily 搜索和同域官网页面提取，再由 Claude 输出结构化专业数据。

```bash
python run_pipeline.py --stage 4 --batch 5
python run_pipeline.py --stage 4 --batch 200 --reset-programs
```

`--reset-programs` 会清空 `programs` 后重跑，使用前请确认数据库状态。

### Stage 5：专业卫星表补全

遍历 `programs`，补齐：

- `program_fees`
- `program_admissions`
- `program_evaluations`
- 可选 `program_art_categories`

```bash
python run_pipeline.py --stage 5 --batch 10
python run_pipeline.py --stage 5 --batch 10 --fill-art-categories
```

### Stage 6：学校资源指标

补齐 `school_resource_metrics`，包括师生比文本、奖学金比例、校园设施摘要等。默认跳过已有实质数据的学校。

```bash
python run_pipeline.py --stage 6 --batch 10
python run_pipeline.py --stage 6 --batch 5 --force-resources
```

### Stage 7：学校对比汇总

全量扫描 active programs 和卫星表，重建 `school_comparison_rollups`。这个阶段会忽略 `--batch`，因为它需要一次性全量重算以保持对比数据一致。

```bash
python run_pipeline.py --stage 7 --batch 1
```

## 常用命令

```bash
# 默认执行 0-3
python run_pipeline.py

# 连续执行多个阶段
python run_pipeline.py --stage 1-3 --batch 10
python run_pipeline.py --stage 4-7 --batch 10

# 重置 error 学校，便于重跑 Stage 1
python run_pipeline.py --retry-errors

# 只刷新缺失的 logo/campus 图片
python run_pipeline.py --refresh-media --batch 20

# 强制刷新所有学校媒体
python run_pipeline.py --refresh-media --force-all --batch 50

# 只刷新指定学校媒体
python run_pipeline.py --refresh-media --schools "Royal College of Art,Parsons School of Design"
```

## 本地测试

测试不依赖 Supabase、Anthropic 或 Tavily，主要覆盖纯逻辑模块：

```bash
python -m unittest discover -s tests
```

也可以做一次语法级检查：

```bash
python -m compileall -q .
```

## 常见问题

### 启动即报缺环境变量

检查 `.env` 是否存在，变量名是否和 `.env.example` 一致。

### Stage 2 匹配不准或没匹配上

看日志中的 `MANUAL REVIEW` 和 `NOT IN QS DATA`。确认学校在 QS 数据中的官方写法后，把映射加入 `data/qs_aliases.json`。

### Stage 3 视频结果少

公开视频本来可能很少；Bilibili 也可能因网络环境不可用，属于可降级路径。

### Stage 4/5 速度慢

这是预期现象。每个学校/专业会做多次搜索、官网页面抓取和 Claude 调用；遇到 403、PDF、超时或 Playwright fallback 时会更慢。建议先用 `--batch 3` 小批量验证。

### Stage 4 报 programs.school_id 类型错误

先确认已执行：

```text
db/migrate_programs_id_to_uuid.sql
db/fix_programs_school_id.sql
```

## 开发建议

- 新 schema 以 `schools.raw_country` 作为原始国家/地区文本字段，`country_code` 和 `region_tag` 是规范化后的字段。
- Stage 4 及之后主要面向 `programs` 和卫星表，不再改变学校主状态流。
- 外部证据抽取遵循“只用 evidence，不凭空补事实”的 prompt 约束，但仍建议对关键学校抽样人工 QA。
- 新增规范化规则时，优先补纯逻辑测试，再同步对应字典表。
