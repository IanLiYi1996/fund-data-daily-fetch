# Fund Manager 任期 + 规模历史 抓取方案

**日期**: 2026-05-14
**作者**: ianleely (with Claude)
**状态**: Draft — 待实施

## 背景

`fund-history` 现有抓取覆盖了基金净值/排行/费率/分红，但缺：

1. **基金经理任期段**：现有 `fund_manager_em` 只给经理粒度的当前快照（在任经理 + AUM + 任职回报），无法回答「某只基金 2023 年是谁在管」「现任经理上任多久」。
2. **规模时序**：`fund_individual_basic_info_xq` 只给当前规模点值，无历史。

下游消费方（孟老板）按 `基金代码` 在 `fund/{YYYY-MM-DD}/*.parquet` 上做 join，新数据需保持同样的访问形态。

## 数据源

⚠️ **修正**：akshare 没有现成的「基金经理变动」「基金规模变动」单基金接口。verified（2026-05-14）实际可用源：

| 源 | URL | 内容 | 频率 |
|---|---|---|---|
| 东财 fundf10（HTML scrape） | `https://fundf10.eastmoney.com/jjjl_{code}.html` 内「基金经理变动一览」表 | 任期段（起始期 / 截止期 / 基金经理 / 任职天数 / 任职回报） | 事件型 |
| 东财 pingzhongdata.js | `https://fund.eastmoney.com/pingzhongdata/{code}.js` 内 `Data_fluctuationScale` 变量 | 季度规模时序（单位：亿元 + 环比变动率） | 季度 |

数据形态示例：

```
# 任期段（多人共管时姓名空格分隔，"至今" 表示现任）
['2026-03-04', '至今',       '李守峰',    '71天',         '12.17%']
['2026-01-30', '2026-03-03', '申坤 李守峰', '32天',       '-3.51%']
['2024-07-26', '2026-01-29', '申坤',       '1年又187天',  '71.50%']

# 规模时序
{"categories": ["2025-03-31", ..., "2026-03-31"],
 "series": [{"y": 6.06, "mom": "7.24%"}, ..., {"y": 5.26, "mom": "-25.08%"}]}
```

全市场 ~25k 基金 × 2 源 ≈ 50k HTTP 请求/全量。

## 决策（已确认）

1. **拆两张独立表**，写到 `fund/{date}/`：
   - `fund_manager_history.parquet`（~150k 行/全量）
   - `fund_scale_history.parquet`（~600k 行/全量）
   - `fund_type` 不动（已在 `fund_name.parquet` 里）。

2. **新建独立 Lambda `fund-history-fetcher`**，与现有 9 个 fetcher 解耦：
   - **周日 18:00 UTC**：`fund_manager_history` 全量（拿 `fund_name_em` 的代码列表 + 并发跑 `fund_open_fund_info_em(indicator="基金经理变动")`）
   - **季末 +3 天**：`fund_scale_history` 全量（季报数据公布有滞后，3 天缓冲）
   - **每日 17:30 UTC**：可选的 `fund_manager_em` diff 增量，零额外调用，捕获日内人事变动用于告警/审计（不写主表）

3. **Canonical 路径，零冗余**：
   - 真实刷新写到 `fund/_history/fund_manager_history.parquet` / `fund/_history/fund_scale_history.parquet`（覆盖）
   - S3 Versioning 自动保留旧版本 365 天（noncurrent expiration policy），可按 versionId 回溯
   - per-partition 中间文件落到 `_history_staging/{name}__part{i}.parquet` —— 在 `fund/` 前缀之外，**不被 S3 Replication 复制**到孟老板那边
   - 孟老板读固定路径：`pd.read_parquet('s3://financial-dataset-mx/fund-data-pipeline/fund/_history/fund_manager_history.parquet')` —— 一行代码，不需要每天判断日期

## 表结构

### `fund_manager_history.parquet`

任期段，每只基金多行；多人共管时按经理姓名拆成多行（一个任期 N 个经理 → N 行）。

| 列 | 类型 | 说明 |
|---|---|---|
| 基金代码 | str | 6 位代码 |
| 基金简称 | str | 来自 `fund_name_em` join |
| 经理姓名 | str | 多人共管时拆成多行 |
| 起始日 | date | 上任日 |
| 结束日 | date | 离任日；现任为 NaT |
| 任期天数 | int | 从「1年又187天」/「71天」解析 |
| 任期回报 | float | 百分比（12.17 / -3.51 / NaN）|
| 是否现任 | bool | 截止期为「至今」时 True |
| snapshot_date | date | 抓取日 |

PK：`(基金代码, 经理姓名, 起始日, snapshot_date)`

### `fund_scale_history.parquet`

季度规模时序（单位：亿元）。pingzhongdata 不给份额数据，只给规模 y 值和环比 mom。

| 列 | 类型 | 说明 |
|---|---|---|
| 基金代码 | str | |
| 基金简称 | str | |
| 报告期 | date | 季报期末日（categories 元素）|
| 期末净资产_亿元 | float | series[i].y |
| 净资产环比变动率 | float | series[i].mom 解析为百分数 |
| snapshot_date | date | |

PK：`(基金代码, 报告期, snapshot_date)`

## Lambda 设计

```
lambda/fund-history-fetcher/
├── Dockerfile
├── handler.py
└── requirements.txt        # akshare, pandas, pyarrow

shared/fetchers/
└── fund_history_fetcher.py # 新增

handler.py 入口接收 event.mode:
- "manager_full"     → 全量经理任期回填
- "scale_full"       → 全量规模季报
- "manager_diff"     → 当日 diff（仅写告警，不写主表）
```

并发与速率：
- `concurrent.futures.ThreadPoolExecutor(max_workers=8)`，每只基金一次 retry 3 次
- 失败基金代码记入 `errors[]`，不阻断
- 25k 调用预计 30-50 min（akshare 单调用 80-150ms + 重试）→ Lambda 内存 3GB / 超时 15min **不够**

**两个选项**：
- **A**：拆分成 4 个 partition Lambda（每个 6.25k 基金 × 8 worker ≈ 8-12 min），Step Functions Map 并行
- **B**：跑在 EC2/Fargate task，单进程跑完，无 15min 限制

→ 推荐 **A**，复用 Step Functions 拓扑、和现有架构一致。

## CDK 改动

```typescript
// 1. 新 Lambda（partition 参数化）
const fundHistoryLambda = this.createDockerLambda(
  "FundHistoryFetcherLambda", lambdaDir,
  "fund-history-fetcher/Dockerfile",
  "Fetch per-fund manager tenure + scale history from akshare",
  3008, 15, lambdaEnv
);

// 2. Step Functions: Map 4 partitions
const partitionMap = new sfn.Map(this, "FundHistoryPartitionMap", {
  itemsPath: "$.partitions",
  maxConcurrency: 4,
});

// 3. EventBridge schedules
new events.Rule(this, "WeeklyManagerHistory", {
  schedule: events.Schedule.cron({ minute: "0", hour: "18", weekDay: "SUN" }),
  targets: [new targets.SfnStateMachine(stateMachine, {
    input: events.RuleTargetInput.fromObject({ mode: "manager_full" })
  })],
});

new events.Rule(this, "QuarterlyScaleHistory", {
  // 季末 +3 天: 1/4/7/10 月 4 日 19:00 UTC
  schedule: events.Schedule.cron({
    minute: "0", hour: "19", day: "4", month: "1,4,7,10"
  }),
  targets: [new targets.SfnStateMachine(stateMachine, {
    input: events.RuleTargetInput.fromObject({ mode: "scale_full" })
  })],
});
```

## catalog-generator 改动

不需要改动。canonical 路径 `fund/_history/` 由 fund-history-fetcher 自身维护，S3 Versioning 提供时光机能力，无需每日 CopyObject。

`find_latest_real_file` 跳过自身的 copy（用 metadata `copied_from` 是否存在判断），保证不会做"复制的复制"。

## 错误处理

| 场景 | 策略 |
|---|---|
| 单只基金接口超时/解析失败 | 重试 3 次后跳过，记入 `errors[]` |
| akshare 整体故障（>50% 基金失败） | 整次失败，不覆盖上一份；SNS 告警 |
| 单 partition Lambda 超时 | Step Functions 内 retry；最终失败该 partition 不参与本次合并 |
| 部分 partition 成功 | merge 时记录 `partial: true` 标志，保留旧文件 fallback |

## 测试

- 单元：`fund_history_fetcher.parse_manager_change_df`（中英列、单经理 vs 多经理、缺失结束日）
- 集成：`moto` mock S3，跑 50 只基金小样本 → 验证 partition merge 后行数和 PK 唯一性
- E2E（dev 账号）：4 个 partition 跑全量 → merge 输出 `fund/_history/fund_manager_history.parquet` → S3 Replication 镜像到 mengxin 端

## 实施里程碑

| # | 任务 | 工作量 |
|---|---|---|
| M1 | `fund_history_fetcher.py` + 单元测试 | 0.5 天 |
| M2 | Lambda 容器 + Dockerfile + handler partition 逻辑 | 0.5 天 |
| M3 | Step Functions Map state machine + EventBridge | 0.5 天 |
| M4 | ~~catalog-generator daily copy~~（不需要，canonical 路径替代）| 0 天 |
| M5 | dev E2E + prod 切换 | 0.5 天 |

总计：~2.5 工作日。

## 与孟老板的接口契约

- **路径**（固定，覆盖式更新）：
  - `s3://financial-dataset-mx/fund-data-pipeline/fund/_history/fund_manager_history.parquet`
  - `s3://financial-dataset-mx/fund-data-pipeline/fund/_history/fund_scale_history.parquet`
- **同步机制**：S3 Replication 规则 `replicate-fund`（前缀 `fund-data-pipeline/fund/`）自动镜像到孟老板账号
- **PK**：`基金代码`（与现有所有 fund_*.parquet 一致）
- **延迟**：经理任期最多滞后 6 天（周日刷新），规模最多滞后 1 季度 + 3 天
- **时光机**：源 bucket 启用 versioning，旧版本保留 365 天，可按 versionId 回溯
- **变更通告**：上线前 + schema 改动时邮件知会

## 不做（YAGNI）

- 不拆持有人结构（`fund_hold_structure_em` 是另一个独立需求）
- 不做基金经理跨基金任职图谱（消费方没要求）
- 不做实时 diff 写主表（diff 只用于告警，避免每日把昨日全量 + 增量混在一起）
