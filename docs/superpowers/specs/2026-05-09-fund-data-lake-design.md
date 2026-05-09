# Fund Data Lake 改造设计

**日期**: 2026-05-09
**作者**: ianleely (with Claude)
**状态**: Draft — 待 review

## 背景与目标

当前系统每天通过 Step Functions 并行触发 9 个 Lambda 抓取 akshare 数据，写入 S3 路径 `{category}/{YYYY-MM-DD}/{name}.parquet`。该结构虽然 parquet 友好，但：

1. **没有"表"语义**：每天独立 parquet，跨日期查询要扫描多个目录
2. **不支持 upsert**：akshare 修订历史数据时只能整分区重写
3. **没有 schema 演进保障**：上游加列会破坏旧文件兼容性
4. **场内+场外覆盖不全**：缺封闭式、FOF、REITs、持仓明细

**改造目标**：

- ✅ 每日盘后抓取并自动合并历史
- ✅ 支持 CRUD、行级 upsert、schema 演进
- ✅ 覆盖 A 股场内+场外全部基金（开放式/ETF/LOF/分级/封闭/FOF/REITs/货币/理财/QDII/港基）
- ✅ Athena / DuckDB / Pandas 三种消费路径

**非目标**：
- 不重写 data-processor / catalog-generator / MCP（保持下游兼容）
- 不引入 Spark/EMR（Lambda 跑得动）
- 不做实时流（每日批处理足够）

---

## §1 整体架构

```
EventBridge (17:00 UTC, 北京 01:00)
        │
        ▼
Step Functions
   ├── (并行) 9 fetcher Lambda
   │     ├──→ ① S3 raw parquet (保留, 双写过渡)
   │     │      {category}/{YYYY-MM-DD}/{name}.parquet
   │     └──→ ② Iceberg 表 (新增主路径)
   │            s3://.../iceberg/fund_data_lake/{table}/
   │            metadata in AWS Glue Data Catalog
   ├── data-processor Lambda    ──→ data/latest/*.json (MCP, 暂不变)
   └── catalog-generator Lambda ──→ _catalog/*.json (不变)

新增独立调度:
EventBridge (周日 20:00 UTC)
   └── iceberg-maintenance Lambda
        ├── rewrite_data_files (target 128 MB)
        └── expire_snapshots (retain 14d)

Glue Database: fund_data_lake (27 张表)
```

### 关键决策

1. **方案选型：自建 Iceberg + Glue Catalog**（对比详见附录 A）
2. **双写过渡**：fetcher 同时写 raw parquet 和 Iceberg；processor/MCP 不动
3. **不改 Step Functions 拓扑**：只在 fetcher Lambda 内部多一步 `iceberg.upsert()`
4. **新增 1 个 weekly maintenance Lambda**：避免小文件累积
5. **写入语义统一**：每日刷新型用 `upsert`（PK 幂等），事件型用 `append`

---

## §2 数据模型

新增 Glue Database `fund_data_lake`，共 **27 张表**，分 5 个域（域 1: 8 + 域 2: 3 + 域 3: 6 + 域 4: 7 + 域 5: 3）。

### 命名约定

- Iceberg 表名、列名一律小写英文
- 日期分区列统一为 `trade_date` / `snapshot_date` / `event_date`（Date 类型）
- 原始中文列保留为普通数据列（不做 partition key）
- Snapshot 默认保留 14 天

### 域 1：基金核心数据（高频，每日盘后）

| 表名 | 主键 | 分区 | 来源接口 | 说明 |
|---|---|---|---|---|
| `fund_daily` | (基金代码, trade_date) | month(trade_date) | `fund_open_fund_daily_em` | 场外开放式 |
| `fund_etf` | (代码, trade_date) | month(trade_date) | `fund_etf_spot_em` | 场内 ETF 实时 |
| `fund_lof` | (代码, trade_date) | month(trade_date) | `fund_lof_spot_em` | 场内 LOF |
| `fund_money_daily` | (基金代码, trade_date) | month(trade_date) | `fund_money_fund_daily_em` | 货币 |
| `fund_financial_daily` | (基金代码, trade_date) | month(trade_date) | `fund_financial_fund_daily_em` | 理财型 |
| `fund_etf_daily` | (基金代码, trade_date) | month(trade_date) | `fund_etf_fund_daily_em` | 场内 ETF 净值/折溢价 |
| `fund_graded_daily` | (基金代码, trade_date) | month(trade_date) | `fund_graded_fund_daily_em` | 分级 |
| `fund_value_estimation` | (基金代码, snapshot_time) | day(snapshot_time) | `fund_value_estimation_em` | 实时估值 |

### 域 2：场内封闭/REITs/FOF（新增）

| 表名 | 主键 | 分区 | 来源接口 |
|---|---|---|---|
| `fund_close_daily` | (基金代码, trade_date) | month(trade_date) | `fund_close_em` |
| `fund_fof_daily` | (基金代码, trade_date) | month(trade_date) | `fund_fof_em` |
| `fund_reits_daily` | (基金代码, trade_date) | month(trade_date) | `public_fund_REITs` |

### 域 3：排行/评级（每日刷新，upsert 覆盖当日）

| 表名 | 主键 | 分区 |
|---|---|---|
| `fund_performance` | (基金代码, snapshot_date) | month(snapshot_date) |
| `fund_exchange_rank` | 同上 | 同上 |
| `fund_money_rank` | 同上 | 同上 |
| `fund_hk_rank` | 同上 | 同上 |
| `fund_dividend_rank` | 同上 | 同上 |
| `fund_rating` | (基金代码, snapshot_date, 评级机构) | month(snapshot_date) |

### 域 4：低频(事件驱动 / 季度)

| 表名 | 主键 | 分区 | 频率 |
|---|---|---|---|
| `fund_dividend` | (基金代码, event_date) | year(event_date) | 事件 |
| `fund_split` | (基金代码, event_date) | year(event_date) | 事件 |
| `fund_purchase` | (基金代码, snapshot_date) | month(snapshot_date) | 每日 |
| `fund_index_info` | (基金代码, snapshot_date) | month(snapshot_date) | 每日 |
| `fund_portfolio_hold` | (基金代码, report_date, 持仓代码) | year(report_date) | 季度（新增） |
| `fund_name` | (基金代码, snapshot_date) | year(snapshot_date) | 每日 SCD-1 |
| `fund_manager` | (基金经理ID, snapshot_date) | year(snapshot_date) | 每日 |

### 域 5：行情 K 线（已有，迁入 Iceberg）

| 表名 | 主键 | 分区 | 来源 |
|---|---|---|---|
| `kline_a` | (代码, freq, trade_date) | (freq, year(trade_date)) | hist-kline-fetcher |
| `kline_hk` | 同上 | 同上 | 同上 |
| `kline_us` | 同上 | 同上 | 同上 |

### 场内+场外完整覆盖

- **场外**：开放式 (`fund_daily`) + 货币 (`fund_money_daily`) + 理财 (`fund_financial_daily`) + QDII/港基 (`fund_hk_rank`)
- **场内**：ETF (`fund_etf` + `fund_etf_daily`) + LOF (`fund_lof`) + 分级 (`fund_graded_daily`) + 封闭式 (`fund_close_daily`) + FOF (`fund_fof_daily`) + REITs (`fund_reits_daily`)

---

## §3 代码改动

### 文件结构

```
lambda/shared/
├── storage/
│   ├── s3_client.py              # 不变（双写 raw）
│   └── iceberg_writer.py         # 新增
├── schemas/                       # 新增
│   ├── __init__.py
│   ├── registry.py                # 27 张表的 spec
│   └── normalizers.py             # 中文列 → 英文 partition key
└── fetchers/
    ├── base_fetcher.py            # 改 ~30 行
    └── fund_fetcher.py            # 加 4 个 _fetch_*

lambda/iceberg-maintenance/        # 新增 Lambda
├── Dockerfile
├── handler.py
└── requirements.txt

cdk/lib/
└── fund-data-fetch-stack.ts      # 改 ~80 行

scripts/
├── backfill_to_iceberg.py        # 新增（一次性）
└── iceberg_init_tables.py        # 新增（dry-run 建表）
```

### `shared/schemas/registry.py`

```python
from dataclasses import dataclass
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec

@dataclass
class TableSpec:
    name: str
    schema: Schema
    partition_spec: PartitionSpec
    identifier_fields: list[str]   # PK 字段名（用于 upsert）
    write_mode: str                # "upsert" | "append"
    source_category: str            # "fund" | "kline_a" | ...
    date_column: str                # normalizer 用，转 trade_date 等

TABLES: dict[str, TableSpec] = {
    "fund_daily": TableSpec(...),
    # ... 26 entries
}
```

### `shared/storage/iceberg_writer.py`

```python
class IcebergWriter:
    def __init__(self, database: str, warehouse_path: str):
        self.catalog = load_catalog("glue", **{
            "type": "glue",
            "glue.region": os.environ["AWS_REGION"],
            "warehouse": warehouse_path,
        })
        self.database = database

    def write(self, table_name: str, df: pd.DataFrame) -> dict:
        spec = TABLES[table_name]
        df = normalize(df, spec)                            # 中文列 → trade_date
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        table = self._ensure_table(spec, arrow_table.schema)
        if spec.write_mode == "upsert":
            result = table.upsert(arrow_table)
            return {"rows_inserted": result.rows_inserted,
                    "rows_updated": result.rows_updated}
        else:
            table.append(arrow_table)
            return {"rows_appended": len(arrow_table)}
```

### `shared/fetchers/base_fetcher.py`（改动）

```python
def _safe_fetch(self, name, fetch_fn) -> FetchResult:
    df = fetch_fn()
    raw_result = self.s3.upload_dataframe(df, self.category, name)  # 现有
    try:
        iceberg_result = self.iceberg.write(name, df)               # 新增
    except Exception as e:
        self.logger.error(f"Iceberg write failed for {name}: {e}")
        iceberg_result = {"error": str(e)}
    return FetchResult(name=name, raw=raw_result, iceberg=iceberg_result)
```

### `lambda/iceberg-maintenance/handler.py`（新增）

```python
def lambda_handler(event, context):
    writer = IcebergWriter(...)
    for table_name in TABLES:
        table = writer.catalog.load_table(f"fund_data_lake.{table_name}")
        table.rewrite_data_files(target_size_bytes=134_217_728)  # 128 MB
        table.expire_snapshots(older_than=timedelta(days=14))
```

### CDK 改动

```typescript
// 1. Glue Database
const glueDatabase = new glue.CfnDatabase(this, "FundDataLakeDb", {
  catalogId: this.account,
  databaseInput: { name: "fund_data_lake" },
});

// 2. IAM 给 fetcher Lambda 加 Glue 权限
const icebergPolicy = new iam.PolicyStatement({
  actions: ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable",
           "glue:UpdateTable", "glue:GetTables"],
  resources: [`arn:aws:glue:${region}:${account}:catalog`,
              `arn:aws:glue:${region}:${account}:database/fund_data_lake`,
              `arn:aws:glue:${region}:${account}:table/fund_data_lake/*`],
});

// 3. Maintenance Lambda + Weekly Schedule
const maintenanceLambda = this.createDockerLambda(
  "IcebergMaintenanceLambda", lambdaDir,
  "iceberg-maintenance/Dockerfile",
  "Weekly Iceberg compaction + snapshot expiration",
  3008, 14, lambdaEnv
);
new events.Rule(this, "WeeklyMaintenance", {
  schedule: events.Schedule.cron({ minute: "0", hour: "20", weekDay: "SUN" }),
  targets: [new targets.LambdaFunction(maintenanceLambda)],
});
```

### 改动量统计

| 类型 | 文件数 | 估算行数 |
|---|---|---|
| 新增 | 5 | ~600 |
| 修改 | 3 | ~150 |
| 不动 | data-processor / catalog-generator / MCP | 0 |

---

## §4 错误处理 & 历史回填

### 错误处理矩阵

| 失败场景 | 影响 | 策略 | 报警 |
|---|---|---|---|
| akshare 接口超时/空 | 单 fetch | retry 3 次 + `_safe_fetch` 隔离，跳过 | 计入错误计数 |
| raw parquet 上传失败 | 单表当日 | retry 3 次后跳过；不阻断 Iceberg | CW 日志 |
| **Iceberg 写入失败** | 单表当日 | **不影响 raw**；当天可手工补写 | ≥3 表/天 → SNS |
| Glue Catalog 节流 | 多表 | pyiceberg 内置重试 | INFO |
| Lambda 超时 | 单 fetcher | 现有 7 个 Lambda 拆分；fund-fetcher 26 接口预估 5-8 分钟 | CRITICAL |
| Schema 加列 | 单表 | pyiceberg 自动 evolve（列 ID 追踪） | INFO |
| Schema 减列/改名 | 单表 | normalizer 检测必填列缺失 → 跳过 | SNS |
| **同日重跑** | 任意 | upsert PK 幂等 | 无影响 |
| compaction 失败 | 仅维护 | 跳过本周 → 下周重试 | 弱告警 |

### 数据正确性保障

1. **行数校验**：`len(arrow_table)` vs `result.rows_inserted + rows_updated`
2. **主键唯一性**：upsert 前 `dropduplicates(subset=identifier_fields, keep='last')`
3. **日期列规范化**：`normalize()` 强制 Date 类型；无法解析的行丢弃
4. **每日双写一致性**：`raw` vs Iceberg 当日分区行数差 >1% → SNS（嵌入 catalog-generator）

### 历史回填

```python
# scripts/backfill_to_iceberg.py 伪代码
for table_name in TABLES:
    spec = TABLES[table_name]
    s3_keys = list_raw_keys(spec.source_category, table_name)
    for key in sorted(s3_keys):
        date_str = parse_date_from_key(key)
        df = pd.read_parquet(f"s3://.../{key}")
        df = normalize(df, spec, fallback_date=date_str)
        writer.write(table_name, df)
```

- 走 upsert（幂等）
- 在 EC2 / 本地跑（非 Lambda），单次约 30-60 分钟
- 完成后跑一次 `rewrite_data_files`
- 行数校验：Iceberg `count(*)` ≈ raw 累计去重行数

### 修复历史脏数据

```python
# 例：修复 2026-04-15 某基金净值
df_correct = ak.fund_open_fund_daily_em()
df_filtered = df_correct[df_correct["基金代码"] == "000001"]
writer.write("fund_daily", df_filtered)  # upsert 自动覆盖
# Athena 立即可见（snapshot 切换原子）
```

### 双写下线时间表

| 阶段 | 时长 | 动作 |
|---|---|---|
| Phase 1 双写并行 | 4 周 | Lambda 双写；processor/MCP 仍读 raw |
| Phase 2 MCP 切流 | 2 周 | data-processor 切 Iceberg 读 |
| Phase 3 raw 只读 | 2 周 | Lambda 停写 raw；旧文件保留 |
| Phase 4 raw 归档 | - | 转 Glacier；新数据只走 Iceberg |

---

## §5 测试策略

### 测试金字塔

```
                ┌─────────────────────┐
                │  Manual Athena 验证 │  (1次, 上线)
                ├─────────────────────┤
                │  E2E (Step Func)    │  (3-5 个场景)
                ├─────────────────────┤
                │  集成测试 (moto)    │  (10-15 个)
                ├─────────────────────┤
                │  单元测试           │  (~40 个)
                └─────────────────────┘
```

### 单元测试（pytest，不依赖 AWS）

| 模块 | 测试要点 |
|---|---|
| `schemas/registry.py` | 27 张表 spec 完整性：identifier_fields ∈ schema、partition 引用正确、write_mode 合法 |
| `schemas/normalizers.py` | 中文列 → trade_date；多日期格式（`2026-05-09` / `20260509` / `2026/5/9`）；缺列降级；时区 |
| `iceberg_writer.write()` | InMemoryCatalog mock：upsert 幂等、append 行数、schema evolve 触发、空 DataFrame 跳过 |
| `base_fetcher._safe_fetch` | raw 成功+iceberg 失败的返回结构、两者皆失败的退化 |

### 集成测试（moto + LocalStack）

- 建表流程：空 catalog → 第一次 write 触发建表 → schema/partition 一致
- Schema evolution：旧表 + akshare 加新列 → 自动 evolve、旧分区可读
- Partition pruning：模拟 Athena 查询 → 只扫目标月分区
- Maintenance：1000 个小文件 → rewrite → 文件数 < 10
- 双写一致性：mock S3 + Glue，验证 raw + Iceberg 行数一致

### E2E 测试（dev 账号实跑）

| 场景 | 验证 |
|---|---|
| 正常一日 | 27 表全部新增分区，行数正常 |
| 同日重跑 | upsert 不重复，snapshot+1 |
| akshare 超时 | 单表跳过，其他正常 |
| Schema 漂移 | 手工加列，下次写入 evolve |
| Athena 查询 | `SELECT count(*) FROM fund_data_lake.fund_daily WHERE trade_date >= ...` 正确 |
| DuckDB 本地 | `iceberg_scan(...)` 与 Athena 结果一致 |

### 持续校验

每日 catalog-generator 末尾跑：

```python
def daily_validation():
    for table_name in TABLES:
        raw_rows = count_parquet_rows(f"s3://.../{table_name}/{date}/")
        iceberg_rows = catalog.load_table(f"fund_data_lake.{table_name}") \
            .scan(row_filter=f"trade_date = '{date}'").count()
        if abs(raw_rows - iceberg_rows) / max(raw_rows, 1) > 0.01:
            send_sns_alert(...)
```

### 不测试（YAGNI）

- pyiceberg 自身（信任上游）
- akshare 数据真实性（用 fixture）
- Athena 查询性能（数据量小）

---

## §6 成本估算（月）

| 资源 | 当前 | 改造后 | 增量 |
|---|---|---|---|
| S3 存储 | ~5 GB raw | ~10 GB（双写期）→ 6 GB（下线后） | +$0.1 |
| S3 PUT | ~600/天 | ~1800/天 | +$0.05 |
| Glue Catalog | 0 | 27 表 + 元数据 | $0（前 100 万对象/月免费） |
| Lambda fetcher | ~$3 | +30%（pyiceberg 开销） | +$1 |
| Lambda maintenance | 0 | 4 次/月 × 5 min × 3 GB | +$0.2 |
| Athena | 按需 | 按需 | 0 |
| **合计** | | | **~$2/月** |

vs 方案 C（S3 Tables）：~$15-25/月（10×）。

---

## §7 实施里程碑

| # | Milestone | 工作量 | 交付物 |
|---|---|---|---|
| **M1** | Schema + IcebergWriter | 1 天 | `shared/schemas/`、`iceberg_writer.py`、单元测试 |
| **M2** | CDK Glue DB + IAM | 0.5 天 | Glue Database、Lambda 角色、dry-run |
| **M3** | base_fetcher 双写 | 0.5 天 | 改 base_fetcher，dev 跑通 |
| **M4** | 补 4 张新表（close/fof/reits/portfolio_hold） | 1 天 | fund_fetcher 加 4 个 _fetch_*、registry 加 4 条 |
| **M5** | Maintenance Lambda | 0.5 天 | 新 Lambda、周触发、手工验证 |
| **M6** | 历史回填 | 1 天 | `backfill_to_iceberg.py`、行数校验 |
| **M7** | 双写下线 | 4-8 周 | Phase 2-4 推进 |

**关键路径**：M1 → M2 → M3 线性；M4/M5 可并行；M6 在 M3 进 prod 后启动。

**总开发**：约 4-5 个工作日。

---

## 附录 A — 方案对比

| 维度 | A: Hive + Glue | **B: 自建 Iceberg + Glue ⭐** | C: AWS S3 Tables |
|---|---|---|---|
| Upsert | ❌ 整分区 OVERWRITE | ✅ `table.upsert()` 行级 | ✅ 同 B |
| Schema 演进 | ❌ 重写历史 | ✅ 列 ID 追踪 | ✅ 同 B |
| Athena | Engine v2+ | Engine v3+ | 原生 |
| DuckDB | `read_parquet` | `iceberg_scan()` | 需新版 |
| 运维成本 | 最低 | 低（每周 compact） | 最低（自动） |
| 托管费 | 0 | 0 | $15-25/月 |
| 升级路径 | 重做 | → S3 Tables 几乎零成本 | 终态 |

**B 胜出原因**：
1. 用户勾选 upsert / 修复历史 → A 出局
2. 数据量 20+ 表 × 几 MB/天 → C 托管费比自建贵 ~10×
3. B → C 升级时数据格式不变（同 Iceberg 协议）

---

## 附录 B — 决策日志

| 决策 | 选择 | 理由 |
|---|---|---|
| 表格式 | Apache Iceberg (自建) | 成本 + 灵活度最佳平衡 |
| Catalog | AWS Glue | 与 Athena/EMR 原生集成；前 100 万对象免费 |
| 主键策略 | (代码, 日期) 复合 PK | 支持当日重跑幂等 + 修复历史 |
| 高频分区 | month(date) | 月分区裁剪足够，单分区 ~30 文件可接受 |
| 低频分区 | year(date) | 季报/事件型数据量小，年分区即可 |
| Snapshot 保留 | 14 天 | 足够回溯一次盘后修复，metadata 开销可控 |
| 双写过渡期 | 4-8 周 | 留足验证 + MCP 切流时间 |
| Maintenance | 独立 Lambda + 周触发 | 与 fetcher 解耦，失败不影响每日抓取 |
