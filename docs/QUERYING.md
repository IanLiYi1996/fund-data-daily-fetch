# 基金数据查询指南

本管道把数据落在两个地方：

| 路径 | 格式 | 用途 |
|---|---|---|
| `s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/{category}/{date}/*.parquet` | 当日 raw parquet 快照 | 排障、一次性脚本、历史回填源 |
| `s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/iceberg/fund_data_lake.db/{table}/` | Apache Iceberg 表 | **所有历史查询、SQL 分析、排行榜** |

**强烈推荐走 Iceberg**：它给你 SQL、时间旅行、分区裁剪、自动增量。Raw parquet 只适合看"某一天的全量快照"。

---

## 快速开始

### 方式 1：DuckDB（本地，最快）

前提：已 `uv pip install duckdb`；本机已配置 AWS 凭证（IAM user / instance profile 均可）。

```python
import duckdb

con = duckdb.connect()
con.sql("INSTALL iceberg; LOAD iceberg;")
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
con.sql("SET unsafe_enable_version_guessing = true;")

BUCKET = "fsi-investmentadvisory-data-463470973226-us-east-1"
DB = f"s3://{BUCKET}/fund-data-pipeline/iceberg/fund_data_lake.db"

def tbl(name: str) -> str:
    return f"iceberg_scan('{DB}/{name}')"

# 查华夏成长的历史净值
df = con.sql(f"""
    SELECT trade_date, unit_nav, accum_nav, daily_return_pct
    FROM {tbl('fund_daily')}
    WHERE fund_code = '000001'
      AND unit_nav IS NOT NULL
    ORDER BY trade_date DESC
""").df()
print(df)
```

#### 注意：Iceberg 内可能有冗余行

历次 run 的 append-fallback 会产生重复（同 `fund_code + trade_date`）。查询时用 `DISTINCT` 或取 `max(snapshot_time)` 的那行：

```python
# 每 (fund_code, trade_date) 只保留一行
sql = f"""
    SELECT DISTINCT ON (fund_code, trade_date)
           fund_code, trade_date, unit_nav, accum_nav, daily_return_pct
    FROM {tbl('fund_daily')}
    WHERE fund_code = '000001'
    ORDER BY fund_code, trade_date, unit_nav DESC NULLS LAST
"""
df = con.sql(sql).df()
```

> Weekly maintenance Lambda 会每周日 compact 一次，届时冗余自动合并。

---

### 方式 2：Athena（SQL 控制台）

1. AWS Console → Athena
2. Workgroup 选一个 Engine v3
3. 数据库下拉选 `fund_data_lake`（27 张表已自动注册到 Glue Catalog）

```sql
-- 某基金本月净值
SELECT trade_date, unit_nav, accum_nav, daily_return_pct
FROM fund_data_lake.fund_daily
WHERE fund_code = '000001'
  AND trade_date >= DATE '2026-05-01'
ORDER BY trade_date DESC;

-- 今日涨幅榜前 10（开放式基金）
SELECT fund_code, fund_name, daily_return_pct
FROM fund_data_lake.fund_daily
WHERE trade_date = current_date
  AND unit_nav IS NOT NULL
ORDER BY daily_return_pct DESC
LIMIT 10;
```

Athena 自动用分区裁剪（月分区），扫描量少。第一次查某张表需要 `MSCK REPAIR TABLE` 或等 Glue Crawler——但我们用 Iceberg，Athena 能直接读 metadata，不需要 repair。

---

### 方式 3：pyiceberg（Python，编程式访问）

适合 notebook 里做分析、pipeline 里做消费。

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("glue", **{
    "type": "glue",
    "glue.region": "us-east-1",
    "warehouse": "s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/iceberg/",
})

table = catalog.load_table(("fund_data_lake", "fund_daily"))

# 过滤条件会下推到 parquet，不全量扫
df = (
    table
    .scan(row_filter="fund_code = '000001'")
    .to_pandas()
)

# 时间旅行：看昨天的快照
snapshots = list(table.snapshots())
yesterday_snap = [s for s in snapshots if s.timestamp_ms < today_ms][-1]
df_historical = (
    table.scan(snapshot_id=yesterday_snap.snapshot_id).to_pandas()
)
```

---

### 方式 4：Pandas 从 raw parquet 读（排障用）

```python
import pandas as pd

s3_path = "s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/fund/2026-05-11/fund_daily.parquet"
df = pd.read_parquet(s3_path)  # 需要 pyarrow + s3fs
```

Raw parquet 是当日全量快照，没去重、没 schema normalize（还是 akshare 原始中文列）。仅用于排查数据质量。

---

## 表清单（27 张 Iceberg 表）

### 每日高频（主键 `fund_code, trade_date`，月分区）
| 表 | 内容 | akshare 源 |
|---|---|---|
| `fund_daily` | 场外开放式基金每日净值 | `fund_open_fund_daily_em` |
| `fund_etf` | 场内 ETF 实时行情 | `fund_etf_spot_em` |
| `fund_lof` | 场内 LOF 行情 | `fund_lof_spot_em` |
| `fund_money_daily` | 货币基金每日万份收益 | `fund_money_fund_daily_em` |
| `fund_financial_daily` | 理财基金每日收益 | `fund_financial_fund_daily_em` |
| `fund_etf_daily` | 场内 ETF 净值 + 折溢价 | `fund_etf_fund_daily_em` |
| `fund_graded_daily` | 分级基金净值 | `fund_graded_fund_daily_em` |
| `fund_value_estimation` | 盘中估值 | `fund_value_estimation_em` |
| `fund_reits_daily` | 公募 REITs | `reits_realtime_em` |

### 排行 / 评级（主键 `fund_code, snapshot_date`）
| 表 | 内容 |
|---|---|
| `fund_performance` | 业绩排行 |
| `fund_exchange_rank` | 场内排行 |
| `fund_money_rank` | 货币排行 |
| `fund_hk_rank` | 港基排行 |
| `fund_dividend_rank` | 分红排行 |
| `fund_rating` | 多机构评级（长表 `fund_code, snapshot_date, rating_agency → rating`） |

### 低频 / 事件（按年分区）
| 表 | 主键 | 内容 |
|---|---|---|
| `fund_name` | `fund_code, snapshot_date` | 基金基本信息 SCD-1 |
| `fund_manager` | `manager_id, snapshot_date` | 基金经理 |
| `fund_dividend` | `fund_code, event_date` | 分红历史 |
| `fund_split` | `fund_code, event_date` | 拆分历史 |
| `fund_purchase` | `fund_code, snapshot_date` | 申购状态 |
| `fund_index_info` | `fund_code, snapshot_date` | 指数型基金元信息 |
| `fund_portfolio_hold` | `fund_code, report_date, holding_code` | 持仓明细（季度，当前 stub） |

### K 线（三市）
| 表 | 主键 | 内容 |
|---|---|---|
| `kline_a` | `code, freq, trade_date` | A 股历史 K 线（当前 stub） |
| `kline_hk` | 同 | 港股 |
| `kline_us` | 同 | 美股 |

---

## 常见查询模板

### 单基金完整历史（去重版）

```sql
SELECT DISTINCT ON (trade_date)
       trade_date, unit_nav, accum_nav, daily_return_pct
FROM fund_data_lake.fund_daily
WHERE fund_code = ?
ORDER BY trade_date DESC, unit_nav DESC NULLS LAST;
```

### 某只基金的评级历史

```sql
SELECT snapshot_date, rating_agency, rating
FROM fund_data_lake.fund_rating
WHERE fund_code = ?
ORDER BY snapshot_date DESC, rating_agency;
```

### 某机构管理的所有基金

```sql
SELECT DISTINCT
       fp.fund_code, fp.fund_name, fn.fund_type
FROM fund_data_lake.fund_performance fp
LEFT JOIN fund_data_lake.fund_name fn USING (fund_code)
WHERE fp.fund_name LIKE '%华夏%';
```

### 某日净值涨幅榜 Top 20（跨 ETF + 开放式）

```sql
WITH open_end AS (
  SELECT fund_code, fund_name, daily_return_pct
  FROM fund_data_lake.fund_daily
  WHERE trade_date = DATE '2026-05-11' AND unit_nav IS NOT NULL
),
etf AS (
  SELECT fund_code, fund_name, change_pct AS daily_return_pct
  FROM fund_data_lake.fund_etf
  WHERE trade_date = DATE '2026-05-11'
)
SELECT * FROM open_end
UNION ALL SELECT * FROM etf
ORDER BY daily_return_pct DESC
LIMIT 20;
```

### 基金经理管理规模变化（时间序列）

```sql
SELECT snapshot_date, aum, tenure_return
FROM fund_data_lake.fund_manager
WHERE manager_name = '艾邦妮'
ORDER BY snapshot_date;
```

### 全市场某日分红发放

```sql
SELECT fund_code, fund_name, dividend_amount, event_date, payment_date
FROM fund_data_lake.fund_dividend
WHERE event_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
ORDER BY dividend_amount DESC;
```

---

## 数据刷新节奏

| 触发 | 时间 | 动作 |
|---|---|---|
| EventBridge `FundDataFetchDailySchedule` | 每日 17:00 UTC（北京 01:00） | 抓取 → raw + Iceberg 双写 |
| EventBridge `WeeklyMaintenanceRule` | 周日 20:00 UTC | Iceberg 小文件 compact + 旧快照清理（保留 14 天） |

手动触发：

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:463470973226:stateMachine:FundDataCollectionWorkflow \
  --input '{"triggered_by":"manual"}'
```

---

## 已知限制

1. **Iceberg 历史深度 = 上线日起**。2026-05-11 上线，之前没有历史。`scripts/backfill_fund_history.py` 可从 akshare 拉单基金完整历史。
2. **重复行**：pyiceberg-core 对部分表 `upsert` 会 SIGSEGV，降级为 append 导致重复。查询时用 `DISTINCT ON` 去重；周日 compaction 后自然合并。
3. **部分列仍为 NULL**（如某些 ranking 表的 `fee`）：这是 akshare 未返回，不是写入 bug。
4. **kline_a / kline_hk / kline_us / fund_close_daily / fund_fof_daily / fund_portfolio_hold**：已注册空表，等待后续 per-code fan-out 设计。
5. **DuckDB 读 `fund_value_estimation`** 目前会报 `Unimplemented type for cast (DATE -> INTEGER)`，这是 DuckDB Iceberg reader 对 `DayTransform` 分区的限制，Athena / pyiceberg 正常。

---

## 故障排查

- **查不到数据**：先确认 `trade_date` 过滤条件在 Iceberg 覆盖的日期内。`_catalog/latest/data_catalog.json` 记录了每日写入的行数。
- **字段全 NULL**：先查 raw parquet（`fund/{date}/*.parquet`），如果 raw 也全 NULL 就是 akshare 本身没返回。
- **DuckDB 报 "No version was provided"**：加 `SET unsafe_enable_version_guessing = true;`
- **Athena 报 "Database fund_data_lake not found"**：切到 Engine v3；region 选 us-east-1。
