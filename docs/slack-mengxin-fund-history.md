@孟老板

新增了两份基金历史数据,已经通过 S3 Replication 同步到你的 bucket 了,直接读取就能用。

## 📁 新文件路径(固定路径,覆盖式更新)

```
s3://financial-dataset-mx/fund-data-pipeline/fund/_history/
├── fund_manager_history.parquet    # 基金经理任期段 (~127k 行 / 26.5k 基金)
└── fund_scale_history.parquet      # 基金规模季度时序 (~121k 行 / 25.9k 基金)
```

注意是**固定路径**,不再按日期分目录。每次刷新会覆盖同一个文件,旧版本通过 S3 Versioning 保留 365 天可回溯。

## 📋 Schema

### `fund_manager_history.parquet`
每只基金多行,多人共管时按经理姓名拆开。

| 列 | 类型 | 说明 |
|---|---|---|
| `基金代码` | str | 6 位代码 |
| `经理姓名` | str | 共管时一行一人 |
| `起始日` | date | 上任日 |
| `结束日` | date | 离任日,**现任为 None** |
| `任期天数` | int | 已解析过的天数 |
| `任期回报` | float | 百分比,e.g. `12.17` 表示 +12.17% |
| `是否现任` | bool | 截止期=至今 时为 True |
| `snapshot_date` | date | 抓取日 |

### `fund_scale_history.parquet`
每只基金按季度多行。

| 列 | 类型 | 说明 |
|---|---|---|
| `基金代码` | str | |
| `报告期` | date | 季报期末日 |
| `期末净资产_亿元` | float | 单位亿元 |
| `净资产环比变动率` | float | 百分比,e.g. `-25.08` 表示 -25.08% |
| `snapshot_date` | date | 抓取日 |

## 🔧 基础用法

```python
import pandas as pd

PREFIX = "s3://financial-dataset-mx/fund-data-pipeline"

# 既有的 daily 文件 (按日期目录, 你之前就在用)
fn = pd.read_parquet(f"{PREFIX}/fund/2026-05-13/fund_name.parquet")
fd = pd.read_parquet(f"{PREFIX}/fund/2026-05-13/fund_daily.parquet")

# 新增的两个 history 文件 (固定路径)
mh = pd.read_parquet(f"{PREFIX}/fund/_history/fund_manager_history.parquet")
sh = pd.read_parquet(f"{PREFIX}/fund/_history/fund_scale_history.parquet")
```

四张表都用 `基金代码` join。

## 💡 常见分析查询

### 1. 取每只基金「当前经理 + 类型 + 最新规模」一行视图

```python
# 现任经理 (一只基金可能多人共管, 这里聚合成 list)
current_mgr = (
    mh[mh["是否现任"]]
    .groupby("基金代码")["经理姓名"]
    .agg(list)
    .reset_index()
    .rename(columns={"经理姓名": "现任经理"})
)

# 最新季度规模
latest_scale = (
    sh.sort_values("报告期")
    .groupby("基金代码")
    .tail(1)
    [["基金代码", "报告期", "期末净资产_亿元"]]
    .rename(columns={"报告期": "最新报告期", "期末净资产_亿元": "最新规模_亿元"})
)

# 一行一只基金的宽表
master = (
    fn[["基金代码", "基金简称", "基金类型"]]
    .merge(current_mgr, on="基金代码", how="left")
    .merge(latest_scale, on="基金代码", how="left")
)
```

### 2. 找规模 100 亿+ 的主动权益基金

```python
big_active = master[
    master["基金类型"].str.startswith("混合型", na=False)
    & (master["最新规模_亿元"] > 100)
].sort_values("最新规模_亿元", ascending=False)
```

### 3. 看某个经理管理过的所有基金 + 任期回报

```python
def manager_track_record(name):
    return (
        mh[mh["经理姓名"] == name]
        .merge(fn[["基金代码", "基金简称"]], on="基金代码")
        .sort_values("起始日", ascending=False)
        [["基金代码", "基金简称", "起始日", "结束日", "任期天数", "任期回报", "是否现任"]]
    )

manager_track_record("张坤")
```

### 4. 单基金的规模走势(季度)

```python
def scale_trend(code):
    return (
        sh[sh["基金代码"] == code]
        .sort_values("报告期")
        [["报告期", "期末净资产_亿元", "净资产环比变动率"]]
    )

scale_trend("110011")
# 报告期      期末净资产_亿元   净资产环比变动率
# 2025-09-30      133.45           4.42
# 2025-12-31      113.85          -14.69
# 2026-03-31       95.44          -16.17
```

### 5. 「经理换人后规模怎么变」分析

```python
# 找任期 > 2 年且规模过百亿的基金,看经理任内规模变化
veterans = mh[(mh["任期天数"] > 730) & (mh["是否现任"] == False)].copy()
# 取每段任期开始/结束季度的规模
def scale_at(code, target_date, window_days=120):
    # 取离 target_date 最近的季报
    rows = sh[sh["基金代码"] == code]
    if rows.empty:
        return None
    rows = rows.assign(diff=(rows["报告期"] - target_date).abs())
    closest = rows.sort_values("diff").iloc[0]
    if closest["diff"].days > window_days:
        return None
    return closest["期末净资产_亿元"]
```

## ⚠️ 注意事项

1. **更新频率**:
   - **manager_history**: 每周日 18:00 UTC(北京时间周一 02:00)全量刷新
   - **scale_history**: 每季度月初(1/4/7/10 月)4 日刷新(等季报基本公布完)
   - 所以 `manager_history` 最多有 6 天延迟,`scale_history` 最多 1 季度 + 3 天延迟

2. **覆盖不完整**:
   - manager_history 覆盖 26,508/26,709 = 99.2% 基金;新发基金或刚清盘的可能缺
   - scale_history 覆盖 25,884/26,709 = 96.9% 基金;封闭式基金、新基金可能没有 fluctuationScale 数据

3. **多人共管处理**:
   - 同一段任期 N 个经理 → manager_history 里有 N 行,起始/结束日相同
   - 用 `groupby("基金代码", "起始日")` 还原成「任期段维度」

4. **NaN 字段说明**(都是数据源固有,不是 bug):
   - `结束日` NaN → 现任经理(可用 `是否现任=True` 判断)
   - `任期回报` NaN(~3%)→ 任期太短或无业绩展示
   - `净资产环比变动率` NaN(~4%)→ 首次建仓季度

5. **历史版本访问**:文件用 S3 Versioning,如果今天的数据有问题想回到上次刷新版本:
   ```bash
   aws s3api list-object-versions --bucket financial-dataset-mx \
     --prefix fund-data-pipeline/fund/_history/fund_manager_history.parquet
   # 找到 VersionId 后用 --version-id 参数读
   ```

6. **不要用 `fund_manager.parquet`(老文件)做任期分析** —— 那个是经理粒度的当前快照(经理 → AUM),`fund_manager_history.parquet` 才是基金粒度的历史任期段。

## 🐛 反馈渠道

数据有问题(行数突然降一半 / schema 变了 / 某只基金数据错乱)直接 ping 我,会查 Step Functions 执行历史和 CloudWatch logs。

---
路径再贴一次:
- `s3://financial-dataset-mx/fund-data-pipeline/fund/_history/fund_manager_history.parquet`
- `s3://financial-dataset-mx/fund-data-pipeline/fund/_history/fund_scale_history.parquet`
