# Spike: 替代数据源能否补上剩余缺口（Issue #1）

> **结论先行**：714 只缺口里 **34 只可补**（全是 FOF，用 `pingzhongdata.js`），
> **680 只上游确实没有当期数据**。Issue 里"~1000 只、货币基金占 ~900"的
> 假设已被 `fund-fallback-fetcher` 上线后的实测推翻。
>
> 数据明细：[`issue-1-missing-codes.csv`](issue-1-missing-codes.csv)（714 行）

---

## 1. 真实缺口清单

口径：最新 `fund_name` universe 减去 `fund_daily` 在 2026-07-05 之后有任何一行的
fund_code —— 即**日常 fallback 跑过之后仍然拿不到的**。

| | |
|---|---|
| universe | 27,468 |
| 近 30 天有数据 | 26,754 |
| **缺口** | **714** |

按基金类型：

| 类别 | 只数 |
|---|---|
| other（普通份额，无明显特征） | 215 |
| bond_periodic（债券/定开/持有期） | 121 |
| backend_share（后端 C/D/E/H/R） | 93 |
| fof | 87 |
| reits | 84 |
| etf_lof | 72 |
| money_market | 29 |
| qdii_other | 11 |
| qdii_fx_share（美元现汇） | 2 |

**与 Issue 原始估算的偏差**：Issue 写"货币基金 ~900 只"，实测只有 **29 只**。
差异来自 `fund-fallback-fetcher`（天天基金 `lsjz`）上线后已经把货币基金基本覆盖，
估算是在那之前做的。同理 QDII 美元现汇实际只有 **2 只**，不是 ~15 只。

---

## 2. 候选源探测

对每个类别抽样 8 只（美元现汇仅 2 只，全测），试三个端点：

| 源 | 说明 |
|---|---|
| `lsjz` | `api.fund.eastmoney.com/f10/lsjz` —— **现用**，作为基线 |
| `pingzhongdata` | `fund.eastmoney.com/pingzhongdata/{code}.js` —— 基金详情页背后的 JS |
| `fundgz` | `fundgz.1234567.com.cn/js/{code}.js` —— 盘中估值 |

抽样命中率（宽松判定：只要变量名出现即算命中）：

| 类别 | lsjz | pingzhongdata | fundgz |
|---|---|---|---|
| qdii_fx_share | 0% | 100% | 0% |
| qdii_other | 0% | 88% | 0% |
| etf_lof | 0% | 62% | 0% |
| backend_share | 38% | 100% | 0% |
| fof | 62% | 100% | 0% |
| money_market | 38% | 62% | 0% |
| reits | 100% | 100% | 0% |
| bond_periodic | 25% | 88% | 0% |
| other | 0% | 12% | 0% |

`fundgz` 全线 0%（只有实时估值，无历史），排除。

**⚠️ 这张表会误导**。宽松判定把"变量存在但数组为空"也算命中 —— 例如
`019739`（美元现汇）在这里显示 100%，实际解析出来是空的。真实结论看下一节。

---

## 3. 严格测量（全部 714 只）

判定改为**真正 JSON 解析** `Data_netWorthTrend` / `Data_ACWorthTrend` /
`Data_millionCopiesIncome`，并要求最新数据点 ≥ 2026-07-05：

| 判定 | 只数 | 含义 |
|---|---|---|
| **recoverable** | **34** | 有当期数据，可补 |
| stale_only | 182 | 有历史但最新点早于 7/05 |
| no_series | 498 | 三个变量全空或不存在 |

按类别：

| 类别 | no_series | stale_only | **recoverable** |
|---|---|---|---|
| fof | 35 | 18 | **34** |
| other | 194 | 21 | 0 |
| bond_periodic | 90 | 31 | 0 |
| etf_lof | 68 | 4 | 0 |
| reits | 13 | 71 | 0 |
| backend_share | 64 | 29 | 0 |
| money_market | 21 | 8 | 0 |
| qdii_other | 11 | 0 | 0 |
| qdii_fx_share | 2 | 0 | 0 |

### `stale_only` 182 只不是缺失

最后数据点的年份分布说明这些是**已终止/停更的产品**，不是我们漏采：

| 最后数据点 | 只数 |
|---|---|
| ≤ 2025 年 | 80 |
| 2026-06 | 48（其中 16 只落在 06-11~06-16） |
| 2026-07 | 8 |
| 其他 2026 | 46 |

- **REITs 71 只**样本最后点在 2024-03 ~ 2025-10（如 `508056` 中金普洛斯 REIT
  停在 2024-03-30）—— 早已摘牌或转让。
- **backend_share** 有一批停在 2026-06-11~06-16，恰好是桶删除事故窗口 ——
  但**上游数据本身也停在那**（清盘/份额转型），不是我们的采集缺口。

---

## 4. 结论

### 可行动

**34 只 FOF 可以补**，用 `pingzhongdata.js` 解析 `Data_netWorthTrend`。
这些是 3~6 个月持有期的新 FOF，`lsjz` 端点对它们返回空，但 pingzhongdata 有数据
（最新点 2026-07-23）。

实现成本低：`fund-fallback-fetcher` 已有"算差集→补漏"的骨架，只需加一个
pingzhongdata 解析分支作为 `lsjz` 之后的第二道 fallback。

### 不可行动（有证据）

- **QDII 美元现汇（2 只）+ qdii_other（11 只）**：三个端点全部 `no_series`。
  Issue 假设"数据在但端点不对"**已被推翻** —— 上游确实没有这些份额的净值序列。
  境外计价份额不在境内基金页面披露。
- **money_market 剩余 29 只**：21 只 no_series、8 只 stale。天天基金 `lsjz`
  已覆盖绝大多数货币基金，剩下这些是停售产品。
- **REITs 84 只**：71 只有历史但已终止，13 只无序列。属正常业务状态。
- **other 215 只 / bond_periodic 121 只**：no_series 占绝对多数（194 / 90），
  多为定开期外、清盘、或从未成立。

### 未验证的付费源

Wind / CSMAR / Choice 未测（需采购）。鉴于免费源已覆盖到只剩 **34 只可补 +
680 只上游确实无数据**，为剩余部分付费**不划算** —— 680 只里绝大多数是已终止
产品，付费源同样不会有当期数据。

---

## 5. 建议的后续动作

1. **做**：给 `fund-fallback-fetcher` 加 pingzhongdata 分支，回收 34 只 FOF。
   预计 +34 只/日覆盖，改动约 40 行。
2. **做**：把 `stale_only` 182 只 + `no_series` 中已确认终止的部分，在
   freshness-check 里建立**已知豁免名单**，避免它们长期计入"缺失"造成噪音。
3. **不做**：付费数据源采购。
4. **不做**：继续找 QDII 美元现汇的替代源 —— 已有三源否证。

## 附：复现方式

```bash
# 1. 生成缺口清单
#    universe(fund_name latest) - fund_daily(since 2026-07-05)
# 2. 严格测量
#    对每只 code 拉 pingzhongdata.js，JSON 解析三个变量，要求最新点 >= 7/05
```
脚本见 spike 期间的 `measure.py`（未入库，逻辑已在本文档说明）。
