# 2026-11-02 港基历史净值重建

## 恢复口径

针对 `fund_hk_rank` 在 2026-10-27、28、29 的原始排行快照缺口，发布一份
**与原表隔离的净值日期重建版**。这不是当时实际采集到的排行，也不声称恢复
当时的信息可见性。原始快照缺口继续保留。

此前已检查这三天的 raw S3 版本、旧 Iceberg 文件、修复备份、采集日志及冗余
净值/排行表。原始 raw 序列化失败，Iceberg 写入前每天丢掉了 187/191 行，
留下四行错位数据；没有找到可还原完整原始快照的副本。

不能把历史净值直接补进原表：净值日与原始采集日不是同一概念，披露延迟会变化。
上游 `MethodFundList` 的 `date1/date2` 也只影响自定义收益，不能获取过去的完整排行。

## 来源与覆盖

- 名册固定为 10-26 正常 raw 快照中的 191 只基金，不使用今天的名册倒填过去。
- 逐基金请求同一上游 `HKFDApi / MethodJZ / action=2` 历史净值接口，完全分页，
  按 JSON 字段名读取 `FCODE / HKFCODE / PDATE / NAV / NAVCHGRT / CURRENCY`。
- 成功请求 191/191 只，共取得 39,943 条记录。为计算基准读取一个有界历史窗口，
  不是重新下载、核验全市场或每只基金成立以来的全部历史。
- 176 只基金的正常快照净值，与历史接口中**实际净值日**的数值逐一一致。
- `968008` 的历史接口返回成功但为空。只保留 10-26 正常 raw 中可证明的
  `2017-11-09` 净值 `46.06`，标记 `known_good_raw_snapshot`；
  不重标净值日期，也不伪造这只停更基金的周/月/年涨幅。
- 14 只在正常快照及请求窗口内均无可用净值，继续保留空值；接口失败不能被当作
  “没有净值”。这不等于已核实这 14 只基金从未披露过任何历史数据。

| 净值截止日期 | 名册行数 | 可用净值 | 同日净值 | 沿用旧净值 | 无可用净值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-10-27 | 191 | 177 | 175 | 2 | 14 |
| 2026-10-28 | 191 | 177 | 174 | 3 | 14 |
| 2026-10-29 | 191 | 177 | 173 | 4 | 14 |

## 消费路径与字段

生产桶：`fund-data-pipeline-463470973226-us-east-1`。

```text
fund-data-pipeline/fund/_reconstructed/fund_hk_history/
  manifest.json
  as_of_date=2026-10-27/part-0.parquet
  as_of_date=2026-10-28/part-0.parquet
  as_of_date=2026-10-29/part-0.parquet
```

消费者应先读取 `manifest.json`，再读取其中列出的文件并核对 SHA-256。
manifest 最后创建，是整批数据已通过校验的提交标记；没有 manifest 的零散文件
不能被视为已发布数据。不自动接入原始排行的消费者，避免混合两种口径。

该独立子目录位于已有 `fund-data-pipeline/fund/` 复制规则下，自动镜像到
消费桶 `financial-dataset-mx`（账户 `845861764576`）的**同名路径**，
无需修改复制配置或 IAM。脚本在读取源数据前确认该现有规则仍然有效。

所有重建行带有：

- `data_origin=reconstructed`、`observation_time_recovered=false`。
- `as_of_date`：按净值日期进行选择的截止日，不是当时的采集/可见时间。
- `nav_date`：上游实际净值日，必定不晚于截止日；`nav_age_days` 保留陈旧程度。
- `nav_source`：`upstream_history`、`known_good_raw_snapshot` 或 `none`。
- `nav_status`：`same_date`、`carried_forward` 或 `no_reported_nav`。

`daily_return_pct` 取自被选中实际净值日的上游 `NAVCHGRT`。

`nav_change_7d_pct / nav_change_1m_pct / nav_change_1y_pct` 是以实际净值日为终点、
用单位净值比值计算的**价格涨幅**，不是含分红再投资的原始排行收益：

```text
(终点单位净值 / 基准单位净值 - 1) × 100
```

基准取不晚于对应日历截止日期的最近可用记录，另存
`baseline_<period>_date / baseline_<period>_nav`。不足一个完整周期、没有基准，
或币种不同，都保留空值，不补成 0。不能将这些列替换成原表
`weekly_return / monthly_return / yearly_return` 用于回测当时的可见排行。

## 安全、审计与重跑

脚本：`scripts/reconstruct_hk_history_20261027.py`。

```bash
# 默认只读，输出精确计划、原表 snapshot ID 与计划 SHA-256。
.venv/bin/python scripts/reconstruct_hk_history_20261027.py

# 使用刚核验的计划执行；源数据、名册、原表或计划变化都会拒绝发布。
.venv/bin/python scripts/reconstruct_hk_history_20261027.py \
  --apply \
  --expected-hk-snapshot <dry-run-snapshot> \
  --expected-plan-sha256 <dry-run-sha256>

# 已有审计响应时，可重新验证同一批原始响应，避免重复请求上游。
.venv/bin/python scripts/reconstruct_hk_history_20261027.py \
  --source-plan-sha256 <existing-audit-plan-sha256>
```

仅创建隔离目录和本次修复审计目录下的新 S3 对象：

```text
fund-data-pipeline/_repairs/2026-11-02-hk-history-reconstructed/<plan-sha256>/
  hk-original-before.parquet
  membership-original.parquet
  source-responses.json.gz
  plan.json
```

创建使用 `IfNoneMatch=*`；相同内容可幂等重跑，不同内容的已有对象会使整批发布
预检失败。所有写入均读回验证；不覆盖原始 raw、Iceberg 表或净值月度导出。
没有修改 Lambda、IAM、定时规则、生命周期/豁免数据，也没有发送通知。

发布前、manifest 创建前及发布后，核对原港基表 snapshot 和全部原行 SHA-256、
主净值表 snapshot、10 月净值导出版本，以及正常名册 raw 版本都未变化。

测试覆盖字段错序、身份/日期错误、重复主键、非有限/非正净值、完整分页、
空数据与接口错误的区别、未来净值排除、旧快照来源日期、币种不一致、
原始字段隔离、条件写入、冲突拒绝、提交标记顺序和消费端 Parquet 类型。

## 执行结果

消费版发布完成：2026-11-02 09:26:46–09:26:52 UTC，即北京时间
17:26:46–17:26:52。实际发布计划：
`c603e318beb990b19f65608ef1fecb2a44824614563bb01d91e3912e361e450c`。

- 三个 Parquet 和 manifest 均读回校验，文件 SHA-256、191 个唯一基金代码及
  实际净值日通过。逐行独立比对保留的原始响应，531 条非空净值全部有来源依据。
- 上述四份消费文件的复制状态均为 `COMPLETED`。
- 原港基表 snapshot `4810267533687732122`，24,354 行及全部原行 SHA-256 不变；
  原三天的排行快照仍为 0 行。
- 主净值表 snapshot `5289416477245374598` 与 10 月消费导出版本不变。
- 全量测试：`353 passed`。新增测试也覆盖审计响应重放以及复制前缀校验。
- 无凭据进入代码；必需的仓库扫描只匹配忽略的依赖测试/示例及扫描说明，
  本次修改文件无命中。

初版曾写入 `fund-data-pipeline/fund_hk_history_reconstructed/`，
计划为 `6abaedfbe2d0da0bd44dd75543e22de7458d8b6482ac75f4cdf3191c73028f13`。
该前缀不在现有复制规则内，因此它仅保留为初版审计，不作为消费入口。
最终消费版重放并重新验证同一批已保存响应，Parquet 内容 SHA-256 与初版一致；
没有再次估算、请求另一批行情或删除初版文件。
