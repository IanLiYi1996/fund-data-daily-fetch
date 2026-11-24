# 2026-11-24 货币基金历史缺口修复

账户 `463470973226`，区域 `us-east-1`。本文用户可见时间均为北京时间。

## 现象与根因

- 11 月 24 日凌晨监控报告 `001973@2026-11-22` 缺失。
  基金为光大保德信耀钱包货币 A，上游披露值 `0.3655`。
- 最新补采实际返回 `success=true`、`complete=true`，普通补采与货币核验
  的 `errors/pending` 均为 0。请求成功不等于历史日期完整。
- 直接查湖和已复制消费文件，均没有该日期；已有日期的值与上游相符。
- 生产 `_verify_money_funds` 只在 `(fund_code, trade_date)` 已经存在时纠正
  值差异。回看响应里虽有新增历史日期，也会被筛掉，并返回核验成功。
- 原函数的隔离回放和真实调用点回归测试均复现遗漏。现有日志无法还原
  该历史日期在上游首次发布的时刻，不能据此断言原始请求曾超时。

## 代码修复与生产部署

- 复用货币基金既有 5 天回看请求，补入上游实际返回、但湖内没有的日期，
  同时保留纠正已有错误值的能力。不生成周末日期，不扩展全市场扫描。
- `added` 与 `corrected` 分开统计；已有正确值不写，空值不填零，
  越界日期拒绝写入。主分支版本也检查提交错误及不完整提交。
- 337 项测试通过。新测试先复现 7 项失败，再验证缺失周日、空本地值、
  幂等、最新写入优先、舍入容忍、越界日期及失败提交等边界。
- 当前主分支缺少生产中此前上线的部分 FOF、预算、分批提交修复。
  因此未从当前分支重建整个 Lambda，更未做全栈 CDK 部署。
- 热修镜像以不可变生产摘要为基线，只改变 `_verify_money_funds`。
  构建脚本检查原 handler SHA，并逐个比较其他顶层 AST 节点未变。
  生产镜像中的真实函数也验证了首次补入 1 行、再次核验不再追加。

基线镜像：

```text
sha256:e12b43b52e4086bf9caef82efc50d84a7ad6da36675677a78d0db0615490b3c5
```

原 handler SHA-256：

```text
b5ee5832212a81f4a552332d65c439b042aff94f9ca5f4e9e75be979badd9a57
```

已上线镜像（原 ECR 资产仓库）：

```text
sha256:b7a3bc1e8ee86eae9ef9d2be072093f8c356c2272a504c2d85c313b28e93cbcf
```

函数 `FundDataFetchStack-FundFallbackLambda82226D2E-dt1F6pJ1tsdM`
已读回确认 `Active / Successful`、解析后的镜像摘要一致。环境变量、
角色、内存、超时、架构、网络及其他配置逐项相同。没有 IAM 写入。
这是配置上线及隔离回归验收，不是声称下一次定时采集已经运行。

## 单行数据修复与验收

- 原湖快照 `8900711743546813456`；原消费文件版本
  `VtzwAIUBjHtgMTte2vHecg5PbnqFXViI`，440,068 行。
- 写入前再次校验源基金、源日期、有限数值、成员类型、快照/对象版本及
  没有正在运行的采集工作流；先备份源响应、湖元数据与完整消费文件。
- 仅追加 `001973@2026-11-22`：`unit_nav=0.3655`、
  `accum_nav=0.664`。独立查湖确认该键仅 1 行。
- 新湖快照 `4570511304526723229`，父快照与上述基线相符，
  写入器返回 `rows_appended=1`。
- 仅调用原月度导出函数重导 `2026-11`。新文件 440,069 行，
  原 440,068 行全部键和值逐项相同，唯一新增键为批准的基金/日期。
- 新消费文件版本 `mJQadDGeZ54l1MAlvGG_WjXnvkpLzHIX`，
  10,431,456 字节，北京时间 2026-11-24 11:10:23 更新；
  复制已读回确认 `COMPLETED`。
- 北京时间 11:13 独立核验该基金 11 月 15—23 日的 8 个实际披露日期，
  缺失日期 0、数值差异 0，目标键仍只有 1 行。

消费文件：

```text
s3://fund-data-pipeline-463470973226-us-east-1/
fund-data-pipeline/fund_history/trade_month=2026-11/part-0.parquet
```

私人备份和验收回执：

```text
fund-data-pipeline/_repairs/2026-11-24-money-history/8900711743546813456/
```

其中保存 `plan.json`、`upstream-before.json`、
`consumer-before.parquet`、`iceberg-metadata-before.json`、
`append-verified.json`、`consumer-verified.json`、
`lambda-before.json`、`lambda-deployed-verified.json`、
`target-and-replication-verified.json`。
原对象版本和原湖快照保留，没有删除数据文件。

## 操作入口与范围

```bash
.venv/bin/python scripts/repair_money_history_20261124.py
.venv/bin/python scripts/verify_money_history_image.py --image <immutable-image>
```

修复器默认只读。写入必须显式 `--apply` 并传入刚验收的
`--expected-snapshot`、`--expected-version`；重复运行已完成的修复不会
再次追加。导出失败后也不能盲目重复追加，应查湖和消费版本分别处理。

未重跑全基金采集，未改变生命周期/豁免名单，未调用会写名单和发送
Slack 的完整新鲜度 Lambda。此次只修复所报历史缺口及其漏补机制；
此前诊断过的请求无重试问题不在本次变更内。

后续全栈部署前仍须整合其他分支中的已上线热修，避免回退无关链路。
