# 2026-11-02 月初导出缺失与上月迟到净值漏更

账户 `463470973226`，区域 `us-east-1`。
代码更新于 2026-11-02 16:39 北京时间（08:39 UTC）完成；
10 月导出刷新于 16:40 北京时间（08:40 UTC）完成。

## 根因

11-01 采集执行返回 `SUCCEEDED`，但导出子任务是
`month=2026-11 / rows=0 / skipped=no-live-files / success=false`。
湖中最新净值日期仍为 2026-10-30，没有 11 月分区；监控却依据日历月份
强制查找 11 月导出文件，因此报 404。版本历史亦无该文件或删除标记。

此外，11-01 的采集新写入五条 10-30 净值，仍属于上月。导出只处理日历上的
“本月”，不再更新 10 月文件，造成真实消费端漏更，不能仅屏蔽 404。

## 修复

- 日常导出同时刷新当前月、上月，保留显式 `event.month` 的单月重导接口。
- 两个月使用同一个 Iceberg 当前快照；元数据和 manifests 只读取一次。
- 无 live files 的月份不写入文件，返回 `waiting_months`。有实际月度导出时
  整次运行成功；两个月均无数据仍返回失败。
- 监控从最近两个月的真实净值日期判断应检查哪些导出文件，而不是按文件是否
  存在来回退。新月无数据显示等待；有数据时文件缺失、过期、权限错误、复制
  未完成仍告警。上月文件同样检查，避免迟到净值再次漏更。
- 监控只扫描日期列，并先在 Arrow 中去重；没有扩大到全历史扫描。
- 月份规则共用 `shared/quality/export_policy.py`，显式月份在读取/写入前校验。
  导出 Dockerfile 已包含该纯 Python 模块，不增加生产依赖。

## 部署与核验

仅更新 `ExportFundHistoryLambda`、`FreshnessCheckLambda` 的镜像代码，
未修改 IAM、定时任务或其他函数。两支函数均为 `Active / Successful`。
热修基于各自原生产镜像，仅覆盖 handler 和共享月份规则，保留生产依赖版本。

| Lambda | 原镜像 digest | 新镜像 digest |
| --- | --- | --- |
| `FundDataFetchStack-ExportFundHistoryLambda4F255537-P4Y7z0yGykTd` | `sha256:ded2346deee2494ac99b4833255ac9522bc6a68a41a9eacd6f3a704954fd26d7` | `sha256:35c3b67743b129bb9c2bccfda229817994b9bdced60cd7b8a152c9eb8673f660` |
| `FundDataFetchStack-FreshnessCheckLambdaEF97F6F2-Fok5ShDoxxlA` | `sha256:29fa2a85813b0afdb0e97fe7aa6a8c494d5b67d7e9dc0eb9dd12ede075f882b0` | `sha256:41c9902f26092c2b55444277e330e35abf1d3add4b19cd363ca9c9ce0aa454ac` |

镜像位于现有 ECR 仓库
`cdk-hnb659fds-container-assets-463470973226-us-east-1`；
标签分别为 `fund-hotfix-20261102-export-rollover`、
`fund-hotfix-20261102-monitor-rollover`。

- 新回归测试先在原代码上复现 **14 failed / 5 passed**；
  修复后全量测试 **306 passed**。
- 两个生产依赖镜像均通过烟雾测试，涵盖纯模块打包和实际监控分支。
- 实际调用部署后的导出 Lambda，使用默认日常入口而非显式月份：
  `success=true / month=2026-10 / rows=468445 / waiting_months=[2026-11]`。
- 新导出 10,819,875 字节，S3 复制状态 `COMPLETED`。
- 从消费文件逐条核验下面五个代码，均恰好一条 10-30 记录，净值与湖中一致。
- 使用新监控代码对真实湖和 S3 执行只读 `_check_export`：
  `problems=[]`，10 月导出已复制，11 月显示“尚无净值数据，等待首条记录”。
- 未调用完整监控入口：没有发送额外 Slack 消息、维护豁免名单或重写生命周期
  导出。原 Slack 告警消息和 11-01 执行历史不会被修复改写。
- 测试包含模拟跨年边界；没有执行未来月份的数据采集或导出。

| 基金代码 | 日期 | 核验净值 |
| --- | --- | --- |
| `019309` | `2026-10-30` | 1.1313 |
| `019310` | `2026-10-30` | 1.1554 |
| `027017` | `2026-10-30` | 1.0000 |
| `028803` | `2026-10-30` | 1.0000 |
| `028804` | `2026-10-30` | 1.0000 |

10 月消费文件由 468,440 行更新为 468,445 行。
11 月仍没有实际净值，未创建空 parquet，也未将 10 月数据改标为 11 月。
一旦新月有数据，监控就会严格检查该月文件。

## 回滚与审计

原 10 月消费文件版本：`S88BDmbNlkiUDV6ba72Wba7U0VvVMp4u`。
新版本：`M3N7f_vhdI0ZnU.HwZeG3oWFLpqFnvvl`。
S3 原版本保留，另有固定备份与验收记录：

```text
s3://fund-data-pipeline-463470973226-us-east-1/fund-data-pipeline/_repairs/2026-11-02-export-rollover/
  october-before.parquet
  before.json
  export-result.json
  verified.json
```

生产热修阶段未创建 Git 提交或 PR/MR；源码、回归测试和验收记录通过修复分支交付，
此前港基与 fallback 的修复一并保留。
CloudFormation 仍引用旧镜像；下次正常 CDK 部署必须使用本次修复后的源码重建，
避免从旧 checkout 还原。回滚代码可使用上表原镜像 digest。
