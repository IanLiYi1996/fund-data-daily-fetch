# Fargate 一次性任务：akshare 历史净值回填

**日期**: 2026-05-11
**作者**: ianleely (with Claude)
**状态**: Draft — 待 review

## 背景与目标

当前 Iceberg `fund_daily` 表只有每日自动抓取的数据（上线日起累积）。如果要立刻拿到每只基金的完整历史净值（从成立日到今天），需要从 akshare `fund_open_fund_info_em` 按单基金逐个拉取 — 26,683 只 × 5000+ 行 ≈ 1.3 亿行。

已经写好 `scripts/backfill_fund_history.py`：resumable、并发 4 worker、100 只一批 upsert。在 EC2 本地跑需 4-6 小时。

**目标**：用 AWS Fargate 一次性 task 在云端跑，完成后自动销毁；最小侵入现有 CDK；checkpoint 放 S3 便于断点续传。

**非目标**：
- 不做定时回填（每日 EventBridge 已经在抓新数据了）
- 不做多 task 并发（单 task 4 worker 已够）
- 不做 Step Functions、Service、ALB、EFS 等额外基建

---

## §1 整体架构

```
开发者本机
    │
    │ ① ./scripts/run_backfill_fargate.sh
    │    (aws ecs run-task ...)
    ▼
Fargate Task (1 vCPU / 2 GB, default VPC public subnet)
    ├── ECR Image: backfill-runner
    │   ├── python:3.11-slim
    │   ├── akshare + pyiceberg[glue,pyiceberg-core] + pandas + boto3
    │   ├── /app/shared/
    │   └── /app/backfill_fund_history.py
    │
    ├── ENTRYPOINT: python /app/backfill_fund_history.py
    │                --progress-s3 s3://.../fund-data-pipeline/_backfill/progress.json
    │
    ├── Task Role:
    │   ├── S3 r/w on fund-data-pipeline/*
    │   └── Glue Catalog r/w on fund_data_lake
    │
    └── Log Group: /ecs/fund-history-backfill (retain 14 天)

完成 → Fargate 自动停止 → 不再计费
```

**关键点**：
- Cluster **不跑任何常驻服务**；ECS cluster 是免费的，只在有 task 运行时才按 1vCPU-秒计费（~$0.04/小时）
- 复用 default VPC（已经有 6 个 AZ 的 public subnet），不新建 VPC
- 不用 EFS / ALB / Service Discovery

---

## §2 组件清单

### (a) `lambda/backfill-runner/Dockerfile` — 新建

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY backfill-runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/  /app/shared/
COPY backfill-runner/entrypoint.sh /app/entrypoint.sh
COPY backfill-runner/backfill_fund_history.py /app/backfill_fund_history.py
RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/entrypoint.sh"]
```

- `requirements.txt`: akshare>=1.14, pyiceberg[glue,pyiceberg-core]>=0.7, pandas>=2.0, pyarrow>=14, boto3>=1.34
- `entrypoint.sh`: `#!/bin/sh\nexec python /app/backfill_fund_history.py "$@"`
- **Backfill script 位置**：把脚本从 `scripts/backfill_fund_history.py` 移到 `lambda/backfill-runner/backfill_fund_history.py`（单一位置），`scripts/` 下保留一个 `backfill_fund_history_local.sh` 包装器供本地调用（`exec uv run python lambda/backfill-runner/backfill_fund_history.py "$@"`）。Docker 的 CDK asset context 仍是 `lambdaDir`，`COPY backfill-runner/backfill_fund_history.py /app/` 即可。

### (b) `scripts/backfill_fund_history.py` — 修改：加 S3 checkpoint

新增参数 `--progress-s3 s3://bucket/key`（与 `--progress-file` 互斥）。`Progress.load` / `Progress.save` 自动分支选 S3 或本地。约 20 行改动。

### (c) `cdk/lib/fund-data-fetch-stack.ts` — 修改：添加 ECS cluster + task def

~60 行新增，imports 补上 `aws-ec2`, `aws-ecs`；复用现有 `listBucketPolicy`, `objectPolicy`, `icebergGluePolicy`。

关键片段（参考 §3 代码改动中的 TypeScript 示例）。

### (d) `scripts/run_backfill_fargate.sh` — 新建

Shell 脚本从 CloudFormation outputs 取 cluster name / task-def ARN / subnets，调 `aws ecs run-task`。输出 task ARN 和查看日志的命令。

---

## §3 代码改动

### `cdk/lib/fund-data-fetch-stack.ts` — 在 stack constructor 末尾加入：

```typescript
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";

// ===== Fargate backfill cluster (one-shot tasks) =====
const vpc = ec2.Vpc.fromLookup(this, "DefaultVpc", { isDefault: true });
const backfillCluster = new ecs.Cluster(this, "BackfillCluster", {
  clusterName: "fund-data-backfill-cluster",
  vpc,
  containerInsights: false,
});

const backfillImage = new ecrAssets.DockerImageAsset(
  this, "BackfillImage", {
    directory: lambdaDir,
    file: "backfill-runner/Dockerfile",
    platform: ecrAssets.Platform.LINUX_AMD64,
  }
);

const backfillLogGroup = new logs.LogGroup(this, "BackfillLogs", {
  logGroupName: "/aws/ecs/fund-history-backfill",
  retention: logs.RetentionDays.TWO_WEEKS,
  removalPolicy: RemovalPolicy.DESTROY,
});

const backfillTaskDef = new ecs.FargateTaskDefinition(
  this, "BackfillTaskDef", {
    cpu: 1024,
    memoryLimitMiB: 2048,
    family: "FundHistoryBackfill",
  }
);

backfillTaskDef.addContainer("backfill", {
  image: ecs.ContainerImage.fromDockerImageAsset(backfillImage),
  command: [
    "--progress-s3",
    `s3://${bucketName}/${s3Prefix}_backfill/progress.json`,
  ],
  environment: {
    S3_BUCKET: bucketName,
    S3_PREFIX: s3Prefix,
    WAREHOUSE_PATH: `s3://${bucketName}/${s3Prefix}iceberg/`,
    AWS_REGION: this.region,
  },
  logging: ecs.LogDrivers.awsLogs({
    streamPrefix: "backfill",
    logGroup: backfillLogGroup,
  }),
});

// Task role — reuse existing policy statements
[listBucketPolicy, objectPolicy, icebergGluePolicy].forEach((p) =>
  backfillTaskDef.taskRole.addToPrincipalPolicy(p)
);
backfillTaskDef.node.addDependency(glueDatabase);

// Outputs for run-task CLI
new CfnOutput(this, "BackfillClusterName", {
  value: backfillCluster.clusterName,
  exportName: "FundDataBackfillClusterName",
});
new CfnOutput(this, "BackfillTaskDefArn", {
  value: backfillTaskDef.taskDefinitionArn,
  exportName: "FundDataBackfillTaskDefArn",
});
new CfnOutput(this, "BackfillSubnetIds", {
  value: vpc.publicSubnets.map((s) => s.subnetId).join(","),
  exportName: "FundDataBackfillSubnetIds",
});
```

### `scripts/backfill_fund_history.py` — `Progress` 类支持 S3

```python
# 新增工具函数
def _is_s3_uri(path: str) -> bool:
    return isinstance(path, str) and path.startswith("s3://")

def _s3_parse(uri: str) -> tuple[str, str]:
    _, _, rest = uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    return bucket, key

# Progress.load / save 内部 if _is_s3_uri(path) 走 boto3，否则走原本 Path
```

新增 `--progress-s3` argparse arg，与 `--progress-file` 互斥。

### `scripts/run_backfill_fargate.sh` — 新建

```bash
#!/usr/bin/env bash
set -euo pipefail
STACK="${STACK:-FundDataFetchStack}"
REGION="${REGION:-us-east-1}"

get_out() {
  aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

CLUSTER=$(get_out BackfillClusterName)
TASK_DEF=$(get_out BackfillTaskDefArn)
SUBNETS=$(get_out BackfillSubnetIds)

# Build subnet array for run-task
SUBNET_JSON=$(echo "$SUBNETS" | tr ',' '\n' | jq -R . | jq -sc .)

TASK_ARN=$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$TASK_DEF" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=$SUBNET_JSON,assignPublicIp=ENABLED}" \
  --region "$REGION" \
  --query "tasks[0].taskArn" --output text)

echo "Started task: $TASK_ARN"
echo
echo "View logs:"
echo "  aws logs tail /aws/ecs/fund-history-backfill --follow --region $REGION"
echo
echo "Watch status:"
echo "  aws ecs describe-tasks --cluster $CLUSTER --tasks $TASK_ARN \\"
echo "      --region $REGION --query 'tasks[0].lastStatus'"
```

---

## §4 错误处理

| 场景 | 行为 |
|---|---|
| akshare 单次超时 / 连接中断 | 现有 try/except，记 `progress.failed`，继续 |
| 某只基金持续失败 | 累积在 `progress.failed`；task 正常结束 |
| Fargate task OOM | 实际用 ~500 MB，2 GB 冗余充足 |
| Task 超过 6 小时没结束 | 无 hard timeout；用户可人工 `stop-task` |
| S3 checkpoint 写失败 | log error，继续处理；下次 flush 再试 |
| 镜像 pull 失败 | Fargate task 报 `STOPPED`, reason 可见 |
| 多 task 并发运行（误操作） | upsert 锁冲突；用户责任：`list-tasks` 确认没有再启动 |

---

## §5 测试 & 验证

1. **本地 Docker build 测试**：`docker build -f lambda/backfill-runner/Dockerfile -t backfill-test lambda/`
2. **CDK synth + diff**：`cdk diff` 预览只看到 ECS cluster + task def 新增
3. **部署后烟雾测试**：用一个 override 限 10 只基金的 command：
   ```bash
   aws ecs run-task ... --overrides '{"containerOverrides":[{"name":"backfill","command":["--progress-s3","s3://.../_backfill/smoke.json","--limit","10"]}]}'
   ```
   1-2 分钟完成，CloudWatch log 可见，Iceberg `fund_daily` 有 10 只基金的历史行
4. **全量回填**：`./scripts/run_backfill_fargate.sh` — 4-6 小时完成
5. **进度实时查看**：`aws s3 cp s3://.../_backfill/progress.json - | jq '{done: (.done|length), failed: (.failed|length)}'`

---

## §6 成本估算

- Fargate task: 1 vCPU × $0.04048/h + 2 GB × $0.004445/h ≈ **$0.049/小时**
- 全量跑 5 小时 ≈ **$0.25**
- Log storage / S3 PUT / Glue API 费用 < $0.01
- **合计一次性约 $0.30**

---

## §7 实施里程碑

| # | 内容 | 工作量 |
|---|---|---|
| M1 | 创建 `lambda/backfill-runner/` 目录 + Dockerfile + requirements + entrypoint + backfill 脚本副本 | 15 分钟 |
| M2 | `scripts/backfill_fund_history.py` 加 S3 checkpoint 支持 | 15 分钟 |
| M3 | CDK 添加 ECS cluster + task def + outputs；`npm run build && cdk diff` 检查 | 15 分钟 |
| M4 | `scripts/run_backfill_fargate.sh` | 5 分钟 |
| M5 | `cdk deploy`，跑 `--limit 10` 烟雾测试 | 10 分钟 |
| M6 | 启动全量回填（后台 Fargate task） | 4-6 小时（被动） |
| M7 | 验证 Iceberg `fund_daily` 覆盖度 | 5 分钟 |

**总开发**：~1 小时 + 6 小时等待。

---

## 附录 — 决策日志

| 决策 | 选择 | 理由 |
|---|---|---|
| 计算层 | AWS Fargate | 用户指定 |
| Docker image | 新建 backfill-runner | fund-fetcher 的 Lambda ENTRYPOINT 不匹配 |
| Checkpoint | S3 progress.json | Fargate 没持久化；S3 同时便于用户查 |
| 触发方式 | aws ecs run-task CLI | 一次性任务不值得专门上 Lambda 触发器 |
| VPC | default VPC public subnet | 已存在，零成本；Fargate 需要 outbound 网络访问 akshare |
| Task 尺寸 | 1 vCPU / 2 GB | 最小有意义规格，4 worker 跑得动，成本最低 |
| Cluster 共享 | 独立 cluster `fund-data-backfill-cluster` | 不混进未来可能的其他 ECS 用途 |
