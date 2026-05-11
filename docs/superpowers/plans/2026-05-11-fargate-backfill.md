# Fargate Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-shot AWS Fargate task that runs `backfill_fund_history.py` in the cloud, writes a checkpoint to S3 for resumability, and tears itself down when finished — so the full 26,683-fund akshare history backfill (~4-6h) runs without tying up a developer laptop.

**Architecture:** Move the backfill script into a dedicated `lambda/backfill-runner/` container image (own Dockerfile, because the existing fund-fetcher image uses the Lambda runtime entrypoint). Add ~60 lines to the existing CDK stack: `ecs.Cluster` in the default VPC, a `FargateTaskDefinition` (1 vCPU / 2 GB) that reuses the existing IAM policy statements for S3 + Glue access, and CfnOutputs so a simple `scripts/run_backfill_fargate.sh` can discover the cluster / task-def / subnets and call `aws ecs run-task`. Progress JSON lives at `s3://.../fund-data-pipeline/_backfill/progress.json`.

**Tech Stack:** AWS CDK (TypeScript), AWS Fargate, AWS ECS, Docker, Python 3.11, pyiceberg, akshare, pandas, boto3, AWS CLI, pytest.

**Spec reference:** `docs/superpowers/specs/2026-05-11-fargate-backfill-design.md` (commit `dd4384b`).

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `lambda/backfill-runner/Dockerfile` | Python 3.11 image: pyiceberg + akshare + pandas + shared/; ENTRYPOINT runs the backfill script |
| `lambda/backfill-runner/requirements.txt` | pyiceberg[glue,pyiceberg-core], akshare, pandas, pyarrow, boto3 |
| `lambda/backfill-runner/entrypoint.sh` | Thin shell wrapper: `exec python /app/backfill_fund_history.py "$@"` — so container overrides can append CLI args |
| `lambda/backfill-runner/backfill_fund_history.py` | Moved from `scripts/backfill_fund_history.py`, now also the single source of truth |
| `scripts/backfill_fund_history_local.sh` | Tiny shell wrapper so existing invocations (`uv run python scripts/backfill_fund_history.py ...`) keep working from the repo root |
| `scripts/run_backfill_fargate.sh` | Discovers CFN outputs, calls `aws ecs run-task` on Fargate |
| `tests/test_backfill_s3_checkpoint.py` | Unit tests for the new S3-checkpoint code path in the script (uses moto) |

### Modified files

| Path | Change |
|---|---|
| `cdk/lib/fund-data-fetch-stack.ts` | +60 lines: ECS cluster, Fargate task def, log group, task role grants, CfnOutputs |
| `scripts/backfill_fund_history.py` | **Deleted** (content moved into `lambda/backfill-runner/backfill_fund_history.py`) |

### Out of scope

- Triggering from EventBridge / Step Functions — run-task CLI is sufficient.
- Multi-task parallelism — 4 worker threads in one task handles 26k funds in 4-6h.
- EFS or persistent volume — checkpoint in S3 is enough.

---

## Task 1: Move backfill script to lambda/backfill-runner and add shim

**Files:**
- Create: `lambda/backfill-runner/backfill_fund_history.py` (moved from `scripts/`)
- Create: `scripts/backfill_fund_history_local.sh`
- Delete: `scripts/backfill_fund_history.py`

- [ ] **Step 1: Move the script**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
mkdir -p lambda/backfill-runner
git mv scripts/backfill_fund_history.py lambda/backfill-runner/backfill_fund_history.py
```

- [ ] **Step 2: Update sys.path inside the moved script**

The script has `sys.path.insert(0, "lambda")` near the top. In the Docker image the working dir will be `/app` with `shared/` at `/app/shared/`, and when invoked from the repo root locally the dir `lambda/backfill-runner/../shared` also resolves. Change the path insert to handle both:

Open `lambda/backfill-runner/backfill_fund_history.py`, find the line:

```python
sys.path.insert(0, "lambda")
```

Replace with:

```python
# Allow import of `shared.*` whether we run inside the Docker image
# (where shared/ is at /app/shared/) or from the repo root locally
# (where shared/ is at lambda/shared/).
_here = Path(__file__).resolve().parent
for _candidate in (_here.parent, _here / "..", Path("lambda")):
    _candidate = _candidate.resolve()
    if (_candidate / "shared").is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))
        break
```

(This makes the script self-locating. Path is already imported at the top.)

- [ ] **Step 3: Create the local shim**

`scripts/backfill_fund_history_local.sh`:

```bash
#!/usr/bin/env bash
# Convenience wrapper for running the backfill locally from the repo root.
# All args forwarded to the script.
set -euo pipefail
cd "$(dirname "$0")/.."
exec uv run python lambda/backfill-runner/backfill_fund_history.py "$@"
```

Make it executable:

```bash
chmod +x scripts/backfill_fund_history_local.sh
```

- [ ] **Step 4: Smoke test (dry run) from the repo root**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
./scripts/backfill_fund_history_local.sh --dry-run --limit 3
```

Expected:
```
Loading fund list from s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/fund/2026-05-11/fund_name.parquet
  found 26683 funds
Progress: N already done, M previously failed
Todo: 3 funds

(dry run) first 10 funds to process:
  <code> <name>
  ...
```

(N, M may be non-zero if the earlier smoke run already populated the progress file.)

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill-runner/backfill_fund_history.py scripts/backfill_fund_history_local.sh
git commit -m "refactor: move backfill script into lambda/backfill-runner/

Single source of truth under lambda/ so the Fargate Docker build can
COPY it without reaching across directories. Local callers use the new
scripts/backfill_fund_history_local.sh wrapper.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2: Add S3 checkpoint support to the backfill script

**Files:**
- Modify: `lambda/backfill-runner/backfill_fund_history.py`
- Test: `tests/test_backfill_s3_checkpoint.py`

- [ ] **Step 1: Write failing tests first**

Create `tests/test_backfill_s3_checkpoint.py`:

```python
"""Tests for the S3-checkpoint branch added to backfill_fund_history.Progress."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws


def _load_script_module():
    """Load the backfill script as a module (it's not on a Python package path)."""
    script = Path(__file__).resolve().parent.parent / "lambda" / "backfill-runner" / "backfill_fund_history.py"
    spec = importlib.util.spec_from_file_location("backfill_fund_history", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["backfill_fund_history"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def backfill_mod():
    return _load_script_module()


@mock_aws
def test_progress_load_from_missing_s3_returns_empty(backfill_mod):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="b")
    prog = backfill_mod.Progress.load("s3://b/missing.json")
    assert prog.done == set()
    assert prog.failed == {}


@mock_aws
def test_progress_save_and_reload_via_s3(backfill_mod):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="b")

    prog = backfill_mod.Progress(done={"000001", "000002"},
                                 failed={"999999": "ak timeout"},
                                 started_at="2026-05-11T00:00:00")
    prog.save("s3://b/progress.json")

    # Re-fetch raw and assert JSON shape
    raw = json.loads(s3.get_object(Bucket="b", Key="progress.json")["Body"].read())
    assert set(raw["done"]) == {"000001", "000002"}
    assert raw["failed"] == {"999999": "ak timeout"}
    assert "updated_at" in raw

    # Reload via helper
    prog2 = backfill_mod.Progress.load("s3://b/progress.json")
    assert prog2.done == {"000001", "000002"}
    assert prog2.failed == {"999999": "ak timeout"}


@mock_aws
def test_progress_roundtrip_local_path_unchanged(backfill_mod, tmp_path):
    """Regression: local-file branch still works exactly as before."""
    p = tmp_path / "progress.json"
    prog = backfill_mod.Progress(done={"000001"}, failed={},
                                 started_at="2026-05-11T00:00:00")
    prog.save(p)
    prog2 = backfill_mod.Progress.load(p)
    assert prog2.done == {"000001"}


def test_is_s3_uri_helper(backfill_mod):
    assert backfill_mod._is_s3_uri("s3://bucket/key")
    assert not backfill_mod._is_s3_uri("/tmp/progress.json")
    assert not backfill_mod._is_s3_uri(Path("/tmp/progress.json"))
    assert not backfill_mod._is_s3_uri(None)
```

- [ ] **Step 2: Run the tests; expect failures**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
uv run pytest tests/test_backfill_s3_checkpoint.py -v
```

Expected: collection error or `AttributeError: module 'backfill_fund_history' has no attribute '_is_s3_uri'` (plus 3 test failures for missing S3 branches).

- [ ] **Step 3: Add `_is_s3_uri` / `_s3_parse` helpers to the script**

Open `lambda/backfill-runner/backfill_fund_history.py`. Locate the `DEFAULT_PROGRESS = Path.home() / ".cache" / "fund_backfill_progress.json"` line. **Immediately below that line**, add:

```python
_S3_URI_PREFIX = "s3://"


def _is_s3_uri(value) -> bool:
    """True when value is an 's3://...' string (not a Path, not None)."""
    return isinstance(value, str) and value.startswith(_S3_URI_PREFIX)


def _s3_parse(uri: str) -> tuple[str, str]:
    """Split an s3://bucket/key URI into (bucket, key)."""
    without_scheme = uri[len(_S3_URI_PREFIX):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, key
```

- [ ] **Step 4: Rewrite `Progress.load` / `Progress.save` to branch on S3**

Replace the existing `Progress` class definition (the `@dataclass class Progress` block) with:

```python
@dataclass
class Progress:
    done: set[str]
    failed: dict[str, str]
    started_at: str

    @classmethod
    def load(cls, path) -> "Progress":
        """Load progress from local path OR s3:// URI. Missing target → empty."""
        if _is_s3_uri(path):
            import boto3
            bucket, key = _s3_parse(path)
            s3 = boto3.client("s3")
            try:
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            except s3.exceptions.NoSuchKey:
                return cls(done=set(), failed={},
                           started_at=datetime.now().isoformat())
            raw = json.loads(body)
        else:
            p = Path(path)
            if not p.exists():
                return cls(done=set(), failed={},
                           started_at=datetime.now().isoformat())
            raw = json.loads(p.read_text())
        return cls(
            done=set(raw.get("done", [])),
            failed=dict(raw.get("failed", {})),
            started_at=raw.get("started_at", datetime.now().isoformat()),
        )

    def save(self, path) -> None:
        """Save to local path OR s3:// URI."""
        body = json.dumps({
            "done": sorted(self.done),
            "failed": self.failed,
            "started_at": self.started_at,
            "updated_at": datetime.now().isoformat(),
            "total_done": len(self.done),
            "total_failed": len(self.failed),
        }, ensure_ascii=False, indent=2)

        if _is_s3_uri(path):
            import boto3
            bucket, key = _s3_parse(path)
            boto3.client("s3").put_object(
                Bucket=bucket, Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json; charset=utf-8",
            )
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(body)
```

- [ ] **Step 5: Add `--progress-s3` CLI flag (mutually exclusive with `--progress-file`)**

Still in the same script, locate the `argparse.ArgumentParser` block inside `main()`. Find the line:

```python
    p.add_argument("--progress-file", type=Path, default=DEFAULT_PROGRESS,
                   help="Path to resume checkpoint JSON")
```

Replace that single line with a mutually-exclusive group:

```python
    prog_group = p.add_mutually_exclusive_group()
    prog_group.add_argument("--progress-file", type=Path, default=None,
                            help="Local filesystem checkpoint path")
    prog_group.add_argument("--progress-s3", type=str, default=None,
                            help="s3://bucket/key checkpoint URI (takes "
                                 "precedence over --progress-file)")
```

Then, right after `args = p.parse_args()`, add a resolver:

```python
    progress_target = args.progress_s3 if args.progress_s3 else (
        args.progress_file if args.progress_file else DEFAULT_PROGRESS
    )
```

Finally, replace every reference to `args.progress_file` elsewhere in `main()` with `progress_target`. Expected matches: `args.reset_progress and args.progress_file.exists()`, `Progress.load(args.progress_file)`, `progress.save(args.progress_file)`, and the final `print(f"Progress file: {args.progress_file}")`.

Updated versions (paste-replace each one):

```python
    if args.reset_progress and not _is_s3_uri(progress_target) and Path(progress_target).exists():
        Path(progress_target).unlink()
        print(f"Deleted progress file: {progress_target}")
```

```python
    progress = Progress.load(progress_target)
```

```python
                progress.save(progress_target)
```

```python
    progress.save(progress_target)
```

```python
    print(f"Progress target: {progress_target}")
```

- [ ] **Step 6: Run the tests again; expect them green**

```bash
uv run pytest tests/test_backfill_s3_checkpoint.py -v
```

Expected: 4 passed. Also run the full suite to confirm no regression:

```bash
uv run pytest -v 2>&1 | tail -3
```

Expected: `70 passed` (66 prior + 4 new).

- [ ] **Step 7: Commit**

```bash
git add lambda/backfill-runner/backfill_fund_history.py tests/test_backfill_s3_checkpoint.py
git commit -m "feat(backfill): support s3:// checkpoint URIs for Fargate runs

Local filesystem is ephemeral in Fargate. Added --progress-s3 flag (mutex
with --progress-file) that routes Progress.load/save through boto3. Helper
functions _is_s3_uri and _s3_parse added. Full regression passes (70/70).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3: Create the backfill-runner Docker image

**Files:**
- Create: `lambda/backfill-runner/Dockerfile`
- Create: `lambda/backfill-runner/requirements.txt`
- Create: `lambda/backfill-runner/entrypoint.sh`

- [ ] **Step 1: Write `requirements.txt`**

Create `lambda/backfill-runner/requirements.txt`:

```
akshare>=1.14.0
pyiceberg[glue,pyiceberg-core]>=0.7.0
pandas>=2.0.0
pyarrow>=14.0.0
boto3>=1.34.0
requests>=2.31.0
lxml>=5.0.0
html5lib>=1.1
beautifulsoup4>=4.12.0
```

(The non-obvious ones — `requests / lxml / html5lib / beautifulsoup4` — are akshare's transitive deps; the fund-fetcher image pins them so we match.)

- [ ] **Step 2: Write `entrypoint.sh`**

Create `lambda/backfill-runner/entrypoint.sh`:

```bash
#!/bin/sh
exec python /app/backfill_fund_history.py "$@"
```

- [ ] **Step 3: Write `Dockerfile`**

Create `lambda/backfill-runner/Dockerfile`:

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY backfill-runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# shared/ holds schemas, storage, utils — same package layout the
# fund-fetcher Lambda uses, imported via PYTHONPATH=/app.
COPY shared/ /app/shared/
COPY backfill-runner/entrypoint.sh /app/entrypoint.sh
COPY backfill-runner/backfill_fund_history.py /app/backfill_fund_history.py

RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
```

- [ ] **Step 4: Build locally and run a --dry-run inside the container**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg/lambda
docker build -f backfill-runner/Dockerfile -t backfill-runner:test .
docker run --rm --entrypoint python backfill-runner:test \
    /app/backfill_fund_history.py --help
```

Expected: `usage: backfill_fund_history.py [-h] ...` prints with `--progress-s3` listed in the options.

- [ ] **Step 5: Verify --dry-run works end-to-end inside the image (needs AWS creds)**

```bash
# Export AWS creds so the container can reach S3
docker run --rm \
    -e AWS_REGION=us-east-1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    backfill-runner:test --dry-run --limit 3
```

Expected: prints `found 26683 funds` and `(dry run) first 10 funds to process:` followed by 3 fund codes.

(If you don't have AWS creds handy at build time, skip Step 5 — the CDK deploy in Task 4 will exercise the image. Document skipping in the commit message.)

- [ ] **Step 6: Commit**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
git add lambda/backfill-runner/Dockerfile lambda/backfill-runner/requirements.txt lambda/backfill-runner/entrypoint.sh
git commit -m "feat(backfill-runner): Dockerfile + requirements + entrypoint

Python 3.11 slim image with pyiceberg + akshare + pandas + shared/.
PYTHONPATH=/app, entrypoint is a thin shell wrapper so 'aws ecs run-task'
containerOverrides.command can pass extra CLI args to the script.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4: CDK — add ECS cluster + Fargate task definition + outputs

**Files:**
- Modify: `cdk/lib/fund-data-fetch-stack.ts`

- [ ] **Step 1: Add imports at the top**

Open `cdk/lib/fund-data-fetch-stack.ts`. Right under the existing `import * as events from "aws-cdk-lib/aws-events";` block (lines 12-13 area), add:

```typescript
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
```

- [ ] **Step 2: Add the ECS cluster, image asset, log group, and task def**

Locate the end of the existing constructor body — just before the final `}` that closes `constructor(scope, id, props)`. The last existing line is likely a `CfnOutput` (for Glue DB). **Immediately before that closing `}`** (but after all the CfnOutputs), insert:

```typescript
    // ========== Fargate: one-shot history backfill ==========

    const backfillVpc = ec2.Vpc.fromLookup(this, "BackfillDefaultVpc", {
      isDefault: true,
    });

    const backfillCluster = new ecs.Cluster(this, "BackfillCluster", {
      clusterName: "fund-data-backfill-cluster",
      vpc: backfillVpc,
      containerInsights: false,
    });

    const backfillImage = new ecrAssets.DockerImageAsset(
      this,
      "BackfillImage",
      {
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
      this,
      "BackfillTaskDef",
      {
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

    // Reuse the same S3 + Glue policy statements as the Lambda fetchers.
    [listBucketPolicy, objectPolicy, icebergGluePolicy].forEach((stmt) =>
      backfillTaskDef.taskRole.addToPrincipalPolicy(stmt)
    );
    backfillTaskDef.node.addDependency(glueDatabase);

    new CfnOutput(this, "BackfillClusterName", {
      value: backfillCluster.clusterName,
      description: "ECS cluster for the one-shot backfill task",
      exportName: "FundDataBackfillClusterName",
    });

    new CfnOutput(this, "BackfillTaskDefArn", {
      value: backfillTaskDef.taskDefinitionArn,
      description: "Fargate task definition for the backfill container",
      exportName: "FundDataBackfillTaskDefArn",
    });

    new CfnOutput(this, "BackfillSubnetIds", {
      value: backfillVpc.publicSubnets.map((s) => s.subnetId).join(","),
      description: "Public subnets to run the Fargate task in",
      exportName: "FundDataBackfillSubnetIds",
    });
```

- [ ] **Step 3: Compile the TS**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg/cdk
npm run build
```

Expected: exits 0 with no output (tsc compiles clean).

- [ ] **Step 4: Synth + diff (preview only, no deploy)**

```bash
CDK_DEFAULT_ACCOUNT=463470973226 CDK_DEFAULT_REGION=us-east-1 \
    npx cdk diff FundDataFetchStack 2>&1 | tail -30
```

Expected highlights in the output:
- `[+] AWS::ECS::Cluster BackfillCluster`
- `[+] AWS::ECS::TaskDefinition BackfillTaskDef`
- `[+] AWS::IAM::Role BackfillTaskDef/TaskRole`
- `[+] AWS::Logs::LogGroup BackfillLogs`
- `[+] Output BackfillClusterName / BackfillTaskDefArn / BackfillSubnetIds`

No diffs on existing resources (fund fetchers etc.).

- [ ] **Step 5: Commit**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
git add cdk/lib/fund-data-fetch-stack.ts
git commit -m "feat(cdk): ECS cluster + Fargate task def for history backfill

Adds a one-shot task (1 vCPU / 2 GiB) in the default VPC. Reuses the
existing listBucketPolicy / objectPolicy / icebergGluePolicy so the
task can write fund-data-pipeline/* and manage the fund_data_lake Glue
catalog. Outputs the cluster name, task-def ARN, and subnet ids so the
run_backfill_fargate.sh script can discover them.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 5: run-task shell script

**Files:**
- Create: `scripts/run_backfill_fargate.sh`

- [ ] **Step 1: Verify jq is installed on the dev machine**

```bash
jq --version
```

Expected: `jq-1.x.x`. If not installed: `sudo yum install -y jq` (Amazon Linux) or `brew install jq` (macOS).

- [ ] **Step 2: Create `scripts/run_backfill_fargate.sh`**

```bash
#!/usr/bin/env bash
# Launch the one-shot fund history backfill on AWS Fargate.
#
# Usage:
#   ./scripts/run_backfill_fargate.sh              # full run (~4-6h, ~$0.30)
#   LIMIT=10 ./scripts/run_backfill_fargate.sh     # smoke test: first 10 funds
#
# Environment overrides:
#   STACK   CloudFormation stack name (default: FundDataFetchStack)
#   REGION  AWS region (default: us-east-1)
#   LIMIT   If set, appends --limit N to the container command
set -euo pipefail

STACK="${STACK:-FundDataFetchStack}"
REGION="${REGION:-us-east-1}"

get_out() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text
}

CLUSTER=$(get_out BackfillClusterName)
TASK_DEF=$(get_out BackfillTaskDefArn)
SUBNETS=$(get_out BackfillSubnetIds)

if [ -z "$CLUSTER" ] || [ -z "$TASK_DEF" ] || [ -z "$SUBNETS" ]; then
    echo "ERROR: could not read CloudFormation outputs from stack '$STACK' in '$REGION'."
    echo "  Is the stack deployed? Does it have BackfillClusterName / BackfillTaskDefArn / BackfillSubnetIds outputs?"
    exit 1
fi

SUBNET_JSON=$(echo "$SUBNETS" | tr ',' '\n' | jq -R . | jq -sc .)
NETWORK="awsvpcConfiguration={subnets=$SUBNET_JSON,assignPublicIp=ENABLED}"

RUN_ARGS=(
  aws ecs run-task
    --cluster "$CLUSTER"
    --task-definition "$TASK_DEF"
    --launch-type FARGATE
    --network-configuration "$NETWORK"
    --region "$REGION"
)

if [ -n "${LIMIT:-}" ]; then
    # Append --limit N to the container's default command via override.
    OVERRIDE=$(jq -nc --arg lim "$LIMIT" '
      {containerOverrides: [{name: "backfill",
        command: ["--progress-s3",
                  "s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json",
                  "--limit", $lim]}]}')
    RUN_ARGS+=(--overrides "$OVERRIDE")
fi

TASK_ARN=$("${RUN_ARGS[@]}" --query "tasks[0].taskArn" --output text)

cat <<EOF
Started Fargate task:
  $TASK_ARN

Watch logs (follow mode):
  aws logs tail /aws/ecs/fund-history-backfill --follow --region $REGION

Check task status:
  aws ecs describe-tasks --cluster $CLUSTER --tasks $TASK_ARN \\
      --region $REGION --query 'tasks[0].{status:lastStatus,reason:stoppedReason}'

Progress file:
  aws s3 cp s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json - \\
      | jq '{done: (.done|length), failed: (.failed|length), updated: .updated_at}'
EOF
```

Make it executable:

```bash
chmod +x scripts/run_backfill_fargate.sh
```

- [ ] **Step 3: Commit**

```bash
git add scripts/run_backfill_fargate.sh
git commit -m "feat(scripts): run_backfill_fargate.sh to launch one-shot Fargate task

Discovers cluster name / task-def / subnets from CloudFormation outputs,
assembles the run-task command, prints log-tail / status / progress-file
commands. LIMIT env var enables smoke testing.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 6: Deploy CDK stack with the new Fargate resources

**Files:** none modified — pure deploy.

- [ ] **Step 1: Deploy**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg/cdk
CDK_DEFAULT_ACCOUNT=463470973226 CDK_DEFAULT_REGION=us-east-1 \
    npx cdk deploy FundDataFetchStack --require-approval never 2>&1 | tail -20
```

Expected tail:
```
 ✅  FundDataFetchStack

✨  Deployment time: <N>s

Outputs:
FundDataFetchStack.BackfillClusterName = fund-data-backfill-cluster
FundDataFetchStack.BackfillTaskDefArn = arn:aws:ecs:us-east-1:...:task-definition/FundHistoryBackfill:1
FundDataFetchStack.BackfillSubnetIds = subnet-aaa,subnet-bbb,subnet-ccc,subnet-ddd,subnet-eee,subnet-fff
... (other existing outputs)
```

If Docker build fails (no Docker daemon, etc.), abort this task, report BLOCKED with the error, and do NOT commit any local state changes.

- [ ] **Step 2: Smoke test — run a --limit 10 task**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
LIMIT=10 ./scripts/run_backfill_fargate.sh
```

Expected: prints a task ARN and follow-up commands.

- [ ] **Step 3: Tail the log until task ends**

Copy the `aws logs tail ...` command the script printed and run it. Wait until the task exits (1-2 minutes).

Expected final log lines:
```
=== Done ===
Processed: <N> funds with data
Empty:     <M>
Failed:    <K>
Elapsed:   <N>s
Progress target: s3://.../fund-data-pipeline/_backfill/progress.json
```

Where N+M = 10 (or fewer if some funds were already in progress.json from earlier local smoke tests).

- [ ] **Step 4: Verify data landed in Iceberg**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
uv run python <<'PY'
import duckdb
con = duckdb.connect()
con.sql("INSTALL iceberg; LOAD iceberg;")
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
con.sql("SET unsafe_enable_version_guessing = true;")
path = "s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/iceberg/fund_data_lake.db/fund_daily"
res = con.sql(f"""
  SELECT MIN(trade_date) AS earliest, MAX(trade_date) AS latest,
         COUNT(DISTINCT fund_code) AS funds, COUNT(*) AS rows
  FROM iceberg_scan('{path}')
""").fetchone()
print(f"earliest={res[0]} latest={res[1]} funds={res[2]} rows={res[3]}")
PY
```

Expected: `earliest` is an early-2000s date (e.g. 2001-12-18), `funds >= 2` (from earlier smoke runs plus the 10 new ones; some may overlap).

- [ ] **Step 5: Verify S3 progress file was written**

```bash
aws s3 cp s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json - \
    | jq '{done: (.done|length), failed: (.failed|length), updated: .updated_at}'
```

Expected: `done >= 10`, `failed` should be 0.

- [ ] **Step 6: Commit a deployment note**

There's nothing to commit if the CDK deploy succeeded purely from source state, but capture the outputs into a notes file so the user can see them historically:

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
aws cloudformation describe-stacks --stack-name FundDataFetchStack \
    --query "Stacks[0].Outputs" --region us-east-1 > /tmp/stack-outputs.json
```

No commit needed — this task is operational, not code.

---

## Task 7: Full-history backfill run (operational)

This task is operational: no code changes, just a long-running job kick-off.

- [ ] **Step 1: Confirm smoke test Iceberg coverage looks right**

From Task 6 Step 4, the Iceberg table should have at least 10 funds' full history. If not, go debug before starting the full run.

- [ ] **Step 2: Start the full run**

```bash
cd /home/ec2-user/research/fund-data-daily-fetch-iceberg
./scripts/run_backfill_fargate.sh
```

(No `LIMIT` this time — full 26,683 funds.)

Save the task ARN printed by the script.

- [ ] **Step 3: Monitor (periodic, not in a tight loop)**

At any time:

```bash
aws s3 cp s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json - \
    | jq '{done: (.done|length), failed: (.failed|length), updated: .updated_at}'
```

Or tail the live log:

```bash
aws logs tail /aws/ecs/fund-history-backfill --follow --region us-east-1
```

Expected cadence: every batch (100 funds) produces a log line around the `flushing X funds, Y rows → fund_daily...` pattern. Should see about 1 batch per 1-2 minutes.

- [ ] **Step 4: When task ends, verify final state**

Check `lastStatus`:

```bash
aws ecs describe-tasks --cluster fund-data-backfill-cluster \
    --tasks <TASK_ARN> --region us-east-1 \
    --query 'tasks[0].{status:lastStatus,reason:stoppedReason,exitCode:containers[0].exitCode}'
```

Expected: `status=STOPPED, exitCode=0`.

Check final progress counts:

```bash
aws s3 cp s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json - \
    | jq '{done: (.done|length), failed_count: (.failed|length), failed: .failed}'
```

Expected: `done` close to 26,683 (give or take funds akshare couldn't serve). A small `failed_count` (< 100) is acceptable.

- [ ] **Step 5: Run the Iceberg coverage query**

```bash
uv run python <<'PY'
import duckdb
con = duckdb.connect()
con.sql("INSTALL iceberg; LOAD iceberg;")
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
con.sql("SET unsafe_enable_version_guessing = true;")
path = "s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/iceberg/fund_data_lake.db/fund_daily"
con.sql(f"""
  SELECT MIN(trade_date) AS earliest, MAX(trade_date) AS latest,
         COUNT(DISTINCT fund_code) AS funds,
         COUNT(*) AS rows,
         COUNT(*) FILTER (WHERE unit_nav IS NOT NULL) AS rows_with_nav
  FROM iceberg_scan('{path}')
""").show()
PY
```

Expected: `funds >= 20000`, `earliest <= 2001-12-18`, `rows` in the hundreds of millions.

- [ ] **Step 6: If there were failures, retry them**

The progress file lists failed codes under `.failed`. To retry them, reset the `failed` dict and relaunch with a code list:

```bash
# Get the list of failed codes
aws s3 cp s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json - \
    | jq -r '.failed | keys | join(",")' > /tmp/retry_codes.txt

# (Optional) Clear the failed dict in the progress file so they're re-attempted
# — but progress.done still filters them out since they succeeded earlier? NO,
# they never succeeded (that's why they're in failed). They just won't be in done either.

# Relaunch with explicit codes via override
aws ecs run-task \
    --cluster fund-data-backfill-cluster \
    --task-definition $(aws cloudformation describe-stacks --stack-name FundDataFetchStack \
        --query "Stacks[0].Outputs[?OutputKey=='BackfillTaskDefArn'].OutputValue" --output text) \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$(aws cloudformation describe-stacks --stack-name FundDataFetchStack --query \"Stacks[0].Outputs[?OutputKey=='BackfillSubnetIds'].OutputValue\" --output text | tr ',' '\n' | head -1)],assignPublicIp=ENABLED}" \
    --overrides "{\"containerOverrides\":[{\"name\":\"backfill\",\"command\":[\"--progress-s3\",\"s3://fsi-investmentadvisory-data-463470973226-us-east-1/fund-data-pipeline/_backfill/progress.json\",\"--codes\",\"$(cat /tmp/retry_codes.txt)\"]}]}" \
    --region us-east-1
```

If you prefer skipping the manual retry, the failed dict is informational — the fund daily EventBridge job will eventually pull these funds' new daily rows naturally.

No commit needed for operational steps.

---

## Self-Review

I checked this plan against the spec (`docs/superpowers/specs/2026-05-11-fargate-backfill-design.md`):

- **§1 Architecture** — Tasks 3+4 build the Fargate task running the image with proper IAM/VPC/log wiring. ✓
- **§2(a) Dockerfile** — Task 3 creates it literally. ✓
- **§2(b) S3 checkpoint** — Task 2 adds `--progress-s3` with mutex against `--progress-file`, S3 load/save paths, moto-backed tests. ✓
- **§2(c) CDK** — Task 4 adds ECS cluster + FargateTaskDefinition + LogGroup + 3 outputs + policy reuse. ✓
- **§2(d) run-task shell** — Task 5 creates `scripts/run_backfill_fargate.sh` with jq-backed subnet JSON. ✓
- **§3 Code changes** — All references to existing variables (`bucketName`, `s3Prefix`, `lambdaDir`, `listBucketPolicy`, `objectPolicy`, `icebergGluePolicy`, `glueDatabase`) were verified present in the current CDK stack before being referenced. ✓
- **§4 Error handling** — Existing script try/except behavior preserved; Tasks 6/7 give operational responses to Fargate task failures. ✓
- **§5 Testing** — Task 3 Step 4/5 cover Docker build test; Task 2 covers unit tests; Task 6 covers deploy + `LIMIT=10` smoke; Task 7 Step 4-5 cover full-run verification. ✓
- **§6 Cost** — Operational, not implementable; ~$0.30 noted in Task 7 via the script's header comment. ✓
- **§7 Milestones** — Plan's 7 tasks map 1:1 to spec's M1-M7. ✓

Placeholder / TODO scan: no `TBD`, `XXX`, `handle edge cases`, etc. in the plan text.

Type consistency: `_is_s3_uri`, `_s3_parse`, `Progress.load`, `Progress.save`, `progress_target`, `BackfillClusterName`, `BackfillTaskDefArn`, `BackfillSubnetIds` appear consistently across tasks. The script move (scripts/ → lambda/backfill-runner/) is reflected in every later path reference.

Known gap: the CDK stack-level `grantReadWrite` for the `_backfill/progress.json` key is already covered by `objectPolicy` (which grants `fund-data-pipeline/*`). Confirmed. ✓
