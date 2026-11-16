"""Guarded portfolio repair deployment. Default is a read-only plan.

Only the portfolio table/file, backfill task definition, and existing quarterly
rule are in scope. No IAM writes, Lambda invocations, or NAV table changes.
"""
from __future__ import annotations

import argparse
import io
import json
from datetime import date
from urllib.parse import urlparse

import boto3

ACCOUNT = "463470973226"
REGION = "us-east-1"
BUCKET = f"fund-data-pipeline-{ACCOUNT}-{REGION}"
PREFIX = "fund-data-pipeline/"
KEY = PREFIX + "fund/_history/fund_portfolio_hold_history.parquet"
RULE = "FundPortfolioHoldQuarterlySchedule"
CLUSTER = "fund-data-backfill-cluster"
REPAIR = PREFIX + "_repairs/2026-11-16-portfolio-history/"
REPOSITORY = f"cdk-hnb659fds-container-assets-{ACCOUNT}-{REGION}"
UNFINISHED_ETFS = {
    "158036", "158043", "158052", "158055", "158062",
    "158066", "158067", "158072", "159047",
}


def _ecs_network_configuration(target):
    """EventBridge and ECS use different casing inside awsvpcConfiguration."""
    config = target["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
    return {"awsvpcConfiguration": {
        "subnets": config["Subnets"],
        "securityGroups": config.get("SecurityGroups", []),
        "assignPublicIp": config.get("AssignPublicIp", "DISABLED"),
    }}


def _assert_scheduler_permission(session, target, task_definition):
    # IAM writes require a separate, explicitly confirmed operation. This
    # script only verifies the grant before changing a live schedule.
    role = target["RoleArn"].rsplit("/", 1)[1]
    iam = session.client("iam")
    for name in iam.list_role_policies(RoleName=role)["PolicyNames"]:
        document = iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
        for statement in document["Statement"]:
            actions = statement.get("Action", [])
            resources = statement.get("Resource", [])
            if isinstance(actions, str):
                actions = [actions]
            if isinstance(resources, str):
                resources = [resources]
            if (
                statement.get("Effect") == "Allow"
                and "ecs:RunTask" in actions
                and task_definition in resources
                and statement.get("Condition") == {"ArnEquals": {"ecs:cluster": target["Arn"]}}
            ):
                return
    raise RuntimeError("scheduler lacks the explicitly scoped task grant; IAM confirmation required")


def _assert_consumer_version_read(session, task):
    """Fail before launching an export if its pinned baseline is unreadable."""
    role = task["taskRoleArn"].rsplit("/", 1)[1]
    iam = session.client("iam")
    resource = f"arn:aws:s3:::{BUCKET}/{KEY}"
    for name in iam.list_role_policies(RoleName=role)["PolicyNames"]:
        document = iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
        for statement in document["Statement"]:
            actions = statement.get("Action", [])
            resources = statement.get("Resource", [])
            if isinstance(actions, str):
                actions = [actions]
            if isinstance(resources, str):
                resources = [resources]
            if (
                statement.get("Effect") == "Allow"
                and actions == ["s3:GetObjectVersion"]
                and resources == [resource]
                and not statement.get("Condition")
            ):
                return
    raise RuntimeError("task lacks the approved one-file version read grant; IAM confirmation required")


def _quarantine_unfinished_quarter(session, state):
    """Isolate exactly the 90 legacy ETF rows stamped with an unfinished Q4."""
    import pyarrow.parquet as pq
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import EqualTo, GreaterThan

    unfinished = date(2026, 12, 31)
    if unfinished <= date.today():
        raise RuntimeError("incident-specific unfinished-quarter quarantine no longer applies")
    credentials = session.get_credentials().get_frozen_credentials()
    catalog = load_catalog("repair", type="glue", **{
        "glue.region": REGION, "s3.region": REGION,
        "s3.access-key-id": credentials.access_key,
        "s3.secret-access-key": credentials.secret_key,
        "s3.session-token": credentials.token,
    })
    table = catalog.load_table(("fund_data_lake", "fund_portfolio_hold"))
    before = table.current_snapshot()
    if before.snapshot_id != state["snapshot"]:
        raise RuntimeError("portfolio snapshot changed before quarantine")
    future = table.scan(row_filter=GreaterThan("report_date", date.today())).to_arrow()
    dates = set(future.column("report_date").to_pylist())
    codes = set(future.column("fund_code").to_pylist())
    if len(future) != 90 or dates != {unfinished} or codes != UNFINISHED_ETFS:
        raise RuntimeError("unfinished-quarter rows differ from the inspected 90-row scope")
    s3 = session.client("s3")
    body = io.BytesIO()
    pq.write_table(future, body, compression="snappy")
    backup = REPAIR + "3722774192629459938/unfinished-quarter-before.parquet"
    s3.put_object(
        Bucket=BUCKET, Key=backup, Body=body.getvalue(),
        Metadata={"reason": "unfinished-quarter", "report-date": unfinished.isoformat(),
                  "source-snapshot": str(before.snapshot_id)},
    )
    table.refresh()
    if table.current_snapshot().snapshot_id != before.snapshot_id:
        raise RuntimeError("portfolio snapshot changed after quarantine backup")
    before_count = int(before.summary.model_dump()["total-records"])
    table.delete(EqualTo("report_date", unfinished))
    table.refresh()
    after = table.current_snapshot()
    after_count = int(after.summary.model_dump()["total-records"])
    if before_count - after_count != 90:
        raise RuntimeError("quarantine affected an unexpected number of records; inspect backup")
    if len(table.scan(row_filter=GreaterThan("report_date", date.today())).to_arrow()):
        raise RuntimeError("unfinished-quarter records remain")
    receipt = {
        "before_snapshot": before.snapshot_id, "after_snapshot": after.snapshot_id,
        "rows_isolated": 90, "report_date": unfinished.isoformat(),
        "backup_key": backup, "other_record_count_unchanged": True,
    }
    s3.put_object(
        Bucket=BUCKET, Key=REPAIR + "3722774192629459938/quarantine-verified.json",
        Body=json.dumps(receipt).encode(), ContentType="application/json",
    )
    print("QUARANTINE_VERIFIED", json.dumps(receipt), flush=True)


def inspect_state(session):
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("wrong AWS account")
    s3 = session.client("s3")
    glue = session.client("glue").get_table(
        DatabaseName="fund_data_lake", Name="fund_portfolio_hold",
    )["Table"]
    location = glue["Parameters"]["metadata_location"]
    uri = urlparse(location)
    metadata = json.loads(s3.get_object(
        Bucket=uri.netloc, Key=uri.path.lstrip("/"),
    )["Body"].read())
    head = s3.head_object(Bucket=BUCKET, Key=KEY)
    events = session.client("events")
    rule = events.describe_rule(Name=RULE)
    targets = events.list_targets_by_rule(Rule=RULE)["Targets"]
    if len(targets) != 1:
        raise RuntimeError("unexpected portfolio target count")
    target = targets[0]
    task = session.client("ecs").describe_task_definition(
        taskDefinition=target["EcsParameters"]["TaskDefinitionArn"],
    )["taskDefinition"]
    return {
        "snapshot": metadata["current-snapshot-id"],
        "metadata_location": location,
        "consumer_version": head["VersionId"],
        "consumer_etag": head["ETag"],
        "rule": {k: rule[k] for k in ("Name", "ScheduleExpression", "State", "Description")},
        "target": target, "task_definition": task,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--phase", choices=("prepare", "canary", "full", "retry", "refresh-legacy", "export", "deploy", "quarantine-future"), default="prepare")
    parser.add_argument("--image", help="Immutable ECR image URI ending in @sha256:...")
    parser.add_argument("--task-definition")
    parser.add_argument("--expected-snapshot", type=int)
    parser.add_argument("--expected-version")
    parser.add_argument("--failure-plan", help="Audited S3 code/error map, required for retry")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.workers <= 4:
        parser.error("incident repair uses 1..4 workers")
    if (args.phase in ("retry", "refresh-legacy")) != bool(args.failure_plan):
        parser.error("failure-plan is required for retry/refresh-legacy and not accepted for other phases")
    if args.failure_plan and not args.failure_plan.startswith(f"s3://{BUCKET}/{REPAIR}"):
        parser.error("failure-plan must be inside this incident's private repair prefix")
    session = boto3.Session(region_name=REGION)
    state = inspect_state(session)
    print(json.dumps({
        "account": ACCOUNT, "region": REGION, "phase": args.phase,
        "snapshot": state["snapshot"], "consumer_version": state["consumer_version"],
        "schedule": state["rule"]["ScheduleExpression"],
        "task_definition": state["task_definition"]["taskDefinitionArn"],
        "apply": args.apply,
    }, indent=2), flush=True)
    if not args.apply:
        return
    if args.expected_snapshot != state["snapshot"]:
        raise RuntimeError("snapshot changed or --expected-snapshot missing")
    if args.expected_version != state["consumer_version"]:
        raise RuntimeError("consumer version changed or --expected-version missing")
    ecs = session.client("ecs")
    if ecs.list_tasks(cluster=CLUSTER, desiredStatus="RUNNING")["taskArns"]:
        raise RuntimeError("backfill cluster has running tasks; refusing concurrent repair")
    s3 = session.client("s3")
    if args.phase == "quarantine-future":
        _quarantine_unfinished_quarter(session, state)
        return
    if args.phase == "prepare":
        if not args.image or not args.image.startswith(
            f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{REPOSITORY}@sha256:"
        ):
            raise RuntimeError("prepare needs an immutable image in the existing ECR repository")
        digest = args.image.rsplit("@", 1)[1]
        images = session.client("ecr").batch_get_image(
            repositoryName=REPOSITORY, imageIds=[{"imageDigest": digest}],
        )["images"]
        if len(images) != 1:
            raise RuntimeError("repair image not found")
        backup = REPAIR + str(state["snapshot"]) + "/"
        s3.copy_object(
            Bucket=BUCKET, Key=backup + "consumer-before.parquet",
            CopySource={"Bucket": BUCKET, "Key": KEY, "VersionId": state["consumer_version"]},
        )
        uri = urlparse(state["metadata_location"])
        s3.copy_object(
            Bucket=BUCKET, Key=backup + "iceberg-metadata-before.json",
            CopySource={"Bucket": uri.netloc, "Key": uri.path.lstrip("/")},
        )
        s3.put_object(
            Bucket=BUCKET, Key=backup + "before.json",
            Body=json.dumps(state, default=str).encode(), ContentType="application/json",
        )
        task = state["task_definition"]
        allowed = set(ecs.meta.service_model.operation_model(
            "RegisterTaskDefinition"
        ).input_shape.members)
        registration = {k: v for k, v in task.items() if k in allowed}
        containers = registration["containerDefinitions"]
        if len(containers) != 1 or containers[0]["name"] != "backfill":
            raise RuntimeError("unexpected backfill container")
        containers[0]["image"] = args.image
        result = ecs.register_task_definition(**registration)["taskDefinition"]
        arn = result["taskDefinitionArn"]
        s3.put_object(
            Bucket=BUCKET, Key=backup + "repair-task.json",
            Body=json.dumps({"task_definition": arn, "image": args.image}).encode(),
        )
        print("REGISTERED_TASK_DEFINITION", arn, flush=True)
        print("BACKUP", f"s3://{BUCKET}/{backup}", flush=True)
        return
    if not args.task_definition or not args.task_definition.startswith(
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/FundHistoryBackfill:"
    ):
        raise RuntimeError("repair task definition missing or outside scope")
    task = ecs.describe_task_definition(
        taskDefinition=args.task_definition,
    )["taskDefinition"]
    if task["containerDefinitions"][0]["image"] != args.image:
        raise RuntimeError("repair task definition/image mismatch")
    if args.phase in ("full", "export", "deploy"):
        _assert_consumer_version_read(session, task)
    if args.phase == "deploy":
        events = session.client("events")
        rule = state["rule"].copy()
        rule["ScheduleExpression"] = "cron(0 20 5 2,5,8,11 ? *)"
        rule["Description"] = "Quarterly refresh of fund_portfolio_hold (5th of May/Aug/Nov/Feb at 20:00 UTC)"
        target = state["target"]
        _assert_scheduler_permission(session, target, args.task_definition)
        target["EcsParameters"]["TaskDefinitionArn"] = args.task_definition
        target["Input"] = json.dumps({"containerOverrides": [{
            "name": "backfill", "command": ["--mode", "portfolio", "--refresh-existing"],
        }]})
        events.put_rule(**rule)
        result = events.put_targets(Rule=RULE, Targets=[target])
        if result["FailedEntryCount"]:
            raise RuntimeError(f"portfolio target update failed: {result}")
        print("DEPLOYED", args.task_definition, rule["ScheduleExpression"], flush=True)
        return
    command = ["--mode", "portfolio", "--target-quarter", "2026-Q3", "--years", "2026"]
    if args.phase == "canary":
        command += ["--limit", "100", "--workers", str(args.workers), "--batch", "100", "--no-export"]
    elif args.phase == "full":
        command += ["--workers", str(args.workers), "--batch", "200"]
    elif args.phase in ("retry", "refresh-legacy"):
        command += ["--workers", str(args.workers), "--batch", "100",
                    "--failure-plan", args.failure_plan, "--no-export"]
        if args.phase == "refresh-legacy":
            command += ["--refresh-existing"]
    else:
        command += ["--export-only"]
    target = state["target"]
    result = ecs.run_task(
        cluster=CLUSTER, taskDefinition=args.task_definition, launchType="FARGATE",
        networkConfiguration=_ecs_network_configuration(target),
        overrides={"containerOverrides": [{"name": "backfill", "command": command}]},
        startedBy="portfolio-repair-20261116",
    )
    if result["failures"] or len(result["tasks"]) != 1:
        raise RuntimeError("repair ECS task did not start")
    print("STARTED", result["tasks"][0]["taskArn"], flush=True)


if __name__ == "__main__":
    main()
