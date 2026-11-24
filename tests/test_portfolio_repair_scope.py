"""Validate repair request shapes against the installed AWS service model."""
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.session import get_session
from botocore.validate import validate_parameters


def test_eventbridge_network_is_translated_for_ecs_without_changing_target():
    path = Path(__file__).resolve().parents[1] / "scripts/repair_portfolio_history_20261116.py"
    spec = importlib.util.spec_from_file_location("portfolio_repair_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = {
        "Subnets": ["subnet-example"],
        "SecurityGroups": ["sg-example"],
        "AssignPublicIp": "ENABLED",
    }
    target = {"EcsParameters": {"NetworkConfiguration": {"awsvpcConfiguration": config}}}
    result = module._ecs_network_configuration(target)
    model = get_session().get_service_model("ecs").operation_model("RunTask").input_shape
    validate_parameters(result, model.members["networkConfiguration"])
    assert result == {"awsvpcConfiguration": {
        "subnets": ["subnet-example"], "securityGroups": ["sg-example"],
        "assignPublicIp": "ENABLED",
    }}
    assert set(config) == {"Subnets", "SecurityGroups", "AssignPublicIp"}


@pytest.mark.parametrize("version,allowed", [(64, True), (65, False)])
def test_scheduler_requires_the_exact_approved_task_version(version, allowed):
    path = Path(__file__).resolve().parents[1] / "scripts/repair_portfolio_history_20261116.py"
    spec = importlib.util.spec_from_file_location("portfolio_permission_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    session = MagicMock()
    iam = session.client.return_value
    iam.list_role_policies.return_value = {"PolicyNames": ["test"]}
    target = {"RoleArn": "arn:aws:iam::463470973226:role/test", "Arn": "arn:aws:ecs:us-east-1:463470973226:cluster/fund-data-backfill-cluster"}
    base = "arn:aws:ecs:us-east-1:463470973226:task-definition/FundHistoryBackfill:"
    iam.get_role_policy.return_value = {"PolicyDocument": {"Statement": [{
        "Effect": "Allow", "Action": "ecs:RunTask", "Resource": [base + "63", base + "64"],
        "Condition": {"ArnEquals": {"ecs:cluster": target["Arn"]}},
    }]}}
    if allowed:
        module._assert_scheduler_permission(session, target, base + str(version))
    else:
        with pytest.raises(RuntimeError, match="IAM confirmation"):
            module._assert_scheduler_permission(session, target, base + str(version))


@pytest.mark.parametrize("action,resource,allowed", [
    ("s3:GetObjectVersion",
     "arn:aws:s3:::fund-data-pipeline-463470973226-us-east-1/fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet", True),
    ("s3:GetObject",
     "arn:aws:s3:::fund-data-pipeline-463470973226-us-east-1/fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet", False),
    ("s3:GetObjectVersion",
     "arn:aws:s3:::fund-data-pipeline-463470973226-us-east-1/fund-data-pipeline/other.parquet", False),
    ("s3:GetObjectVersion",
     "arn:aws:s3:::fund-data-pipeline-463470973226-us-east-1/fund-data-pipeline/*", False),
])
def test_export_requires_the_exact_one_file_version_read_grant(action, resource, allowed):
    path = Path(__file__).resolve().parents[1] / "scripts/repair_portfolio_history_20261116.py"
    spec = importlib.util.spec_from_file_location("portfolio_version_permission_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    session = MagicMock()
    iam = session.client.return_value
    iam.list_role_policies.return_value = {"PolicyNames": ["PortfolioConsumerVersionRead"]}
    iam.get_role_policy.return_value = {"PolicyDocument": {"Statement": [{
        "Effect": "Allow", "Action": action, "Resource": resource,
    }]}}
    task = {"taskRoleArn": "arn:aws:iam::463470973226:role/test"}
    if allowed:
        module._assert_consumer_version_read(session, task)
    else:
        with pytest.raises(RuntimeError, match="version read"):
            module._assert_consumer_version_read(session, task)
    iam.put_role_policy.assert_not_called()
