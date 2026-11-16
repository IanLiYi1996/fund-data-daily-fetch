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
