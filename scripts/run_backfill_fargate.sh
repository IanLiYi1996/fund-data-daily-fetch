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
