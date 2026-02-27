#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CDK_DIR="$PROJECT_ROOT/cdk"

echo "=========================================="
echo "Fund Data Daily Fetch - Destroy Script"
echo "=========================================="

# Check AWS credentials
echo "Checking AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    echo "Error: AWS credentials not configured"
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=${AWS_REGION:-$(aws configure get region)}

echo "AWS Account: $AWS_ACCOUNT"
echo "AWS Region: $AWS_REGION"

# Warning
echo ""
echo "WARNING: This will destroy all resources in the FundDataFetchStack."
echo "NOTE: The S3 bucket will be retained (RemovalPolicy.RETAIN)"
echo ""
read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Change to CDK directory
cd "$CDK_DIR"

# Destroy stack
echo ""
echo "Destroying stack..."
npx cdk destroy --force

echo ""
echo "=========================================="
echo "Stack destroyed successfully!"
echo "=========================================="

echo ""
echo "NOTE: The S3 bucket 'fund-data-$AWS_ACCOUNT-$AWS_REGION' was retained."
echo "To delete the bucket manually:"
echo "  aws s3 rb s3://fund-data-$AWS_ACCOUNT-$AWS_REGION --force"
