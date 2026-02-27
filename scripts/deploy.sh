#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CDK_DIR="$PROJECT_ROOT/cdk"

echo "=========================================="
echo "Fund Data Daily Fetch - Deployment Script"
echo "=========================================="

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v node &> /dev/null; then
    echo "Error: Node.js is not installed"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "Error: npm is not installed"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed"
    exit 1
fi

if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

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

# Install CDK dependencies
echo ""
echo "Installing CDK dependencies..."
cd "$CDK_DIR"
npm install

# Bootstrap CDK if needed
echo ""
echo "Checking CDK bootstrap..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit &> /dev/null; then
    echo "Bootstrapping CDK..."
    npx cdk bootstrap aws://$AWS_ACCOUNT/$AWS_REGION
else
    echo "CDK already bootstrapped"
fi

# Synthesize CDK stack
echo ""
echo "Synthesizing CDK stack..."
npx cdk synth

# Deploy
echo ""
echo "Deploying stack..."
npx cdk deploy --require-approval never

echo ""
echo "=========================================="
echo "Deployment completed successfully!"
echo "=========================================="

# Show outputs
echo ""
echo "Stack Outputs:"
aws cloudformation describe-stacks \
    --stack-name FundDataFetchStack \
    --query "Stacks[0].Outputs[*].[OutputKey,OutputValue]" \
    --output table

echo ""
echo "To manually trigger the Step Functions workflow:"
echo "  aws stepfunctions start-execution --state-machine-arn \$(aws cloudformation describe-stacks --stack-name FundDataFetchStack --query \"Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue\" --output text) --input '{\"triggered_by\": \"manual\"}'"
echo ""
echo "To check S3 data:"
echo "  aws s3 ls s3://fund-data-$AWS_ACCOUNT-$AWS_REGION/ --recursive"
