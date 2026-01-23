import { Stack, StackProps, Duration, RemovalPolicy, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cloudwatchActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import * as path from "path";

export interface FundDataFetchStackProps extends StackProps {
  alertEmail?: string;
}

export class FundDataFetchStack extends Stack {
  public readonly bucket: s3.Bucket;
  public readonly lambdaFunction: lambda.Function;

  constructor(scope: Construct, id: string, props?: FundDataFetchStackProps) {
    super(scope, id, props);

    // S3 Bucket with lifecycle policies
    this.bucket = new s3.Bucket(this, "FundDataBucket", {
      bucketName: `fund-data-${this.account}-${this.region}`,
      versioned: true,
      removalPolicy: RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: "IntelligentTiering",
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: Duration.days(30),
            },
          ],
        },
        {
          id: "GlacierArchive",
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: Duration.days(90),
            },
          ],
        },
        {
          id: "DeleteOldVersions",
          enabled: true,
          noncurrentVersionExpiration: Duration.days(365),
        },
      ],
    });

    // CloudWatch Log Group
    const logGroup = new logs.LogGroup(this, "FundDataFetchLogs", {
      logGroupName: "/aws/lambda/FundDataFetchLambda",
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Lambda execution role
    const lambdaRole = new iam.Role(this, "FundDataFetchLambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      description: "Execution role for Fund Data Fetch Lambda",
    });

    // Add CloudWatch Logs permissions
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ],
        resources: [logGroup.logGroupArn, `${logGroup.logGroupArn}:*`],
      })
    );

    // Add S3 permissions
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
        ],
        resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
      })
    );

    // Build Docker image for Lambda
    const dockerImageAsset = new ecrAssets.DockerImageAsset(this, "FundDataFetchImage", {
      directory: path.join(__dirname, "../../lambda"),
      file: "Dockerfile",
      platform: ecrAssets.Platform.LINUX_AMD64,
    });

    // Lambda Function (Container Image)
    this.lambdaFunction = new lambda.DockerImageFunction(this, "FundDataFetchLambda", {
      functionName: "FundDataFetchLambda",
      description: "Daily fund data fetch from akshare to S3",
      code: lambda.DockerImageCode.fromEcr(dockerImageAsset.repository, {
        tagOrDigest: dockerImageAsset.imageTag,
      }),
      memorySize: 1024,
      timeout: Duration.minutes(15),
      role: lambdaRole,
      logGroup: logGroup,
      environment: {
        S3_BUCKET: this.bucket.bucketName,
        LOG_LEVEL: "INFO",
        PYTHONUNBUFFERED: "1",
      },
    });

    // EventBridge Rule - Daily at 17:00 UTC (01:00 Beijing Time)
    const scheduleRule = new events.Rule(this, "DailyScheduleRule", {
      ruleName: "FundDataFetchDailySchedule",
      description: "Trigger fund data fetch daily at 17:00 UTC (01:00 Beijing Time)",
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "17",
        day: "*",
        month: "*",
        year: "*",
      }),
    });

    scheduleRule.addTarget(new targets.LambdaFunction(this.lambdaFunction, {
      retryAttempts: 2,
    }));

    // SNS Topic for alerts
    const alertTopic = new sns.Topic(this, "FundDataFetchAlertTopic", {
      topicName: "FundDataFetchAlerts",
      displayName: "Fund Data Fetch Alerts",
    });

    // Add email subscription if provided
    if (props?.alertEmail) {
      alertTopic.addSubscription(
        new subscriptions.EmailSubscription(props.alertEmail)
      );
    }

    // CloudWatch Alarm for Lambda errors
    const errorAlarm = new cloudwatch.Alarm(this, "FundDataFetchErrorAlarm", {
      alarmName: "FundDataFetchLambdaErrors",
      alarmDescription: "Alarm when fund data fetch Lambda has errors",
      metric: this.lambdaFunction.metricErrors({
        period: Duration.minutes(5),
        statistic: "Sum",
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    errorAlarm.addAlarmAction(new cloudwatchActions.SnsAction(alertTopic));

    // CloudWatch Alarm for Lambda duration (approaching timeout)
    const durationAlarm = new cloudwatch.Alarm(this, "FundDataFetchDurationAlarm", {
      alarmName: "FundDataFetchLambdaDuration",
      alarmDescription: "Alarm when fund data fetch Lambda approaches timeout",
      metric: this.lambdaFunction.metricDuration({
        period: Duration.minutes(5),
        statistic: "Maximum",
      }),
      threshold: 840000, // 14 minutes in milliseconds (timeout is 15 min)
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    durationAlarm.addAlarmAction(new cloudwatchActions.SnsAction(alertTopic));

    // Outputs
    new CfnOutput(this, "BucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for fund data",
      exportName: "FundDataBucketName",
    });

    new CfnOutput(this, "LambdaFunctionName", {
      value: this.lambdaFunction.functionName,
      description: "Lambda function name",
      exportName: "FundDataFetchLambdaName",
    });

    new CfnOutput(this, "LambdaFunctionArn", {
      value: this.lambdaFunction.functionArn,
      description: "Lambda function ARN",
      exportName: "FundDataFetchLambdaArn",
    });

    new CfnOutput(this, "AlertTopicArn", {
      value: alertTopic.topicArn,
      description: "SNS topic ARN for alerts",
      exportName: "FundDataFetchAlertTopicArn",
    });

    new CfnOutput(this, "ScheduleRuleArn", {
      value: scheduleRule.ruleArn,
      description: "EventBridge schedule rule ARN",
      exportName: "FundDataFetchScheduleRuleArn",
    });
  }
}
