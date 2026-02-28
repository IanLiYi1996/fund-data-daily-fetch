import {
  Stack,
  StackProps,
  Duration,
  RemovalPolicy,
  CfnOutput,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cloudwatchActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as path from "path";

export interface FundDataFetchStackProps extends StackProps {
  alertEmail?: string;
}

export class FundDataFetchStack extends Stack {
  public readonly bucket: s3.Bucket;
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props?: FundDataFetchStackProps) {
    super(scope, id, props);

    const lambdaDir = path.join(__dirname, "../../lambda");

    // ========== S3 Bucket ==========

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

    // ========== Lambda Environment ==========

    const lambdaEnv = {
      S3_BUCKET: this.bucket.bucketName,
      LOG_LEVEL: "INFO",
      PYTHONUNBUFFERED: "1",
    };

    // ========== Lambda Functions ==========

    const fundFetchLambda = this.createDockerLambda(
      "FundFetchLambda",
      lambdaDir,
      "fund-fetcher/Dockerfile",
      "Fetch fund data (20 sources) from akshare",
      2048,
      15,
      lambdaEnv
    );

    const cnIndexFetchLambda = this.createDockerLambda(
      "CnIndexFetchLambda",
      lambdaDir,
      "cn-index-fetcher/Dockerfile",
      "Fetch CN stock index data from akshare",
      1024,
      10,
      lambdaEnv
    );

    const cnMacroFetchLambda = this.createDockerLambda(
      "CnMacroFetchLambda",
      lambdaDir,
      "cn-macro-fetcher/Dockerfile",
      "Fetch CN macroeconomic data from akshare",
      1024,
      10,
      lambdaEnv
    );

    const aShareFetchLambda = this.createDockerLambda(
      "AShareFetchLambda",
      lambdaDir,
      "a-share-fetcher/Dockerfile",
      "Fetch A-share market data from akshare",
      2048,
      15,
      lambdaEnv
    );

    const hkStockFetchLambda = this.createDockerLambda(
      "HKStockFetchLambda",
      lambdaDir,
      "hk-stock-fetcher/Dockerfile",
      "Fetch HK stock market data from akshare",
      1024,
      10,
      lambdaEnv
    );

    const usStockFetchLambda = this.createDockerLambda(
      "USStockFetchLambda",
      lambdaDir,
      "us-stock-fetcher/Dockerfile",
      "Fetch US stock + US macro data from akshare",
      2048,
      15,
      lambdaEnv
    );

    const histKlineFetchLambda = this.createDockerLambda(
      "HistKlineFetchLambda",
      lambdaDir,
      "hist-kline-fetcher/Dockerfile",
      "Fetch historical K-line data (A-share/HK/US) via yfinance",
      2048,
      15,
      lambdaEnv
    );

    const dataProcessorLambda = this.createDockerLambda(
      "DataProcessorLambda",
      lambdaDir,
      "data-processor/Dockerfile",
      "Post-process raw parquet data into MCP-ready JSON",
      2048,
      10,
      lambdaEnv
    );

    const catalogLambda = this.createDockerLambda(
      "CatalogGeneratorLambda",
      lambdaDir,
      "catalog-generator/Dockerfile",
      "Generate data catalog from parallel results",
      512,
      5,
      lambdaEnv
    );

    // Grant S3 access to all Lambdas
    [
      fundFetchLambda,
      cnIndexFetchLambda,
      cnMacroFetchLambda,
      aShareFetchLambda,
      hkStockFetchLambda,
      usStockFetchLambda,
      histKlineFetchLambda,
      dataProcessorLambda,
      catalogLambda,
    ].forEach((fn) => this.bucket.grantReadWrite(fn));

    // ========== Step Functions ==========

    // Parallel data collection branches
    const parallelCollection = new sfn.Parallel(
      this,
      "ParallelDataCollection",
      {
        resultPath: "$.results",
        comment: "Fetch all data categories in parallel",
      }
    );

    // Fund fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeFundFetch", {
        lambdaFunction: fundFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Fund data (20 sources)",
      }).addCatch(
        new sfn.Pass(this, "FundFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "fund",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // CN Index fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeCnIndexFetch", {
        lambdaFunction: cnIndexFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "CN stock index data (3 sources)",
      }).addCatch(
        new sfn.Pass(this, "CnIndexFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "cn-index",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // CN Macro fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeCnMacroFetch", {
        lambdaFunction: cnMacroFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "CN macroeconomic data (3 sources)",
      }).addCatch(
        new sfn.Pass(this, "CnMacroFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "cn-macro",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // A-share fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeAShareFetch", {
        lambdaFunction: aShareFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "A-share market data (11 sources)",
      }).addCatch(
        new sfn.Pass(this, "AShareFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "a-share",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // HK Stock fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeHKStockFetch", {
        lambdaFunction: hkStockFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "HK stock market data (5 sources)",
      }).addCatch(
        new sfn.Pass(this, "HKStockFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "hk-stock",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // US Stock + Macro fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeUSStockFetch", {
        lambdaFunction: usStockFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "US stock + US macro data (18 sources)",
      }).addCatch(
        new sfn.Pass(this, "USStockFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "us-stock",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // Historical K-line fetch branch
    parallelCollection.branch(
      new tasks.LambdaInvoke(this, "InvokeHistKlineFetch", {
        lambdaFunction: histKlineFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Historical K-line data (9 sources: 3 markets x 3 frequencies)",
      }).addCatch(
        new sfn.Pass(this, "HistKlineFetchFailed", {
          result: sfn.Result.fromObject({
            downloader: "hist-kline",
            success: false,
            error: "Lambda invocation failed",
          }),
        }),
        { errors: ["States.ALL"], resultPath: "$" }
      )
    );

    // Data processor step (sequential after parallel, before catalog)
    // Passes minimal trigger event — processor reads parquet directly from S3
    const dataProcessorStep = new tasks.LambdaInvoke(
      this,
      "InvokeDataProcessor",
      {
        lambdaFunction: dataProcessorLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Post-process raw data into MCP-ready JSON",
        payload: sfn.TaskInput.fromObject({
          action: "process",
          triggered_by: "step-functions",
        }),
        resultPath: "$.processing",
      }
    ).addCatch(
      new sfn.Pass(this, "DataProcessorFailed", {
        result: sfn.Result.fromObject({
          downloader: "data-processor",
          success: false,
          error: "Data processor Lambda failed",
        }),
        resultPath: "$.processing",
      }),
      { errors: ["States.ALL"] }
    );

    // Catalog generation step (sequential after data processing)
    // Receives minimal trigger — reads fetcher results from data-processor summary
    const catalogStep = new tasks.LambdaInvoke(
      this,
      "InvokeCatalogGenerator",
      {
        lambdaFunction: catalogLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Generate unified data catalog",
        payload: sfn.TaskInput.fromObject({
          action: "generate-catalog",
          processing: sfn.JsonPath.objectAt("$.processing"),
        }),
      }
    );

    // Define state machine: parallel collection → data processing → catalog
    const definition = parallelCollection
      .next(dataProcessorStep)
      .next(catalogStep);

    // Step Functions log group
    const sfnLogGroup = new logs.LogGroup(this, "DataCollectionWorkflowLogs", {
      logGroupName: "/aws/stepfunctions/DataCollectionWorkflow",
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.stateMachine = new sfn.StateMachine(
      this,
      "DataCollectionWorkflow",
      {
        stateMachineName: "FundDataCollectionWorkflow",
        definitionBody: sfn.DefinitionBody.fromChainable(definition),
        timeout: Duration.minutes(30),
        tracingEnabled: true,
        logs: {
          destination: sfnLogGroup,
          level: sfn.LogLevel.ALL,
          includeExecutionData: true,
        },
        comment:
          "Orchestrates parallel data collection (fund, CN index, CN macro, A-share, HK stock, US stock+macro, hist K-line), data processing, and catalog generation",
      }
    );

    // ========== EventBridge Schedule ==========

    const scheduleRule = new events.Rule(this, "DailyScheduleRule", {
      ruleName: "FundDataFetchDailySchedule",
      description:
        "Trigger fund data collection workflow daily at 17:00 UTC (01:00 Beijing Time)",
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "17",
        day: "*",
        month: "*",
        year: "*",
      }),
    });

    scheduleRule.addTarget(
      new targets.SfnStateMachine(this.stateMachine, {
        input: events.RuleTargetInput.fromObject({
          timestamp: events.EventField.time,
          triggered_by: "scheduled-daily-collection",
        }),
      })
    );

    // ========== SNS Topic ==========

    const alertTopic = new sns.Topic(this, "FundDataFetchAlertTopic", {
      topicName: "FundDataFetchAlerts",
      displayName: "Fund Data Fetch Alerts",
    });

    if (props?.alertEmail) {
      alertTopic.addSubscription(
        new subscriptions.EmailSubscription(props.alertEmail)
      );
    }

    // ========== CloudWatch Alarms ==========

    // Step Functions execution failure alarm
    const sfnFailureAlarm = new cloudwatch.Alarm(
      this,
      "WorkflowExecutionFailedAlarm",
      {
        alarmName: "FundDataCollectionWorkflowFailed",
        alarmDescription:
          "Alarm when the data collection Step Functions workflow fails",
        metric: this.stateMachine.metricFailed({
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator
            .GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }
    );

    sfnFailureAlarm.addAlarmAction(
      new cloudwatchActions.SnsAction(alertTopic)
    );

    // Step Functions execution timeout alarm
    const sfnTimeoutAlarm = new cloudwatch.Alarm(
      this,
      "WorkflowExecutionTimedOutAlarm",
      {
        alarmName: "FundDataCollectionWorkflowTimedOut",
        alarmDescription:
          "Alarm when the data collection workflow times out",
        metric: this.stateMachine.metricTimedOut({
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator
            .GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }
    );

    sfnTimeoutAlarm.addAlarmAction(
      new cloudwatchActions.SnsAction(alertTopic)
    );

    // ========== Outputs ==========

    new CfnOutput(this, "BucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for fund data",
      exportName: "FundDataBucketName",
    });

    new CfnOutput(this, "StateMachineArn", {
      value: this.stateMachine.stateMachineArn,
      description: "Step Functions state machine ARN",
      exportName: "FundDataCollectionWorkflowArn",
    });

    new CfnOutput(this, "StateMachineName", {
      value: this.stateMachine.stateMachineName,
      description: "Step Functions state machine name",
      exportName: "FundDataCollectionWorkflowName",
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

  /**
   * Helper to create a Docker-based Lambda function with standard configuration.
   */
  private createDockerLambda(
    id: string,
    lambdaDir: string,
    dockerfile: string,
    description: string,
    memorySize: number,
    timeoutMinutes: number,
    environment: Record<string, string>
  ): lambda.DockerImageFunction {
    return new lambda.DockerImageFunction(this, id, {
      code: lambda.DockerImageCode.fromImageAsset(lambdaDir, {
        file: dockerfile,
        platform: ecrAssets.Platform.LINUX_AMD64,
      }),
      memorySize,
      timeout: Duration.minutes(timeoutMinutes),
      environment,
      description,
    });
  }
}
