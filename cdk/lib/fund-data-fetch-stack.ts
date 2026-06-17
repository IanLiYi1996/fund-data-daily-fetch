import {
  Stack,
  StackProps,
  Duration,
  RemovalPolicy,
  CfnOutput,
  Size,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as events from "aws-cdk-lib/aws-events";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
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
  bucketName?: string;
  s3Prefix?: string;
}

export class FundDataFetchStack extends Stack {
  public readonly bucket: s3.IBucket;
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props?: FundDataFetchStackProps) {
    super(scope, id, props);

    const lambdaDir = path.join(__dirname, "../../lambda");

    // ========== S3 Bucket (owned by this stack) ==========
    //
    // The pipeline previously used a bucket owned by an unrelated stack
    // (InvestmentAdvisory). When that stack was torn down on 2026-06-12 the
    // data bucket — and the entire data lake — went with it. Owning the
    // bucket here makes the lifecycle of data and pipeline align: deleting
    // FundDataFetchStack is the only path that drops the bucket, and even
    // then RemovalPolicy.RETAIN forces a manual confirm.
    //
    // The bucket itself is the source of truth for replication into
    // Mengxin's account. We re-attach the same replication-role ARN that
    // the destination-side bucket policy in financial-dataset-mx already
    // trusts, so no cross-account changes are required.

    const bucketName =
      props?.bucketName ?? `fund-data-pipeline-${this.account}-${this.region}`;
    const s3Prefix = props?.s3Prefix ?? "fund-data-pipeline/";

    const replicationRole = iam.Role.fromRoleArn(
      this,
      "ReplicationRoleToMengxin",
      `arn:aws:iam::${this.account}:role/s3-replication-to-financial-dataset-mx`,
      { mutable: false }
    );
    const mengxinDestination = s3.Bucket.fromBucketAttributes(
      this,
      "MengxinDestinationBucket",
      {
        bucketName: "financial-dataset-mx",
        account: "845861764576",
        region: "us-east-1",
      }
    );

    const replicatedPrefixes = [
      "data/",
      `${s3Prefix}fund/`,
      `${s3Prefix}stock/`,
      `${s3Prefix}macro/`,
      `${s3Prefix}a_share/`,
      `${s3Prefix}hk_stock/`,
      `${s3Prefix}us_stock/`,
      `${s3Prefix}hist_kline/`,
      `${s3Prefix}hist_kline_indicators/`,
      `${s3Prefix}fund_history/`,
    ];

    const dataBucket = new s3.Bucket(this, "FundDataBucket", {
      bucketName,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      removalPolicy: RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
      replicationRole,
      replicationRules: replicatedPrefixes.map((prefix, i) => ({
        id: `replicate-${prefix.replace(/\W+/g, "-").replace(/-+$/, "")}`,
        destination: mengxinDestination,
        accessControlTransition: true,
        priority: i + 1,
        deleteMarkerReplication: false,
        filter: { prefix },
      })),
    });
    this.bucket = dataBucket;

    // ========== Glue Catalog ==========
    const FUND_DATA_LAKE_DB = "fund_data_lake";
    const glueDatabase = new glue.CfnDatabase(this, "FundDataLakeDb", {
      catalogId: this.account,
      databaseInput: { name: FUND_DATA_LAKE_DB },
    });

    // ========== Lambda Environment ==========

    const lambdaEnv = {
      S3_BUCKET: bucketName,
      S3_PREFIX: s3Prefix,
      LOG_LEVEL: "INFO",
      PYTHONUNBUFFERED: "1",
      WAREHOUSE_PATH: `s3://${bucketName}/${s3Prefix}iceberg/`,
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

    const exportFundHistoryLambda = this.createDockerLambda(
      "ExportFundHistoryLambda",
      lambdaDir,
      "export-fund-history/Dockerfile",
      "Export current month's fund_daily to partitioned parquet for cross-account share",
      2048,
      10,
      lambdaEnv
    );

    const fundHistoryFetchLambda = this.createDockerLambda(
      "FundHistoryFetchLambda",
      lambdaDir,
      "fund-history-fetcher/Dockerfile",
      "Fetch per-fund manager tenure + scale history (one partition or merge)",
      3008,
      15,
      lambdaEnv
    );

    // Grant S3 access scoped to {bucket}/{s3Prefix}* (not the whole shared bucket).
    const listBucketPolicy = new iam.PolicyStatement({
      actions: ["s3:ListBucket", "s3:GetBucketLocation"],
      resources: [this.bucket.bucketArn],
      conditions: {
        StringLike: { "s3:prefix": [`${s3Prefix}*`] },
      },
    });
    const objectPolicy = new iam.PolicyStatement({
      actions: [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
      ],
      resources: [`${this.bucket.bucketArn}/${s3Prefix}*`],
    });
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
      exportFundHistoryLambda,
      fundHistoryFetchLambda,
    ].forEach((fn) => {
      fn.addToRolePolicy(listBucketPolicy);
      fn.addToRolePolicy(objectPolicy);
    });

    // ========== Iceberg / Glue IAM Policy ==========

    const icebergGluePolicy = new iam.PolicyStatement({
      actions: [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
      ],
      resources: [
        `arn:aws:glue:${this.region}:${this.account}:catalog`,
        `arn:aws:glue:${this.region}:${this.account}:database/${FUND_DATA_LAKE_DB}`,
        `arn:aws:glue:${this.region}:${this.account}:table/${FUND_DATA_LAKE_DB}/*`,
      ],
    });

    [
      fundFetchLambda,
      cnIndexFetchLambda,
      cnMacroFetchLambda,
      aShareFetchLambda,
      hkStockFetchLambda,
      usStockFetchLambda,
      histKlineFetchLambda,
      catalogLambda,
    ].forEach((fn) => fn.addToRolePolicy(icebergGluePolicy));

    // ========== Iceberg Maintenance Lambda ==========

    const icebergMaintenanceLambda = this.createDockerLambda(
      "IcebergMaintenanceLambda",
      lambdaDir,
      "iceberg-maintenance/Dockerfile",
      "Weekly Iceberg compaction + snapshot expiration",
      3008,
      14,
      lambdaEnv
    );
    icebergMaintenanceLambda.addToRolePolicy(listBucketPolicy);
    icebergMaintenanceLambda.addToRolePolicy(objectPolicy);
    icebergMaintenanceLambda.addToRolePolicy(icebergGluePolicy);

    // Ensure all Iceberg-writing Lambdas are created after the Glue DB.
    const icebergClients = [
      fundFetchLambda,
      cnIndexFetchLambda,
      cnMacroFetchLambda,
      aShareFetchLambda,
      hkStockFetchLambda,
      usStockFetchLambda,
      histKlineFetchLambda,
      icebergMaintenanceLambda,
      catalogLambda,
    ];
    icebergClients.forEach((fn) => fn.node.addDependency(glueDatabase));

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

    // Fund fetch branch — partitioned fan-out by table.
    //
    // Previously a single Lambda fetched all 21 fund tables serially via
    // akshare and routinely hit the 15 min Lambda timeout (fetch ~9 min +
    // upserts ~7 min). Splitting them into a Map state runs each table in
    // its own Lambda concurrently, so the slowest single table (~5 min:
    // fund_daily fetch + upsert) bounds the whole branch.
    const FUND_TABLES = [
      "fund_performance", "fund_etf", "fund_name", "fund_manager",
      "fund_daily", "fund_money_daily", "fund_financial_daily",
      "fund_etf_daily", "fund_lof", "fund_value_estimation", "fund_purchase",
      "fund_exchange_rank", "fund_money_rank", "fund_hk_rank",
      "fund_rating", "fund_dividend_rank", "fund_dividend", "fund_split",
      "fund_index_info", "fund_graded_daily", "fund_reits_daily",
    ];

    const fundPartitionTask = new tasks.LambdaInvoke(
      this,
      "InvokeFundPartition",
      {
        lambdaFunction: fundFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Fetch one fund table partition",
      }
    ).addCatch(
      new sfn.Pass(this, "FundPartitionFailed", {
        result: sfn.Result.fromObject({
          downloader: "fund",
          success: false,
          error: "Lambda partition failed",
        }),
      }),
      { errors: ["States.ALL"] }
    );

    const fundMap = new sfn.Map(this, "FundPartitionMap", {
      itemsPath: "$.fund_partitions",
      maxConcurrency: 8,
      comment: "Fan out fund fetch across 21 table slices (cap concurrency to 8 to avoid akshare rate limits)",
    });
    fundMap.itemProcessor(fundPartitionTask);

    const fundBranch = new sfn.Pass(this, "FundSeed", {
      result: sfn.Result.fromObject({
        fund_partitions: FUND_TABLES.map((table) => ({ table })),
      }),
    }).next(fundMap);

    parallelCollection.branch(fundBranch);

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

    // Historical K-line fetch branch — partitioned fan-out.
    //
    // 9 partitions = 3 markets (a_share, hk, us) × 3 frequencies (daily,
    // weekly, monthly). Previously a single Lambda fetched all 9 serially
    // via yfinance and routinely hit the 15 min Lambda timeout. Splitting
    // them into a Map state runs each (market, frequency) in its own
    // Lambda concurrently, so the slowest single partition (~3 min)
    // bounds the whole branch.
    const HIST_KLINE_PARTITIONS: Array<{ market: string; interval: string }> = [];
    for (const market of ["a_share", "hk", "us"]) {
      for (const interval of ["daily", "weekly", "monthly"]) {
        HIST_KLINE_PARTITIONS.push({ market, interval });
      }
    }

    const histKlinePartitionTask = new tasks.LambdaInvoke(
      this,
      "InvokeHistKlinePartition",
      {
        lambdaFunction: histKlineFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Fetch one (market, frequency) hist-kline partition",
      }
    ).addCatch(
      new sfn.Pass(this, "HistKlinePartitionFailed", {
        result: sfn.Result.fromObject({
          downloader: "hist-kline",
          success: false,
          error: "Lambda partition failed",
        }),
      }),
      { errors: ["States.ALL"] }
    );

    const histKlineMap = new sfn.Map(this, "HistKlinePartitionMap", {
      itemsPath: "$.hist_kline_partitions",
      maxConcurrency: HIST_KLINE_PARTITIONS.length,
      comment: "Fan out hist-kline fetch across 9 (market, frequency) slices",
    });
    histKlineMap.itemProcessor(histKlinePartitionTask);

    // Inject the partitions array at the start of the branch so the
    // top-level workflow input doesn't need to know about hist-kline.
    const histKlineBranch = new sfn.Pass(this, "HistKlineSeed", {
      result: sfn.Result.fromObject({
        hist_kline_partitions: HIST_KLINE_PARTITIONS,
      }),
    }).next(histKlineMap);

    parallelCollection.branch(histKlineBranch);

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
        resultPath: "$.catalog",
      }
    );

    // Export current month's fund_daily to partitioned parquet for cross-account
    // share with financial-dataset-mx (S3 Replication mirrors automatically).
    // Failure here should not fail the whole pipeline — it only affects the
    // downstream share, not the primary Iceberg data.
    const exportFundHistoryStep = new tasks.LambdaInvoke(
      this,
      "InvokeExportFundHistory",
      {
        lambdaFunction: exportFundHistoryLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Export current month fund_daily parquet for cross-account share",
        payload: sfn.TaskInput.fromObject({}),
        resultPath: "$.export_fund_history",
      }
    ).addCatch(
      new sfn.Pass(this, "ExportFundHistoryFailed", {
        result: sfn.Result.fromObject({
          downloader: "export-fund-history",
          success: false,
          error: "Export fund history Lambda failed",
        }),
        resultPath: "$.export_fund_history",
      }),
      { errors: ["States.ALL"] }
    );

    // Define state machine:
    //   parallel collection → data processing → catalog → export fund_history
    const definition = parallelCollection
      .next(dataProcessorStep)
      .next(catalogStep)
      .next(exportFundHistoryStep);

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

    const weeklyMaintenanceRule = new events.Rule(this, "WeeklyMaintenanceRule", {
      ruleName: "FundDataLakeWeeklyMaintenance",
      description: "Weekly Iceberg compaction + snapshot expiration (Sunday 20:00 UTC)",
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "20",
        weekDay: "SUN",
      }),
    });
    weeklyMaintenanceRule.addTarget(
      new targets.LambdaFunction(icebergMaintenanceLambda)
    );

    // ========== Fund History Step Functions (partitioned fanout + merge) ==========
    //
    // 25k funds × 2 sources can't fit in one 15 min Lambda. We fan out into
    // FUND_HISTORY_PARTITIONS partitions via a Map state, each writing
    // fund_*_history__part{i}.parquet, then run a merge step that concats
    // the parts into a single fund_*_history.parquet and deletes the parts.

    // 8 partitions × ~3.1k funds/partition × 8 worker concurrency
    // ≈ 7-8 min/partition. Stays well under Lambda 15 min timeout for both
    // manager (fast HTML page) and scale (~500 KB pingzhongdata.js per fund).
    const FUND_HISTORY_PARTITIONS = 8;

    const partitionTask = new tasks.LambdaInvoke(
      this,
      "InvokeFundHistoryPartition",
      {
        lambdaFunction: fundHistoryFetchLambda,
        retryOnServiceExceptions: true,
        payloadResponseOnly: true,
        comment: "Fetch one partition of fund history",
      }
    ).addCatch(
      new sfn.Pass(this, "FundHistoryPartitionFailed", {
        result: sfn.Result.fromObject({
          downloader: "fund-history",
          success: false,
          error: "Lambda partition failed",
        }),
      }),
      { errors: ["States.ALL"] }
    );

    const partitionMap = new sfn.Map(this, "FundHistoryPartitionMap", {
      itemsPath: "$.partitions",
      maxConcurrency: FUND_HISTORY_PARTITIONS,
      resultPath: "$.partition_results",
      comment: "Fan out fund-history fetch across partitions",
    });
    partitionMap.itemProcessor(partitionTask);

    // After all partitions finish, merge per-part parquet files into a
    // single fund_*_history.parquet so downstream consumers can read one file.
    // snapshot_date intentionally omitted; merge Lambda defaults to UTC today,
    // which is the same day the partitions just wrote to.
    const mergeTask = new tasks.LambdaInvoke(this, "InvokeFundHistoryMerge", {
      lambdaFunction: fundHistoryFetchLambda,
      retryOnServiceExceptions: true,
      payloadResponseOnly: true,
      comment: "Merge per-partition parquet files into a single history file",
      payload: sfn.TaskInput.fromObject({
        mode: sfn.JsonPath.stringAt("$.merge_mode"),
        partition_total: FUND_HISTORY_PARTITIONS,
      }),
      resultPath: "$.merge_result",
    }).addCatch(
      new sfn.Pass(this, "FundHistoryMergeFailed", {
        result: sfn.Result.fromObject({
          downloader: "fund-history-merge",
          success: false,
          error: "Merge Lambda failed",
        }),
        resultPath: "$.merge_result",
      }),
      { errors: ["States.ALL"] }
    );

    const fundHistoryDefinition = partitionMap.next(mergeTask);

    const fundHistoryLogGroup = new logs.LogGroup(
      this,
      "FundHistoryWorkflowLogs",
      {
        logGroupName: "/aws/stepfunctions/FundHistoryWorkflow",
        retention: logs.RetentionDays.TWO_WEEKS,
        removalPolicy: RemovalPolicy.DESTROY,
      }
    );

    const fundHistoryStateMachine = new sfn.StateMachine(
      this,
      "FundHistoryWorkflow",
      {
        stateMachineName: "FundHistoryFetchWorkflow",
        definitionBody: sfn.DefinitionBody.fromChainable(fundHistoryDefinition),
        timeout: Duration.minutes(60),
        tracingEnabled: true,
        logs: {
          destination: fundHistoryLogGroup,
          level: sfn.LogLevel.ALL,
          includeExecutionData: true,
        },
        comment:
          "Fan-out fetch of per-fund manager tenure + scale history across partitions, then merge into a single parquet",
      }
    );

    // EventBridge input includes both per-partition Map items and top-level
    // $.merge_mode the merge step reads.
    const buildPartitionsInput = (mode: "manager_full" | "scale_full") => {
      const merge_mode = mode === "manager_full" ? "manager_merge" : "scale_merge";
      return events.RuleTargetInput.fromObject({
        partitions: Array.from(
          { length: FUND_HISTORY_PARTITIONS },
          (_, i) => ({
            mode,
            partition_index: i,
            partition_total: FUND_HISTORY_PARTITIONS,
          })
        ),
        merge_mode,
        triggered_by: `scheduled-${mode}`,
      });
    };

    // Weekly: manager tenure full refresh — Sundays 18:00 UTC
    const managerHistoryRule = new events.Rule(
      this,
      "FundManagerHistoryWeeklySchedule",
      {
        ruleName: "FundManagerHistoryWeeklySchedule",
        description:
          "Weekly full refresh of fund_manager_history (Sundays 18:00 UTC)",
        schedule: events.Schedule.cron({
          minute: "0",
          hour: "18",
          weekDay: "SUN",
        }),
      }
    );
    managerHistoryRule.addTarget(
      new targets.SfnStateMachine(fundHistoryStateMachine, {
        input: buildPartitionsInput("manager_full"),
      })
    );

    // Quarterly: scale history full refresh — Jan/Apr/Jul/Oct 4th at 19:00 UTC
    // (3-day buffer after quarter end so most fund-of-quarter data is published)
    const scaleHistoryRule = new events.Rule(
      this,
      "FundScaleHistoryQuarterlySchedule",
      {
        ruleName: "FundScaleHistoryQuarterlySchedule",
        description:
          "Quarterly full refresh of fund_scale_history (Jan/Apr/Jul/Oct 4th 19:00 UTC)",
        schedule: events.Schedule.cron({
          minute: "0",
          hour: "19",
          day: "4",
          month: "1,4,7,10",
        }),
      }
    );
    scaleHistoryRule.addTarget(
      new targets.SfnStateMachine(fundHistoryStateMachine, {
        input: buildPartitionsInput("scale_full"),
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

    // Fund history workflow failure alarm
    const fundHistoryFailureAlarm = new cloudwatch.Alarm(
      this,
      "FundHistoryWorkflowFailedAlarm",
      {
        alarmName: "FundHistoryWorkflowFailed",
        alarmDescription:
          "Alarm when the fund-history fetch workflow fails",
        metric: fundHistoryStateMachine.metricFailed({
          period: Duration.minutes(15),
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
    fundHistoryFailureAlarm.addAlarmAction(
      new cloudwatchActions.SnsAction(alertTopic)
    );

    // ========== Outputs ==========

    new CfnOutput(this, "BucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for fund data",
      exportName: "FundDataBucketName",
    });

    new CfnOutput(this, "S3Prefix", {
      value: s3Prefix,
      description: "S3 key prefix for fund data pipeline",
      exportName: "FundDataS3Prefix",
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

    new CfnOutput(this, "GlueDatabaseName", {
      value: FUND_DATA_LAKE_DB,
      description: "Glue database for Iceberg tables",
      exportName: "FundDataLakeGlueDb",
    });

    new CfnOutput(this, "FundHistoryWorkflowArn", {
      value: fundHistoryStateMachine.stateMachineArn,
      description: "Fund history fetch Step Functions ARN",
      exportName: "FundHistoryWorkflowArn",
    });

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
        cpu: 2048,
        memoryLimitMiB: 4096,
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
      // pyiceberg + pandas round-trip uses /tmp for parquet staging; 512 MB
      // default fills up on 26k-row tables. 2 GB is plenty and cheap.
      ephemeralStorageSize: Size.mebibytes(2048),
      environment,
      description,
    });
  }
}
