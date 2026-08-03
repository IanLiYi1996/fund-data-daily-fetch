#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { FundDataFetchStack } from "../lib/fund-data-fetch-stack";

const app = new cdk.App();

// Alerting config. Both channels are wired by default so an alarm can
// never publish into an empty topic again (which is exactly why the
// 2026-07-29 → 08-02 outage went unnoticed for five days).
//   -c alertEmail=...              override the email subscriber
//   -c slackWebhookSecretName=...   override the Secrets Manager secret
//                                   holding the Slack webhook URL
const alertEmail =
  app.node.tryGetContext("alertEmail") ?? "ianleely@amazon.com";
const slackWebhookSecretName = app.node.tryGetContext("slackWebhookSecretName");

new FundDataFetchStack(app, "FundDataFetchStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  description: "Fund Data Daily Fetch System - S3 + Lambda + EventBridge",
  alertEmail,
  slackWebhookSecretName,
});

app.synth();
