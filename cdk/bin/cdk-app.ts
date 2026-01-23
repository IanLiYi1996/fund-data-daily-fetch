#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { FundDataFetchStack } from "../lib/fund-data-fetch-stack";

const app = new cdk.App();

new FundDataFetchStack(app, "FundDataFetchStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  description: "Fund Data Daily Fetch System - S3 + Lambda + EventBridge",
});

app.synth();
