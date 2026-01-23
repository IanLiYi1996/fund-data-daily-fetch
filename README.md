# Fund Data Daily Fetch

每日定时从 akshare 拉取基金、股票指数、宏观经济数据并存储到 S3 的系统。

## 架构

```
┌─────────────────┐
│  EventBridge    │  (每日 17:00 UTC / 北京时间 01:00)
│  Cron Schedule  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Lambda         │  (容器镜像，支持 akshare)
│  Python 3.11    │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌───────┐ ┌───────────┐
│  S3   │ │CloudWatch │
│Bucket │ │  Logs     │
└───────┘ └───────────┘
```

## 数据类型

### 基金数据 (fund/)
- `fund_nav.parquet` - 开放式基金净值
- `fund_performance.parquet` - 基金业绩排名
- `fund_etf.parquet` - ETF 实时数据
- `fund_name.parquet` - 基金基本信息
- `fund_manager.parquet` - 基金经理信息

### 股票指数 (stock/)
- `stock_index_sh.parquet` - 上证系列指数
- `stock_index_sz.parquet` - 深证系列指数
- `stock_market_activity.parquet` - 市场活跃度

### 宏观经济 (macro/)
- `macro_lpr.parquet` - LPR 利率
- `macro_cpi.parquet` - CPI 通胀
- `macro_ppi.parquet` - PPI 指数

## S3 数据组织

```
s3://fund-data-{account}-{region}/
├── fund/{YYYY-MM-DD}/
│   ├── fund_nav.parquet
│   ├── fund_performance.parquet
│   ├── fund_etf.parquet
│   ├── fund_name.parquet
│   └── fund_manager.parquet
├── stock/{YYYY-MM-DD}/
│   ├── stock_index_sh.parquet
│   ├── stock_index_sz.parquet
│   └── stock_market_activity.parquet
└── macro/{YYYY-MM-DD}/
    ├── macro_lpr.parquet
    ├── macro_cpi.parquet
    └── macro_ppi.parquet
```

## 前置条件

- Node.js >= 18
- Python >= 3.11
- Docker
- AWS CLI (已配置凭证)
- AWS CDK CLI (`npm install -g aws-cdk`)

## 部署

```bash
# 部署
./scripts/deploy.sh

# 销毁 (S3 bucket 会保留)
./scripts/destroy.sh
```

## 手动触发

```bash
# 触发 Lambda
aws lambda invoke --function-name FundDataFetchLambda output.json
cat output.json | jq

# 检查 S3 数据
aws s3 ls s3://fund-data-xxx/fund/$(date +%Y-%m-%d)/

# 查看日志
aws logs tail /aws/lambda/FundDataFetchLambda --follow
```

## 使用 Athena 查询

Parquet 格式支持直接使用 Athena 查询：

```sql
-- 创建外部表
CREATE EXTERNAL TABLE fund_performance (
  fund_code STRING,
  fund_name STRING,
  nav DOUBLE,
  ytd_return DOUBLE
)
STORED AS PARQUET
LOCATION 's3://fund-data-xxx/fund/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 查询最新数据
SELECT * FROM fund_performance
WHERE dt = '2024-01-01'
ORDER BY ytd_return DESC
LIMIT 10;
```

## 项目结构

```
fund-data-daily-fetch/
├── cdk/                           # CDK 基础设施 (TypeScript)
│   ├── bin/cdk-app.ts             # CDK 入口
│   ├── lib/fund-data-fetch-stack.ts  # 主堆栈
│   ├── package.json
│   └── tsconfig.json
├── lambda/                        # Lambda 代码 (Python)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── handler.py             # Lambda 入口
│       ├── fetchers/              # 数据拉取模块
│       │   ├── base_fetcher.py
│       │   ├── fund_fetcher.py
│       │   ├── stock_fetcher.py
│       │   └── macro_fetcher.py
│       ├── storage/
│       │   └── s3_client.py
│       └── utils/
│           ├── config.py
│           ├── logger.py
│           └── retry.py
├── scripts/
│   ├── deploy.sh
│   └── destroy.sh
└── README.md
```

## AWS 资源

| 资源 | 配置 |
|------|------|
| S3 Bucket | 版本控制、30天智能分层、90天归档至 Glacier |
| Lambda | 容器镜像、1024MB 内存、15分钟超时 |
| EventBridge | 每日 17:00 UTC（北京时间 01:00） |
| CloudWatch | 日志保留2周 |
| SNS | 错误告警通知 |

## 配置告警邮件

在 `cdk/lib/fund-data-fetch-stack.ts` 中添加 `alertEmail` 参数：

```typescript
new FundDataFetchStack(app, "FundDataFetchStack", {
  alertEmail: "your-email@example.com",
});
```

## 本地开发

```bash
# 安装 Lambda 依赖
cd lambda
pip install -r requirements.txt

# 设置环境变量
export S3_BUCKET=your-test-bucket
export LOG_LEVEL=DEBUG

# 运行本地测试
python src/handler.py
```

## License

MIT
