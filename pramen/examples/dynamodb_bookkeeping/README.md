# DynamoDB Bookkeeping Example

This example demonstrates how to configure Pramen to use AWS DynamoDB for bookkeeping instead of MongoDB, JDBC databases, or Hadoop-based storage.

## Overview

DynamoDB bookkeeping provides a serverless, fully managed solution for tracking pipeline state, record counts, and data availability in Pramen pipelines.

### Benefits

- **Serverless**: No database servers to manage or maintain
- **Auto-scaling**: Automatically scales to handle workload
- **Pay-per-request**: No fixed costs, pay only for what you use
- **High Availability**: Built-in replication across AWS availability zones
- **Multi-environment**: Easy separation via table prefixes
- **Automatic Table Creation**: Tables are created automatically on first run

## Configuration

### Minimal Configuration

```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
}
```

This creates tables:
- `pramen_bookkeeping`
- `pramen_schemas`

### Production Configuration

```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
  dynamodb.table.prefix = "pramen_production"
}
```

This creates tables:
- `pramen_production_bookkeeping`
- `pramen_production_schemas`

### Multi-Environment Configuration

**Development:**
```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
  dynamodb.table.prefix = "pramen_dev"
}
```

**Staging:**
```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
  dynamodb.table.prefix = "pramen_staging"
}
```

**Production:**
```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
  dynamodb.table.prefix = "pramen_production"
}
```

### Cross-Account Configuration

If DynamoDB tables are in a different AWS account:

```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-west-2"
  dynamodb.table.arn = "arn:aws:dynamodb:us-west-2:987654321098:table/"
  dynamodb.table.prefix = "shared_pramen"
}
```

## AWS Setup

### 1. AWS Credentials

Pramen uses the AWS SDK's `DefaultCredentialsProvider`, which loads credentials from:

1. **Environment Variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

2. **AWS Credentials File** (`~/.aws/credentials`):
   ```ini
   [default]
   aws_access_key_id = your_access_key
   aws_secret_access_key = your_secret_key
   region = us-east-1
   ```

3. **IAM Role** (recommended for EC2, ECS, EMR, etc.):
   - No credentials needed in configuration
   - Automatically uses the instance/task role

### 2. Required IAM Permissions

Create an IAM policy with these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable",
        "dynamodb:Query",
        "dynamodb:PutItem",
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:Scan"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:*:table/pramen_*"
      ]
    }
  ]
}
```

**Note**: Adjust the region and table name pattern based on your configuration.

### 3. Table Structure

Tables are automatically created with the following schema:

#### Bookkeeping Table (`{prefix}_bookkeeping`)
- **Partition Key**: `tableName` (String)
- **Sort Key**: `infoDate` (String, format: yyyy-MM-dd)
- **Billing Mode**: PAY_PER_REQUEST (on-demand)
- **Attributes**:
  - `tableName`: Name of the metastore table
  - `infoDate`: Information date
  - `infoDateBegin`: Start of date range
  - `infoDateEnd`: End of date range
  - `inputRecordCount`: Number of input records
  - `outputRecordCount`: Number of output records
  - `jobStarted`: Job start timestamp (milliseconds)
  - `jobFinished`: Job finish timestamp (milliseconds)
  - `batchId`: Batch execution ID
  - `appendedRecordCount`: Records appended (optional)

#### Schema Table (`{prefix}_schemas`)
- **Partition Key**: `tableName` (String)
- **Sort Key**: `infoDate` (String)
- **Billing Mode**: PAY_PER_REQUEST (on-demand)
- **Attributes**:
  - `tableName`: Name of the metastore table
  - `infoDate`: Date when schema was recorded
  - `schemaJson`: Spark schema in JSON format

## Running the Example

1. **Configure AWS credentials** (see above)

2. **Update the configuration file**:
   ```bash
   vi examples/dynamodb_bookkeeping/dynamodb_bookkeeping.conf
   ```

3. **Run Pramen**:
   ```bash
   spark-submit \
     --class za.co.absa.pramen.runner.PipelineRunner \
     --master local[*] \
     pramen-runner_2.12-1.13.10.jar \
     --config examples/dynamodb_bookkeeping/dynamodb_bookkeeping.conf \
     --date 2024-01-15
   ```

4. **Verify tables were created**:
   ```bash
   aws dynamodb list-tables --region us-east-1
   ```

   You should see:
   - `pramen_production_bookkeeping`
   - `pramen_production_schemas`

5. **Query bookkeeping data**:
   ```bash
   aws dynamodb query \
     --table-name pramen_production_bookkeeping \
     --key-condition-expression "tableName = :table" \
     --expression-attribute-values '{":table":{"S":"example_table"}}' \
     --region us-east-1
   ```

## Cost Considerations

DynamoDB uses pay-per-request billing:

- **On-demand mode** (default):
  - Write: $1.25 per million write requests
  - Read: $0.25 per million read requests
  - Storage: $0.25 per GB-month

- **Typical Pramen workload**:
  - Small pipelines: < $1/month
  - Medium pipelines: $5-20/month
  - Large pipelines: $50-100/month

**Cost optimization tips**:
1. Use table prefixes to separate environments (avoid duplicating production data)
2. Archive old bookkeeping data periodically
3. Monitor usage via AWS Cost Explorer

## Troubleshooting

### Issue: "Access Denied" error

**Cause**: Missing IAM permissions

**Solution**: Verify IAM policy includes all required DynamoDB permissions

### Issue: "Region not found" error

**Cause**: Invalid AWS region specified

**Solution**: Check region name in configuration matches AWS region codes
(e.g., `us-east-1`, `eu-west-1`, `ap-southeast-1`)

### Issue: Tables not created automatically

**Cause**: Missing `dynamodb:CreateTable` permission

**Solution**: Add CreateTable permission to IAM policy, or manually create tables:

```bash
# Create bookkeeping table
aws dynamodb create-table \
  --table-name pramen_production_bookkeeping \
  --attribute-definitions \
    AttributeName=tableName,AttributeType=S \
    AttributeName=infoDate,AttributeType=S \
  --key-schema \
    AttributeName=tableName,KeyType=HASH \
    AttributeName=infoDate,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Create schema table
aws dynamodb create-table \
  --table-name pramen_production_schemas \
  --attribute-definitions \
    AttributeName=tableName,AttributeType=S \
    AttributeName=infoDate,AttributeType=S \
  --key-schema \
    AttributeName=tableName,KeyType=HASH \
    AttributeName=infoDate,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### Issue: Slow queries

**Cause**: Large number of bookkeeping records

**Solution**:
1. Use date range filters in queries
2. Consider implementing table retention policy
3. Archive old data to S3

## Comparison with Other Bookkeeping Options

| Feature | DynamoDB | JDBC | MongoDB | Hadoop/Delta |
|---------|----------|------|---------|--------------|
| Setup Complexity | Low | Medium | Medium | Low |
| Maintenance | None | High | Medium | Low |
| Cost (small) | Very Low | Medium | Medium | Very Low |
| Cost (large) | Medium | High | Medium | Low |
| Scaling | Automatic | Manual | Manual | Automatic |
| Multi-region | Yes | No | Yes | Yes |
| Query Performance | Fast | Fast | Fast | Slower |
| Incremental Support | No* | Yes | No | No |

*Note: Offset management for incremental pipelines is not yet implemented for DynamoDB bookkeeper.

## Advanced Topics

### Using DynamoDB Local for Development

For local development/testing, use DynamoDB Local:

1. **Start DynamoDB Local**:
   ```bash
   docker run -p 8000:8000 amazon/dynamodb-local
   ```

2. **Configure endpoint** (requires code modification):
   ```scala
   val client = DynamoDbClient.builder()
     .endpointOverride(new URI("http://localhost:8000"))
     .region(Region.US_EAST_1)
     .build()
   ```

### Table Backup and Restore

Use AWS Backup or DynamoDB point-in-time recovery:

```bash
# Enable point-in-time recovery
aws dynamodb update-continuous-backups \
  --table-name pramen_production_bookkeeping \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

### Monitoring

Monitor DynamoDB metrics in CloudWatch:
- `UserErrors` - Check for configuration issues
- `ConsumedReadCapacityUnits` / `ConsumedWriteCapacityUnits` - Monitor costs
- `SystemErrors` - Check for service issues

## References

- [AWS DynamoDB Documentation](https://docs.aws.amazon.com/dynamodb/)
- [AWS SDK for Java Documentation](https://docs.aws.amazon.com/sdk-for-java/)
- [DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
