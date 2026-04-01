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
- `pramen_bookkeeping` - Data availability and record counts
- `pramen_schemas` - Table schema evolution
- `pramen_locks` - Distributed locking (if locks enabled)
- `pramen_journal` - Task completion history
- `pramen_metadata` - Custom metadata key-value pairs
- `pramen_offsets` - Incremental ingestion offset tracking

### Production Configuration

```hocon
pramen.bookkeeping {
  enabled = true
  dynamodb.region = "us-east-1"
  dynamodb.table.prefix = "pramen_production"
}
```

This creates tables:
- `pramen_production_bookkeeping` - Data availability and record counts
- `pramen_production_schemas` - Table schema evolution
- `pramen_production_locks` - Distributed locking (if locks enabled)
- `pramen_production_journal` - Task completion history
- `pramen_production_metadata` - Custom metadata key-value pairs
- `pramen_production_offsets` - Incremental ingestion offset tracking

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

#### Offset Table (`{prefix}_offsets`)
- **Partition Key**: `pramenTableName` (String)
- **Sort Key**: `compositeKey` (String, format: "infoDate#createdAtMilli")
- **Billing Mode**: PAY_PER_REQUEST (on-demand)
- **Attributes**:
  - `pramenTableName`: Name of the metastore table
  - `compositeKey`: Composite key for efficient querying (infoDate#createdAtMilli)
  - `infoDate`: Information date
  - `dataType`: Offset data type (e.g., "IntegralType", "StringType")
  - `minOffset`: Minimum offset value for this batch
  - `maxOffset`: Maximum offset value for this batch
  - `batchId`: Batch execution ID
  - `createdAt`: Timestamp when offset was created (milliseconds)
  - `committedAt`: Timestamp when offset was committed (milliseconds, optional)

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
   - `pramen_production_offsets` (if using incremental ingestion)
   - `pramen_production_locks` (if locks enabled)
   - `pramen_production_journal` (if journal enabled)
   - `pramen_production_metadata` (if metadata enabled)

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
| Incremental Support | Yes | Yes | No | No |

## Distributed Locking with DynamoDB

When DynamoDB is configured for bookkeeping, Pramen automatically uses it for distributed locking to prevent concurrent pipeline runs. This ensures data consistency in multi-instance deployments.

### How It Works

1. **Automatic Lock Table Creation**: A locks table is created automatically using a builder pattern:
   - Table name: `{prefix}_locks` (e.g., `pramen_production_locks`)
   - Schema: `token` (partition key), `owner`, `expires`, `createdAt`
   - Created via `TokenLockFactoryDynamoDb.builder`

2. **Lock Acquisition**: Uses DynamoDB conditional writes (`attribute_not_exists`) for atomic lock operations

3. **Lock Renewal**: Active pipelines automatically renew their locks every 2 minutes

4. **Lock Expiration**: Locks expire after 10 minutes of inactivity and can be taken over

5. **Hard Expiration**: Stale locks are cleaned up after 1 day

6. **Builder Pattern**: Lock factory is created using a fluent builder API for flexible configuration

### Configuration

Enable locking along with DynamoDB bookkeeping:

```hocon
pramen {
  # Enable distributed locking
  runtime.use.locks = true

  bookkeeping {
    enabled = true
    dynamodb.region = "us-east-1"
    dynamodb.table.prefix = "pramen_production"
  }
}
```

This creates six tables:
- `pramen_production_bookkeeping` - Bookkeeping data
- `pramen_production_schemas` - Table schemas
- `pramen_production_locks` - Distributed locks
- `pramen_production_journal` - Task completion history
- `pramen_production_metadata` - Custom metadata
- `pramen_production_offsets` - Incremental ingestion offsets

See `dynamodb_with_locks.conf` for a complete example.

### IAM Permissions for Locks

Add the locks table to your IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable",
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:DeleteItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/pramen_production_bookkeeping",
        "arn:aws:dynamodb:*:*:table/pramen_production_schemas",
        "arn:aws:dynamodb:*:*:table/pramen_production_locks",
        "arn:aws:dynamodb:*:*:table/pramen_production_journal",
        "arn:aws:dynamodb:*:*:table/pramen_production_metadata",
        "arn:aws:dynamodb:*:*:table/pramen_production_offsets"
      ]
    }
  ]
}
```

### Programmatic Usage

You can also create lock factories programmatically using the builder pattern:

```scala
import za.co.absa.pramen.core.lock.TokenLockFactoryDynamoDb

// Basic usage
val lockFactory = TokenLockFactoryDynamoDb.builder
  .withRegion("us-east-1")
  .withTablePrefix("my_app")
  .build()

try {
  val lock = lockFactory.getLock("my_pipeline")

  if (lock.tryAcquire()) {
    try {
      // Run your pipeline
    } finally {
      lock.release()
    }
  }
} finally {
  lockFactory.close()
}

// Testing with DynamoDB Local
val testFactory = TokenLockFactoryDynamoDb.builder
  .withRegion("us-east-1")
  .withEndpoint("http://localhost:8000")
  .build()
```

See `core/src/main/scala/za/co/absa/pramen/core/lock/TokenLockFactoryDynamoDbExample.scala` for more examples.

### Lock Behavior

**Scenario 1: Single Pipeline Run**
- Pipeline acquires lock → processes data → releases lock

**Scenario 2: Concurrent Pipeline Runs**
- Instance A acquires lock → starts processing
- Instance B tries to acquire same lock → blocked (lock already held)
- Instance A completes → releases lock
- Instance B can now acquire lock (if still attempting)

**Scenario 3: Pipeline Crash**
- Pipeline acquires lock → crashes
- Lock expires after 10 minutes (no renewal)
- New pipeline run can take over expired lock

### Monitoring Locks

Query active locks:

```bash
aws dynamodb scan \
  --table-name pramen_production_locks \
  --region us-east-1
```

Check specific lock:

```bash
aws dynamodb get-item \
  --table-name pramen_production_locks \
  --key '{"token":{"S":"my_pipeline_lock"}}' \
  --region us-east-1
```

Manually release stuck lock (use with caution):

```bash
aws dynamodb delete-item \
  --table-name pramen_production_locks \
  --key '{"token":{"S":"my_pipeline_lock"}}' \
  --region us-east-1
```

### Lock Cost

Lock operations add minimal cost:
- Lock acquisition: 1 write request (~$0.00000125)
- Lock renewal (every 2 min): 1 write request per renewal
- Lock release: 1 delete request (~$0.00000125)
- Total per pipeline run: ~$0.00001 (for 10-minute pipeline)

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
