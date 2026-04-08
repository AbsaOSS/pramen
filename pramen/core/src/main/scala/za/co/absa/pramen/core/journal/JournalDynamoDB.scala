/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.core.journal

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb.waitForTableActive
import za.co.absa.pramen.core.journal.model.TaskCompleted

import java.net.URI
import java.time.{Instant, LocalDate}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * DynamoDB-based journal for tracking completed tasks.
  *
  * This journal stores task completion records in a DynamoDB table with automatic table creation.
  *
  * @param dynamoDbClient The DynamoDB client to use
  * @param tableArn Optional ARN prefix for the journal table
  * @param tablePrefix Prefix for the journal table name (default: "pramen")
  */
class JournalDynamoDB private (
    dynamoDbClient: DynamoDbClient,
    tableArn: Option[String] = None,
    tablePrefix: String = JournalDynamoDB.DEFAULT_TABLE_PREFIX
) extends Journal {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = InfoDateConfig.defaultDateFormatter

  private val journalTableBaseName = s"${tablePrefix}_${JournalDynamoDB.DEFAULT_JOURNAL_TABLE}"
  private val journalTableFullName = BookkeeperDynamoDb.getFullTableName(tableArn, journalTableBaseName)

  // Initialize table on creation
  createJournalTableIfNotExists()

  /**
    * Add a task completion entry to the journal.
    * Failure reason is truncated to 4KB to fit DynamoDB item size limits.
    */
  override def addEntry(entry: TaskCompleted): Unit = {
    val periodBegin = entry.periodBegin.format(dateFormatter)
    val periodEnd = entry.periodEnd.format(dateFormatter)
    val infoDate = entry.informationDate.format(dateFormatter)

    // Truncate failure reason to 4KB maximum
    val truncatedFailureReason = entry.failureReason.map { reason =>
      if (reason.length > JournalDynamoDB.MAX_FAILURE_REASON_LENGTH) {
        val truncated = reason.substring(0, JournalDynamoDB.MAX_FAILURE_REASON_LENGTH - 20)
        truncated + "\n[... truncated ...]"
      } else {
        reason
      }
    }

    val itemBuilder = Map.newBuilder[String, AttributeValue]

    // Primary key: composite of jobName and finishedAt (for sorting by time)
    itemBuilder += (JournalDynamoDB.ATTR_JOB_NAME -> AttributeValue.builder().s(entry.jobName).build())
    itemBuilder += (JournalDynamoDB.ATTR_FINISHED_AT -> AttributeValue.builder().n(entry.finishedAt.toString).build())

    // Attributes
    itemBuilder += (JournalDynamoDB.ATTR_TABLE_NAME -> AttributeValue.builder().s(entry.tableName).build())
    itemBuilder += (JournalDynamoDB.ATTR_PERIOD_BEGIN -> AttributeValue.builder().s(periodBegin).build())
    itemBuilder += (JournalDynamoDB.ATTR_PERIOD_END -> AttributeValue.builder().s(periodEnd).build())
    itemBuilder += (JournalDynamoDB.ATTR_INFO_DATE -> AttributeValue.builder().s(infoDate).build())
    itemBuilder += (JournalDynamoDB.ATTR_INPUT_RECORD_COUNT -> AttributeValue.builder().n(entry.inputRecordCount.getOrElse(-1L).toString).build())
    itemBuilder += (JournalDynamoDB.ATTR_INPUT_RECORD_COUNT_OLD -> AttributeValue.builder().n(entry.inputRecordCountOld.getOrElse(-1L).toString).build())

    entry.outputRecordCount.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_OUTPUT_RECORD_COUNT -> AttributeValue.builder().n(v.toString).build()))
    entry.outputRecordCountOld.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_OUTPUT_RECORD_COUNT_OLD -> AttributeValue.builder().n(v.toString).build()))
    entry.appendedRecordCount.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_APPENDED_RECORD_COUNT -> AttributeValue.builder().n(v.toString).build()))
    entry.outputSize.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_OUTPUT_SIZE -> AttributeValue.builder().n(v.toString).build()))

    itemBuilder += (JournalDynamoDB.ATTR_STARTED_AT -> AttributeValue.builder().n(entry.startedAt.toString).build())
    itemBuilder += (JournalDynamoDB.ATTR_STATUS -> AttributeValue.builder().s(entry.status).build())

    truncatedFailureReason.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_FAILURE_REASON -> AttributeValue.builder().s(v).build()))
    entry.sparkApplicationId.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_SPARK_APP_ID -> AttributeValue.builder().s(v).build()))
    entry.pipelineId.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_PIPELINE_ID -> AttributeValue.builder().s(v).build()))
    entry.pipelineName.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_PIPELINE_NAME -> AttributeValue.builder().s(v).build()))
    entry.environmentName.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_ENVIRONMENT_NAME -> AttributeValue.builder().s(v).build()))
    entry.tenant.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_TENANT -> AttributeValue.builder().s(v).build()))
    entry.country.foreach(v => itemBuilder += (JournalDynamoDB.ATTR_COUNTRY -> AttributeValue.builder().s(v).build()))

    itemBuilder += (JournalDynamoDB.ATTR_BATCH_ID -> AttributeValue.builder().n(entry.batchId.toString).build())

    try {
      val putRequest = PutItemRequest.builder()
        .tableName(journalTableFullName)
        .item(itemBuilder.result().asJava)
        .build()

      dynamoDbClient.putItem(putRequest)
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to write to the journal table '$journalTableFullName'.", ex)
    }
  }

  /**
    * Get journal entries within a time range.
    */
  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {
    val fromSec = from.getEpochSecond
    val toSec = to.getEpochSecond

    try {
      val scanRequest = ScanRequest.builder()
        .tableName(journalTableFullName)
        .filterExpression(s"${JournalDynamoDB.ATTR_FINISHED_AT} >= :from_time AND ${JournalDynamoDB.ATTR_FINISHED_AT} <= :to_time")
        .expressionAttributeValues(Map(
          ":from_time" -> AttributeValue.builder().n(fromSec.toString).build(),
          ":to_time" -> AttributeValue.builder().n(toSec.toString).build()
        ).asJava)
        .build()

      val result = dynamoDbClient.scan(scanRequest)

      result.items().asScala.map { item =>
        val getS = (attr: String) => Option(item.get(attr)).map(_.s())
        val getN = (attr: String) => Option(item.get(attr)).map(_.n().toLong)

        val inputRecordCount = getN(JournalDynamoDB.ATTR_INPUT_RECORD_COUNT).flatMap(v => if (v < 0) None else Some(v))
        val inputRecordCountOld = getN(JournalDynamoDB.ATTR_INPUT_RECORD_COUNT_OLD).flatMap(v => if (v < 0) None else Some(v))

        TaskCompleted(
          jobName = item.get(JournalDynamoDB.ATTR_JOB_NAME).s(),
          tableName = item.get(JournalDynamoDB.ATTR_TABLE_NAME).s(),
          periodBegin = LocalDate.parse(item.get(JournalDynamoDB.ATTR_PERIOD_BEGIN).s(), dateFormatter),
          periodEnd = LocalDate.parse(item.get(JournalDynamoDB.ATTR_PERIOD_END).s(), dateFormatter),
          informationDate = LocalDate.parse(item.get(JournalDynamoDB.ATTR_INFO_DATE).s(), dateFormatter),
          inputRecordCount = inputRecordCount,
          inputRecordCountOld = inputRecordCountOld,
          outputRecordCount = getN(JournalDynamoDB.ATTR_OUTPUT_RECORD_COUNT),
          outputRecordCountOld = getN(JournalDynamoDB.ATTR_OUTPUT_RECORD_COUNT_OLD),
          appendedRecordCount = getN(JournalDynamoDB.ATTR_APPENDED_RECORD_COUNT),
          outputSize = getN(JournalDynamoDB.ATTR_OUTPUT_SIZE),
          startedAt = item.get(JournalDynamoDB.ATTR_STARTED_AT).n().toLong,
          finishedAt = item.get(JournalDynamoDB.ATTR_FINISHED_AT).n().toLong,
          status = item.get(JournalDynamoDB.ATTR_STATUS).s(),
          failureReason = getS(JournalDynamoDB.ATTR_FAILURE_REASON),
          sparkApplicationId = getS(JournalDynamoDB.ATTR_SPARK_APP_ID),
          pipelineId = getS(JournalDynamoDB.ATTR_PIPELINE_ID),
          pipelineName = getS(JournalDynamoDB.ATTR_PIPELINE_NAME),
          environmentName = getS(JournalDynamoDB.ATTR_ENVIRONMENT_NAME),
          tenant = getS(JournalDynamoDB.ATTR_TENANT),
          country = getS(JournalDynamoDB.ATTR_COUNTRY),
          batchId = getN(JournalDynamoDB.ATTR_BATCH_ID).getOrElse(0L)
        )
      }.toSeq
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to read from the journal table '$journalTableFullName'.", ex)
        Seq.empty
    }
  }

  /**
    * Creates the journal table if it doesn't exist.
    */
  private def createJournalTableIfNotExists(): Unit = {
    try {
      val describeRequest = DescribeTableRequest.builder()
        .tableName(journalTableFullName)
        .build()

      dynamoDbClient.describeTable(describeRequest)
      log.info(s"Journal table '$journalTableFullName' already exists")
    } catch {
      case _: ResourceNotFoundException =>
        log.info(s"Creating journal table '$journalTableFullName'")
        createJournalTable()
      case NonFatal(ex) =>
        log.error(s"Error checking if journal table exists", ex)
        throw ex
    }
  }

  /**
    * Creates the journal table in DynamoDB.
    */
  private def createJournalTable(): Unit = {
    val createRequest = CreateTableRequest.builder()
      .tableName(journalTableFullName)
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(JournalDynamoDB.ATTR_JOB_NAME)
          .attributeType(ScalarAttributeType.S)
          .build(),
        AttributeDefinition.builder()
          .attributeName(JournalDynamoDB.ATTR_FINISHED_AT)
          .attributeType(ScalarAttributeType.N)
          .build()
      )
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(JournalDynamoDB.ATTR_JOB_NAME)
          .keyType(KeyType.HASH)
          .build(),
        KeySchemaElement.builder()
          .attributeName(JournalDynamoDB.ATTR_FINISHED_AT)
          .keyType(KeyType.RANGE)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    dynamoDbClient.createTable(createRequest)
    waitForTableActive(journalTableFullName, dynamoDbClient)
    log.info(s"Journal table '$journalTableFullName' created successfully")
  }

  /**
    * Closes the DynamoDB client.
    */
  override def close(): Unit = {
    try {
      dynamoDbClient.close()
    } catch {
      case NonFatal(ex) =>
        log.warn("Error closing DynamoDB client", ex)
    }
  }
}

object JournalDynamoDB {
  val DEFAULT_JOURNAL_TABLE = "journal"
  val DEFAULT_TABLE_PREFIX = "pramen"

  // Maximum length for failure reason (4KB minus some overhead)
  val MAX_FAILURE_REASON_LENGTH = 4000

  // Attribute names for journal table
  val ATTR_JOB_NAME = "jobName"
  val ATTR_TABLE_NAME = "tableName"
  val ATTR_PERIOD_BEGIN = "periodBegin"
  val ATTR_PERIOD_END = "periodEnd"
  val ATTR_INFO_DATE = "infoDate"
  val ATTR_INPUT_RECORD_COUNT = "inputRecordCount"
  val ATTR_INPUT_RECORD_COUNT_OLD = "inputRecordCountOld"
  val ATTR_OUTPUT_RECORD_COUNT = "outputRecordCount"
  val ATTR_OUTPUT_RECORD_COUNT_OLD = "outputRecordCountOld"
  val ATTR_APPENDED_RECORD_COUNT = "appendedRecordCount"
  val ATTR_OUTPUT_SIZE = "outputSize"
  val ATTR_STARTED_AT = "startedAt"
  val ATTR_FINISHED_AT = "finishedAt"
  val ATTR_STATUS = "status"
  val ATTR_FAILURE_REASON = "failureReason"
  val ATTR_SPARK_APP_ID = "sparkApplicationId"
  val ATTR_PIPELINE_ID = "pipelineId"
  val ATTR_PIPELINE_NAME = "pipelineName"
  val ATTR_ENVIRONMENT_NAME = "environmentName"
  val ATTR_TENANT = "tenant"
  val ATTR_COUNTRY = "country"
  val ATTR_BATCH_ID = "batchId"

  /**
    * Builder for creating JournalDynamoDB instances.
    */
  class JournalDynamoDBBuilder {
    private var region: Option[String] = None
    private var tableArn: Option[String] = None
    private var tablePrefix: String = DEFAULT_TABLE_PREFIX
    private var credentialsProvider: Option[AwsCredentialsProvider] = None
    private var endpoint: Option[String] = None

    def withRegion(region: String): JournalDynamoDBBuilder = {
      this.region = Some(region)
      this
    }

    def withTableArn(arn: String): JournalDynamoDBBuilder = {
      this.tableArn = Some(arn)
      this
    }

    def withTableArn(arnOpt: Option[String]): JournalDynamoDBBuilder = {
      this.tableArn = arnOpt
      this
    }

    def withTablePrefix(prefix: String): JournalDynamoDBBuilder = {
      this.tablePrefix = prefix
      this
    }

    def withCredentialsProvider(provider: AwsCredentialsProvider): JournalDynamoDBBuilder = {
      this.credentialsProvider = Some(provider)
      this
    }

    def withEndpoint(endpoint: String): JournalDynamoDBBuilder = {
      this.endpoint = Some(endpoint)
      this
    }

    def build(): JournalDynamoDB = {
      if (region.isEmpty) {
        throw new IllegalArgumentException("Region must be provided")
      }

      val clientBuilder = DynamoDbClient.builder()
        .region(Region.of(region.get))

      credentialsProvider.foreach(clientBuilder.credentialsProvider)
      endpoint.foreach { ep =>
        clientBuilder.endpointOverride(URI.create(ep))
      }

      val client = clientBuilder.build()

      new JournalDynamoDB(
        dynamoDbClient = client,
        tableArn = tableArn,
        tablePrefix = tablePrefix
      )
    }
  }

  def builder: JournalDynamoDBBuilder = new JournalDynamoDBBuilder
}
