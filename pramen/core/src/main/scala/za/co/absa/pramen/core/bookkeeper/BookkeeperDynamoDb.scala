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

package za.co.absa.pramen.core.bookkeeper

import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.core.bookkeeper.model.DataAvailability
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.utils.{AlgorithmUtils, TimeUtils}

import java.net.URI
import java.time.LocalDate
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * DynamoDB-based implementation of the Bookkeeper.
  *
  * Table schema for bookkeeping:
  * - Partition key: tableName (String)
  * - Sort key: infoDate (String in yyyy-MM-dd format)
  *
  * Table schema for schemas:
  * - Partition key: tableName (String)
  * - Sort key: infoDate (String in yyyy-MM-dd format)
  *
  * @param dynamoDbClient The DynamoDB client to use for operations
  * @param batchId The batch ID for this execution
  * @param tableArn Optional ARN prefix for DynamoDB tables (e.g., "arn:aws:dynamodb:region:account-id:table/")
  * @param tablePrefix Prefix for table names to allow multiple bookkeeping sets in the same account (default: "pramen")
  */
class BookkeeperDynamoDb(
    dynamoDbClient: DynamoDbClient,
    batchId: Long,
    tableArn: Option[String] = None,
    tablePrefix: String = BookkeeperDynamoDb.DEFAULT_TABLE_PREFIX
) extends BookkeeperBase(isBookkeepingEnabled = true, batchId) {

  import BookkeeperDynamoDb._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val queryWarningTimeoutMs = 10000L

  // Construct table names with prefix
  private val bookkeepingTableBaseName = s"${tablePrefix}_$DEFAULT_BOOKKEEPING_TABLE"
  private val schemaTableBaseName = s"${tablePrefix}_$DEFAULT_SCHEMA_TABLE"

  // Full table names/ARNs
  private val bookkeepingTableName = getFullTableName(tableArn, bookkeepingTableBaseName)
  private val schemaTableName = getFullTableName(tableArn, schemaTableBaseName)

  // Offset management
  private val offsetManagement = new OffsetManagerCached(
    new OffsetManagerDynamoDb(dynamoDbClient, batchId, tableArn, tablePrefix)
  )

  // Initialize tables on construction
  init()

  override val bookkeepingEnabled: Boolean = true

  /**
    * Initializes the DynamoDB tables for bookkeeping and schemas.
    * Checks if tables exist and creates them if they don't.
    */
  def init(): Unit = {
    try {
      log.info(s"Initializing DynamoDB bookkeeper with tables: bookkeeping='$bookkeepingTableName', schemas='$schemaTableName'")

      // Initialize bookkeeping table
      if (!tableExists(bookkeepingTableBaseName)) {
        log.info(s"Creating DynamoDB bookkeeping table: $bookkeepingTableBaseName")
        createBookkeepingTable(bookkeepingTableBaseName)
        log.info(s"Successfully created bookkeeping table: $bookkeepingTableBaseName")
      } else {
        log.info(s"DynamoDB bookkeeping table already exists: $bookkeepingTableBaseName")
      }

      // Initialize schema table
      if (!tableExists(schemaTableBaseName)) {
        log.info(s"Creating DynamoDB schema table: $schemaTableBaseName")
        createSchemaTable(schemaTableBaseName)
        log.info(s"Successfully created schema table: $schemaTableBaseName")
      } else {
        log.info(s"DynamoDB schema table already exists: $schemaTableBaseName")
      }

      log.info(s"DynamoDB bookkeeper initialization complete")
    } catch {
      case NonFatal(ex) =>
        log.error("Error initializing DynamoDB bookkeeper tables", ex)
        throw new RuntimeException("Failed to initialize DynamoDB bookkeeper", ex)
    }
  }

  /**
    * Checks if a DynamoDB table exists.
    *
    * @param tableName The name of the table to check
    * @return true if the table exists, false otherwise
    */
  private def tableExists(tableName: String): Boolean = {
    try {
      val describeRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build()

      dynamoDbClient.describeTable(describeRequest)
      true
    } catch {
      case _: ResourceNotFoundException => false
      case NonFatal(ex) =>
        log.warn(s"Error checking if table exists: $tableName", ex)
        throw ex
    }
  }

  /**
    * Creates the bookkeeping table with the appropriate schema.
    *
    * @param tableName The name of the table to create
    */
  private def createBookkeepingTable(tableName: String): Unit = {
    val createTableRequest = CreateTableRequest.builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(ATTR_TABLE_NAME)
          .keyType(KeyType.HASH)
          .build(),
        KeySchemaElement.builder()
          .attributeName(ATTR_INFO_DATE)
          .keyType(KeyType.RANGE)
          .build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(ATTR_TABLE_NAME)
          .attributeType(ScalarAttributeType.S)
          .build(),
        AttributeDefinition.builder()
          .attributeName(ATTR_INFO_DATE)
          .attributeType(ScalarAttributeType.S)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST) // On-demand billing
      .build()

    dynamoDbClient.createTable(createTableRequest)

    // Wait for table to become active
    waitForTableActive(tableName)
  }

  /**
    * Creates the schema table with the appropriate schema.
    *
    * @param tableName The name of the table to create
    */
  private def createSchemaTable(tableName: String): Unit = {
    val createTableRequest = CreateTableRequest.builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(ATTR_TABLE_NAME)
          .keyType(KeyType.HASH)
          .build(),
        KeySchemaElement.builder()
          .attributeName(ATTR_INFO_DATE)
          .keyType(KeyType.RANGE)
          .build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(ATTR_TABLE_NAME)
          .attributeType(ScalarAttributeType.S)
          .build(),
        AttributeDefinition.builder()
          .attributeName(ATTR_INFO_DATE)
          .attributeType(ScalarAttributeType.S)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST) // On-demand billing
      .build()

    dynamoDbClient.createTable(createTableRequest)

    // Wait for table to become active
    waitForTableActive(tableName)
  }

  /**
    * Waits for a table to become active after creation.
    *
    * @param tableName The name of the table to wait for
    * @param maxWaitSeconds Maximum time to wait in seconds (default: 60)
    */
  private def waitForTableActive(tableName: String, maxWaitSeconds: Int = 60): Unit = {
    val startTime = System.currentTimeMillis()
    val maxWaitMs = maxWaitSeconds * 1000L

    var tableActive = false
    while (!tableActive && (System.currentTimeMillis() - startTime) < maxWaitMs) {
      try {
        val describeRequest = DescribeTableRequest.builder()
          .tableName(tableName)
          .build()

        val response = dynamoDbClient.describeTable(describeRequest)
        val status = response.table().tableStatus()

        if (status == TableStatus.ACTIVE) {
          tableActive = true
          log.debug(s"Table $tableName is now ACTIVE")
        } else {
          log.debug(s"Table $tableName status: $status, waiting...")
          Thread.sleep(2000) // Wait 2 seconds before checking again
        }
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Error checking table status for $tableName", ex)
          Thread.sleep(2000)
      }
    }

    if (!tableActive) {
      throw new RuntimeException(s"Table $tableName did not become active within $maxWaitSeconds seconds")
    }
  }

  override def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate]): Option[LocalDate] = {
    try {
      val queryBuilder = QueryRequest.builder()
        .tableName(bookkeepingTableName)
        .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName")
        .expressionAttributeValues(Map(
          ":tableName" -> AttributeValue.builder().s(table).build()
        ).asJava)
        .scanIndexForward(false) // descending order

      val query = until match {
        case Some(endDate) =>
          val endDateStr = getDateStr(endDate)
          queryBuilder
            .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE <= :endDate")
            .expressionAttributeValues(Map(
              ":tableName" -> AttributeValue.builder().s(table).build(),
              ":endDate" -> AttributeValue.builder().s(endDateStr).build()
            ).asJava)
        case None =>
          queryBuilder
      }

      val response = dynamoDbClient.query(query.build())
      val items = response.items().asScala

      if (items.isEmpty) {
        None
      } else {
        // Find the maximum infoDateEnd
        val latestDate = items
          .map(item => LocalDate.parse(item.get(ATTR_INFO_DATE_END).s()))
          .maxBy(_.toEpochDay)
        Some(latestDate)
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error querying latest processed date for table '$table'", ex)
        throw ex
    }
  }

  override def getLatestDataChunkFromStorage(table: String, infoDate: LocalDate): Option[DataChunk] = {
    try {
      val dateStr = getDateStr(infoDate)

      val queryRequest = QueryRequest.builder()
        .tableName(bookkeepingTableName)
        .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE = :infoDate")
        .expressionAttributeValues(Map(
          ":tableName" -> AttributeValue.builder().s(table).build(),
          ":infoDate" -> AttributeValue.builder().s(dateStr).build()
        ).asJava)
        .build()

      val response = dynamoDbClient.query(queryRequest)
      val items = response.items().asScala

      if (items.isEmpty) {
        None
      } else {
        // Sort by jobFinished descending and take the first
        items
          .map(itemToDataChunk)
          .sortBy(-_.jobFinished)
          .headOption
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error getting latest data chunk for table '$table' at $infoDate", ex)
        throw ex
    }
  }

  override def getDataChunksFromStorage(table: String, infoDate: LocalDate, batchIdFilter: Option[Long]): Seq[DataChunk] = {
    try {
      val dateStr = getDateStr(infoDate)

      val queryBuilder = QueryRequest.builder()
        .tableName(bookkeepingTableName)
        .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE = :infoDate")
        .expressionAttributeValues(Map(
          ":tableName" -> AttributeValue.builder().s(table).build(),
          ":infoDate" -> AttributeValue.builder().s(dateStr).build()
        ).asJava)

      val query = batchIdFilter match {
        case Some(bId) =>
          queryBuilder
            .filterExpression(s"$ATTR_BATCH_ID = :batchId")
            .expressionAttributeValues(Map(
              ":tableName" -> AttributeValue.builder().s(table).build(),
              ":infoDate" -> AttributeValue.builder().s(dateStr).build(),
              ":batchId" -> AttributeValue.builder().n(bId.toString).build()
            ).asJava)
        case None =>
          queryBuilder
      }

      val response = dynamoDbClient.query(query.build())
      val chunks = response.items().asScala
        .map(itemToDataChunk)
        .sortBy(_.jobFinished)
        .toSeq

      log.debug(s"For $table ($infoDate) : ${chunks.mkString("[ ", ", ", " ]")}")
      chunks
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error getting data chunks for table '$table' at $infoDate", ex)
        throw ex
    }
  }

  override def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    try {
      var count = 0L
      var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null

      do {
        val queryBuilder = buildQueryForDateRange(table, dateBeginOpt, dateEndOpt)
          .select(Select.COUNT)

        if (lastEvaluatedKey != null) {
          queryBuilder.exclusiveStartKey(lastEvaluatedKey)
        }

        val response = dynamoDbClient.query(queryBuilder.build())
        count += response.count()
        lastEvaluatedKey = response.lastEvaluatedKey()
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

      count
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error counting data chunks for table '$table'", ex)
        throw ex
    }
  }

  override def getDataAvailabilityFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability] = {
    try {
      val allChunks = getAllChunksInDateRange(table, dateBegin, dateEnd)

      // Group by infoDate and aggregate
      val grouped = allChunks.groupBy(_.infoDate)
      val availability = grouped.map { case (dateStr, chunks) =>
        val date = LocalDate.parse(dateStr)
        val totalRecords = chunks.map(_.outputRecordCount).sum
        DataAvailability(date, chunks.length, totalRecords)
      }.toSeq.sortBy(_.infoDate.toEpochDay)

      availability
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error getting data availability for table '$table'", ex)
        throw ex
    }
  }

  override def saveRecordCountToStorage(
      table: String,
      infoDate: LocalDate,
      inputRecordCount: Long,
      outputRecordCount: Long,
      recordsAppended: Option[Long],
      jobStarted: Long,
      jobFinished: Long
  ): Unit = {
    try {
      val dateStr = getDateStr(infoDate)
      val item = dataChunkToItem(
        DataChunk(table, dateStr, dateStr, dateStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished, Some(batchId), recordsAppended)
      )

      val putRequest = PutItemRequest.builder()
        .tableName(bookkeepingTableName)
        .item(item)
        .build()

      dynamoDbClient.putItem(putRequest)
      log.debug(s"Saved bookkeeping record for table '$table', infoDate='$dateStr', batchId=$batchId")
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error saving record count for table '$table' at $infoDate", ex)
        throw ex
    }
  }

  override def deleteNonCurrentBatchRecords(table: String, infoDate: LocalDate): Unit = {
    try {
      val dateStr = getDateStr(infoDate)

      AlgorithmUtils.runActionWithElapsedTimeEvent(queryWarningTimeoutMs) {
        // Query all items for this table and date
        val queryRequest = QueryRequest.builder()
          .tableName(bookkeepingTableName)
          .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE = :infoDate")
          .expressionAttributeValues(Map(
            ":tableName" -> AttributeValue.builder().s(table).build(),
            ":infoDate" -> AttributeValue.builder().s(dateStr).build()
          ).asJava)
          .build()

        val response = dynamoDbClient.query(queryRequest)
        val items = response.items().asScala

        // Filter and delete items with different batchId
        items.foreach { item =>
          val itemBatchId = Option(item.get(ATTR_BATCH_ID)).flatMap(av =>
            if (av.n() != null) Some(av.n().toLong) else None
          )

          if (itemBatchId.exists(_ != batchId)) {
            val deleteRequest = DeleteItemRequest.builder()
              .tableName(bookkeepingTableName)
              .key(Map(
                ATTR_TABLE_NAME -> AttributeValue.builder().s(table).build(),
                ATTR_INFO_DATE -> AttributeValue.builder().s(dateStr).build()
              ).asJava)
              .conditionExpression(s"$ATTR_JOB_FINISHED = :jobFinished")
              .expressionAttributeValues(Map(
                ":jobFinished" -> item.get(ATTR_JOB_FINISHED)
              ).asJava)
              .build()

            try {
              dynamoDbClient.deleteItem(deleteRequest)
            } catch {
              case _: ConditionalCheckFailedException =>
                // Item was already modified or deleted, ignore
                log.debug(s"Could not delete item for table '$table', date '$dateStr' - already modified")
            }
          }
        }
      } { actualTimeMs =>
        val elapsedTime = TimeUtils.prettyPrintElapsedTimeShort(actualTimeMs)
        log.warn(s"DynamoDB query took too long ($elapsedTime) while deleting from $bookkeepingTableName, tableName='$table', infoDate='$infoDate', batchId!=$batchId")
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error deleting non-current batch records for table '$table' at $infoDate", ex)
        throw ex
    }
  }

  override def deleteTable(tableWithWildcard: String): Seq[String] = {
    // DynamoDB implementation for wildcard deletion
    // This would require scanning and deleting matching items
    throw new UnsupportedOperationException("deleteTable with wildcards is not yet implemented for DynamoDB bookkeeper")
  }

  override def getLatestSchema(tableName: String, until: LocalDate): Option[(StructType, LocalDate)] = {
    try {
      val untilDateStr = until.toString

      val queryRequest = QueryRequest.builder()
        .tableName(schemaTableName)
        .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE <= :untilDate")
        .expressionAttributeValues(Map(
          ":tableName" -> AttributeValue.builder().s(tableName).build(),
          ":untilDate" -> AttributeValue.builder().s(untilDateStr).build()
        ).asJava)
        .scanIndexForward(false) // descending order
        .limit(1)
        .build()

      val response = dynamoDbClient.query(queryRequest)
      val items = response.items().asScala

      items.headOption.flatMap { item =>
        val tableSchema = TableSchema(
          tableName = item.get(ATTR_TABLE_NAME).s(),
          infoDate = item.get(ATTR_INFO_DATE).s(),
          schemaJson = item.get(ATTR_SCHEMA_JSON).s()
        )
        TableSchema.toSchemaAndDate(tableSchema)
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error getting latest schema for table '$tableName' until $until", ex)
        throw ex
    }
  }

  private[pramen] override def saveSchema(tableName: String, infoDate: LocalDate, schema: StructType): Unit = {
    try {
      val item = Map(
        ATTR_TABLE_NAME -> AttributeValue.builder().s(tableName).build(),
        ATTR_INFO_DATE -> AttributeValue.builder().s(infoDate.toString).build(),
        ATTR_SCHEMA_JSON -> AttributeValue.builder().s(schema.json).build()
      ).asJava

      val putRequest = PutItemRequest.builder()
        .tableName(schemaTableName)
        .item(item)
        .build()

      dynamoDbClient.putItem(putRequest)
      log.debug(s"Saved schema for table '$tableName', infoDate='$infoDate'")
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error saving schema for table '$tableName' at $infoDate", ex)
        throw ex
    }
  }

  private[pramen] override def getOffsetManager: OffsetManager = {
    offsetManagement
  }

  override def close(): Unit = {
    try {
      // Note: offsetManagement wraps OffsetManagerDynamoDb which shares the same dynamoDbClient,
      // so we don't need to close it separately
      dynamoDbClient.close()
    } catch {
      case NonFatal(ex) =>
        log.warn("Error closing DynamoDB client", ex)
    }
  }

  private def itemToDataChunk(item: java.util.Map[String, AttributeValue]): DataChunk = {
    DataChunk(
      tableName = item.get(ATTR_TABLE_NAME).s(),
      infoDate = item.get(ATTR_INFO_DATE).s(),
      infoDateBegin = item.get(ATTR_INFO_DATE_BEGIN).s(),
      infoDateEnd = item.get(ATTR_INFO_DATE_END).s(),
      inputRecordCount = item.get(ATTR_INPUT_RECORD_COUNT).n().toLong,
      outputRecordCount = item.get(ATTR_OUTPUT_RECORD_COUNT).n().toLong,
      jobStarted = item.get(ATTR_JOB_STARTED).n().toLong,
      jobFinished = item.get(ATTR_JOB_FINISHED).n().toLong,
      batchId = Option(item.get(ATTR_BATCH_ID)).flatMap(av => if (av.n() != null) Some(av.n().toLong) else None),
      appendedRecordCount = Option(item.get(ATTR_APPENDED_RECORD_COUNT)).flatMap(av => if (av.n() != null) Some(av.n().toLong) else None)
    )
  }

  private def dataChunkToItem(chunk: DataChunk): java.util.Map[String, AttributeValue] = {
    val baseMap = Map(
      ATTR_TABLE_NAME -> AttributeValue.builder().s(chunk.tableName).build(),
      ATTR_INFO_DATE -> AttributeValue.builder().s(chunk.infoDate).build(),
      ATTR_INFO_DATE_BEGIN -> AttributeValue.builder().s(chunk.infoDateBegin).build(),
      ATTR_INFO_DATE_END -> AttributeValue.builder().s(chunk.infoDateEnd).build(),
      ATTR_INPUT_RECORD_COUNT -> AttributeValue.builder().n(chunk.inputRecordCount.toString).build(),
      ATTR_OUTPUT_RECORD_COUNT -> AttributeValue.builder().n(chunk.outputRecordCount.toString).build(),
      ATTR_JOB_STARTED -> AttributeValue.builder().n(chunk.jobStarted.toString).build(),
      ATTR_JOB_FINISHED -> AttributeValue.builder().n(chunk.jobFinished.toString).build()
    )

    val withBatchId = chunk.batchId match {
      case Some(bid) => baseMap + (ATTR_BATCH_ID -> AttributeValue.builder().n(bid.toString).build())
      case None => baseMap
    }

    val withAppendedCount = chunk.appendedRecordCount match {
      case Some(count) => withBatchId + (ATTR_APPENDED_RECORD_COUNT -> AttributeValue.builder().n(count.toString).build())
      case None => withBatchId
    }

    withAppendedCount.asJava
  }

  private def buildQueryForDateRange(
      table: String,
      dateBeginOpt: Option[LocalDate],
      dateEndOpt: Option[LocalDate]
  ): QueryRequest.Builder = {
    val builder = QueryRequest.builder()
      .tableName(bookkeepingTableName)

    (dateBeginOpt, dateEndOpt) match {
      case (Some(begin), Some(end)) =>
        val beginStr = getDateStr(begin)
        val endStr = getDateStr(end)
        builder
          .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE BETWEEN :beginDate AND :endDate")
          .expressionAttributeValues(Map(
            ":tableName" -> AttributeValue.builder().s(table).build(),
            ":beginDate" -> AttributeValue.builder().s(beginStr).build(),
            ":endDate" -> AttributeValue.builder().s(endStr).build()
          ).asJava)
      case (Some(begin), None) =>
        val beginStr = getDateStr(begin)
        builder
          .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE >= :beginDate")
          .expressionAttributeValues(Map(
            ":tableName" -> AttributeValue.builder().s(table).build(),
            ":beginDate" -> AttributeValue.builder().s(beginStr).build()
          ).asJava)
      case (None, Some(end)) =>
        val endStr = getDateStr(end)
        builder
          .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName AND $ATTR_INFO_DATE <= :endDate")
          .expressionAttributeValues(Map(
            ":tableName" -> AttributeValue.builder().s(table).build(),
            ":endDate" -> AttributeValue.builder().s(endStr).build()
          ).asJava)
      case (None, None) =>
        builder
          .keyConditionExpression(s"$ATTR_TABLE_NAME = :tableName")
          .expressionAttributeValues(Map(
            ":tableName" -> AttributeValue.builder().s(table).build()
          ).asJava)
    }
  }

  private def getAllChunksInDateRange(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = {
    val chunks = scala.collection.mutable.ListBuffer[DataChunk]()
    var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null

    do {
      val queryBuilder = buildQueryForDateRange(table, Some(dateBegin), Some(dateEnd))

      if (lastEvaluatedKey != null) {
        queryBuilder.exclusiveStartKey(lastEvaluatedKey)
      }

      val response = dynamoDbClient.query(queryBuilder.build())
      chunks ++= response.items().asScala.map(itemToDataChunk)
      lastEvaluatedKey = response.lastEvaluatedKey()
    } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

    chunks.toSeq
  }
}

object BookkeeperDynamoDb {
  val DEFAULT_BOOKKEEPING_TABLE = "bookkeeping"
  val DEFAULT_SCHEMA_TABLE = "schemas"
  val DEFAULT_TABLE_PREFIX = "pramen"

  // Attribute names for bookkeeping table
  val ATTR_TABLE_NAME = "tableName"
  val ATTR_INFO_DATE = "infoDate"
  val ATTR_INFO_DATE_BEGIN = "infoDateBegin"
  val ATTR_INFO_DATE_END = "infoDateEnd"
  val ATTR_INPUT_RECORD_COUNT = "inputRecordCount"
  val ATTR_OUTPUT_RECORD_COUNT = "outputRecordCount"
  val ATTR_JOB_STARTED = "jobStarted"
  val ATTR_JOB_FINISHED = "jobFinished"
  val ATTR_BATCH_ID = "batchId"
  val ATTR_APPENDED_RECORD_COUNT = "appendedRecordCount"

  // Attribute names for schema table
  val ATTR_SCHEMA_JSON = "schemaJson"

  val MODEL_VERSION = 1

  /**
    * Builder for creating BookkeeperDynamoDb instances.
    * Provides a fluent API for configuring DynamoDB bookkeeper.
    *
    * Example:
    * {{{
    * val bookkeeper = BookkeeperDynamoDb.builder
    *   .withRegion("us-east-1")
    *   .withBatchId(System.currentTimeMillis())
    *   .withTablePrefix("my_app")
    *   .build()
    * }}}
    */
  class BookkeeperDynamoDbBuilder {
    private var region: Option[String] = None
    private var batchId: Option[Long] = None
    private var tableArn: Option[String] = None
    private var tablePrefix: String = DEFAULT_TABLE_PREFIX
    private var credentialsProvider: Option[AwsCredentialsProvider] = None
    private var endpoint: Option[String] = None

    /**
      * Sets the AWS region for the DynamoDB client.
      *
      * @param region AWS region (e.g., "us-east-1", "eu-west-1")
      * @return this builder
      */
    def withRegion(region: String): BookkeeperDynamoDbBuilder = {
      this.region = Some(region)
      this
    }

    /**
      * Sets the batch ID for this bookkeeper instance.
      *
      * @param batchId Batch ID (typically timestamp in milliseconds)
      * @return this builder
      */
    def withBatchId(batchId: Long): BookkeeperDynamoDbBuilder = {
      this.batchId = Some(batchId)
      this
    }

    /**
      * Sets the table ARN prefix for cross-account or cross-region access.
      *
      * @param arn ARN prefix (e.g., "arn:aws:dynamodb:us-east-1:123456789012:table/")
      * @return this builder
      */
    def withTableArn(arn: String): BookkeeperDynamoDbBuilder = {
      this.tableArn = Some(arn)
      this
    }

    /**
      * Sets the table ARN prefix for cross-account or cross-region access.
      *
      * @param arnOpt ARN prefix (e.g., "arn:aws:dynamodb:us-east-1:123456789012:table/")
      * @return this builder
      */
    def withTableArn(arnOpt: Option[String]): BookkeeperDynamoDbBuilder = {
      this.tableArn = arnOpt
      this
    }

    /**
      * Sets the table name prefix to allow multiple bookkeeping sets in the same account.
      *
      * @param prefix Table name prefix (default: "pramen")
      * @return this builder
      */
    def withTablePrefix(prefix: String): BookkeeperDynamoDbBuilder = {
      this.tablePrefix = prefix
      this
    }

    /**
      * Sets custom AWS credentials provider.
      *
      * @param provider AWS credentials provider
      * @return this builder
      */
    def withCredentialsProvider(provider: AwsCredentialsProvider): BookkeeperDynamoDbBuilder = {
      this.credentialsProvider = Some(provider)
      this
    }

    /**
      * Sets a custom DynamoDB endpoint (useful for testing with LocalStack or DynamoDB Local).
      *
      * @param endpoint Endpoint URI (e.g., "http://localhost:8000")
      * @return this builder
      */
    def withEndpoint(endpoint: String): BookkeeperDynamoDbBuilder = {
      this.endpoint = Some(endpoint)
      this
    }

    /**
      * Builds the BookkeeperDynamoDb instance.
      *
      * @return Configured BookkeeperDynamoDb instance
      * @throws IllegalArgumentException if required parameters are missing
      */
    def build(): BookkeeperDynamoDb = {
      val actualBatchId = batchId.getOrElse(System.currentTimeMillis())

      if (region.isEmpty) {
        throw new IllegalArgumentException("Either region or dynamoDbClient must be provided")
      }

      val clientBuilder = DynamoDbClient.builder()
        .region(Region.of(region.get))

      credentialsProvider.foreach(clientBuilder.credentialsProvider)

      endpoint.foreach { ep =>
        clientBuilder.endpointOverride(URI.create(ep))
      }

      val client = clientBuilder.build()

      new BookkeeperDynamoDb(
        dynamoDbClient = client,
        batchId = actualBatchId,
        tableArn = tableArn,
        tablePrefix = tablePrefix
      )
    }
  }

  def builder: BookkeeperDynamoDbBuilder = new BookkeeperDynamoDbBuilder

  /**
    * Constructs the full table name using ARN prefix and table name.
    * If tableArn is provided, uses it as a prefix, otherwise returns just the table name.
    *
    * @param tableArn Optional ARN prefix for the table
    * @param tableName The table name
    * @return Full table name or ARN
    */
  def getFullTableName(tableArn: Option[String], tableName: String): String = {
    tableArn match {
      case Some(arn) if arn.nonEmpty =>
        // If ARN ends with table/, append the table name, otherwise append /table/tableName
        if (arn.endsWith("/")) {
          s"${arn}table/$tableName"
        } else if (arn.contains("/table/")) {
          arn // ARN already includes table path
        } else {
          s"$arn/table/$tableName"
        }
      case _ => tableName
    }
  }
}
