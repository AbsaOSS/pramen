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

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.api.offset.DataOffset.UncommittedOffset
import za.co.absa.pramen.api.offset.{DataOffset, OffsetType, OffsetValue}
import za.co.absa.pramen.core.bookkeeper.model._

import java.net.URI
import java.time.{Instant, LocalDate}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * DynamoDB-based offset manager for tracking incremental ingestion offsets.
  *
  * Table schema for offsets:
  * - Partition key: pramenTableName (String)
  * - Sort key: compositeKey (String) - format: "infoDate#createdAtMilli" for efficient querying
  *
  * The composite sort key allows:
  * 1. Efficient queries for all offsets of a table+infoDate combination
  * 2. Time-ordered offset records
  * 3. Support for aggregation queries (fetch all offsets for a table+date)
  *
  * @param dynamoDbClient The DynamoDB client to use
  * @param batchId The batch ID for this execution
  * @param tableArn Optional ARN prefix for the offset table
  * @param tablePrefix Prefix for the offset table name (default: "pramen")
  */
class OffsetManagerDynamoDb(
    dynamoDbClient: DynamoDbClient,
    batchId: Long,
    tableArn: Option[String] = None,
    tablePrefix: String = OffsetManagerDynamoDb.DEFAULT_TABLE_PREFIX,
    closesClient: Boolean = true
) extends OffsetManager {

  import OffsetManagerDynamoDb._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val offsetTableBaseName = s"${tablePrefix}_${DEFAULT_OFFSET_TABLE}"
  private val offsetTableFullName = BookkeeperDynamoDb.getFullTableName(tableArn, offsetTableBaseName)

  // Initialize table on creation
  createOffsetTableIfNotExists()

  override def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset] = {
    val offsets = getOffsetRecords(table, infoDate)

    if (offsets.isEmpty) {
      return Array.empty
    }

    offsets.map(OffsetRecordConverter.toDataOffset)
  }

  override def getUncommittedOffsets(table: String, onlyForInfoDate: Option[LocalDate]): Array[UncommittedOffset] = {
    try {
      onlyForInfoDate match {
        case Some(infoDate) =>
          // Query for specific table and info date
          val offsets = getOffsetRecords(table, infoDate)
          offsets
            .filter(_.committedAtMilli.isEmpty)
            .map(record => OffsetRecordConverter.toDataOffset(record).asInstanceOf[UncommittedOffset])

        case None =>
          // Query all offsets for this table with pagination
          var allItems = Seq.empty[java.util.Map[String, AttributeValue]]
          var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null

          do {
            val queryRequestBuilder = QueryRequest.builder()
              .tableName(offsetTableFullName)
              .keyConditionExpression(s"$ATTR_PRAMEN_TABLE_NAME = :table_name")
              .filterExpression(s"attribute_not_exists($ATTR_COMMITTED_AT)")
              .expressionAttributeValues(Map(
                ":table_name" -> AttributeValue.builder().s(table).build()
              ).asJava)

            if (lastEvaluatedKey != null) {
              queryRequestBuilder.exclusiveStartKey(lastEvaluatedKey)
            }

            val result = dynamoDbClient.query(queryRequestBuilder.build())
            allItems = allItems ++ result.items().asScala
            lastEvaluatedKey = result.lastEvaluatedKey()
          } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

          allItems
            .map(itemToOffsetRecord)
            .map(record => OffsetRecordConverter.toDataOffset(record).asInstanceOf[UncommittedOffset])
            .toArray
      }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to read uncommitted offsets from the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def getMaxInfoDateAndOffset(table: String, onlyForInfoDate: Option[LocalDate]): Option[DataOffsetAggregated] = {
    val maxInfoDateOpt = onlyForInfoDate.orElse(getMaximumInfoDate(table))

    try {
      maxInfoDateOpt.flatMap { infoDate =>
        getMinMaxOffsets(table, infoDate)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def startWriteOffsets(table: String, infoDate: LocalDate, offsetType: OffsetType): DataOffsetRequest = {
    val createdAt = Instant.now()
    val createdAtMilli = createdAt.toEpochMilli
    val compositeKey = s"${infoDate.toString}#${createdAtMilli}"

    try {
      val putRequest = PutItemRequest.builder()
        .tableName(offsetTableFullName)
        .item(Map(
          ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(table).build(),
          ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build(),
          ATTR_INFO_DATE -> AttributeValue.builder().s(infoDate.toString).build(),
          ATTR_DATA_TYPE -> AttributeValue.builder().s(offsetType.dataTypeString).build(),
          ATTR_MIN_OFFSET -> AttributeValue.builder().s("").build(),
          ATTR_MAX_OFFSET -> AttributeValue.builder().s("").build(),
          ATTR_BATCH_ID -> AttributeValue.builder().n(batchId.toString).build(),
          ATTR_CREATED_AT -> AttributeValue.builder().n(createdAtMilli.toString).build()
        ).asJava)
        .build()

      dynamoDbClient.putItem(putRequest)

      DataOffsetRequest(table, infoDate, batchId, createdAt)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to write to the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def commitOffsets(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit = {
    val committedAt = Instant.now().toEpochMilli
    val compositeKey = s"${request.infoDate.toString}#${request.createdAt.toEpochMilli}"

    try {
      val updateRequest = UpdateItemRequest.builder()
        .tableName(offsetTableFullName)
        .key(Map(
          ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(request.tableName).build(),
          ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build()
        ).asJava)
        .updateExpression(s"SET ${ATTR_MIN_OFFSET} = :min_offset, ${ATTR_MAX_OFFSET} = :max_offset, ${ATTR_COMMITTED_AT} = :committed_at")
        .expressionAttributeValues(Map(
          ":min_offset" -> AttributeValue.builder().s(minOffset.valueString).build(),
          ":max_offset" -> AttributeValue.builder().s(maxOffset.valueString).build(),
          ":committed_at" -> AttributeValue.builder().n(committedAt.toString).build()
        ).asJava)
        .build()

      dynamoDbClient.updateItem(updateRequest)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to commit offsets to the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def commitRerun(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit = {
    if (minOffset.compareTo(maxOffset) > 0) {
      throw new IllegalArgumentException(s"minOffset is greater than maxOffset: ${minOffset.valueString} > ${maxOffset.valueString}")
    }

    val committedAt = Instant.now().toEpochMilli
    val compositeKey = s"${request.infoDate.toString}#${request.createdAt.toEpochMilli}"

    try {
      // First, update the current offset
      val updateRequest = UpdateItemRequest.builder()
        .tableName(offsetTableFullName)
        .key(Map(
          ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(request.tableName).build(),
          ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build()
        ).asJava)
        .updateExpression(s"SET ${ATTR_MIN_OFFSET} = :min_offset, ${ATTR_MAX_OFFSET} = :max_offset, ${ATTR_COMMITTED_AT} = :committed_at")
        .expressionAttributeValues(Map(
          ":min_offset" -> AttributeValue.builder().s(minOffset.valueString).build(),
          ":max_offset" -> AttributeValue.builder().s(maxOffset.valueString).build(),
          ":committed_at" -> AttributeValue.builder().n(committedAt.toString).build()
        ).asJava)
        .build()

      dynamoDbClient.updateItem(updateRequest)

      // Then, delete all other offsets for this table and info date
      val allOffsets = getOffsetRecords(request.tableName, request.infoDate)
      allOffsets
        .filter(r => r.createdAtMilli != request.createdAt.toEpochMilli)
        .foreach { record =>
          val deleteCompositeKey = s"${record.infoDate}#${record.createdAtMilli}"
          val deleteRequest = DeleteItemRequest.builder()
            .tableName(offsetTableFullName)
            .key(Map(
              ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(request.tableName).build(),
              ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(deleteCompositeKey).build()
            ).asJava)
            .build()

          dynamoDbClient.deleteItem(deleteRequest)
        }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to commit rerun to the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def postCommittedRecords(commitRequests: Seq[OffsetCommitRequest]): Unit = {
    val committedAt = Instant.now()
    val committedAtMilli = committedAt.toEpochMilli

    try {
      // Insert all new committed records
      commitRequests.foreach { req =>
        val compositeKey = s"${req.infoDate.toString}#${req.createdAt.toEpochMilli}"

        val putRequest = PutItemRequest.builder()
          .tableName(offsetTableFullName)
          .item(Map(
            ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(req.table).build(),
            ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build(),
            ATTR_INFO_DATE -> AttributeValue.builder().s(req.infoDate.toString).build(),
            ATTR_DATA_TYPE -> AttributeValue.builder().s(req.minOffset.dataType.dataTypeString).build(),
            ATTR_MIN_OFFSET -> AttributeValue.builder().s(req.minOffset.valueString).build(),
            ATTR_MAX_OFFSET -> AttributeValue.builder().s(req.maxOffset.valueString).build(),
            ATTR_BATCH_ID -> AttributeValue.builder().n(batchId.toString).build(),
            ATTR_CREATED_AT -> AttributeValue.builder().n(req.createdAt.toEpochMilli.toString).build(),
            ATTR_COMMITTED_AT -> AttributeValue.builder().n(committedAtMilli.toString).build()
          ).asJava)
          .build()

        dynamoDbClient.putItem(putRequest)
      }

      // Delete old offsets for each (table, infoDate) pair
      commitRequests.map(r => (r.table, r.infoDate))
        .distinct
        .foreach { case (table, infoDate) =>
          val allOffsets = getOffsetRecords(table, infoDate)
          allOffsets
            .filter(_.committedAtMilli.exists(_ != committedAtMilli))
            .foreach { record =>
              val deleteCompositeKey = s"${record.infoDate}#${record.createdAtMilli}"
              val deleteRequest = DeleteItemRequest.builder()
                .tableName(offsetTableFullName)
                .key(Map(
                  ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(table).build(),
                  ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(deleteCompositeKey).build()
                ).asJava)
                .build()

              dynamoDbClient.deleteItem(deleteRequest)
            }
        }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to post committed records to the offset table '$offsetTableFullName'.", ex)
    }
  }

  override def rollbackOffsets(request: DataOffsetRequest): Unit = {
    val compositeKey = s"${request.infoDate.toString}#${request.createdAt.toEpochMilli}"

    try {
      val deleteRequest = DeleteItemRequest.builder()
        .tableName(offsetTableFullName)
        .key(Map(
          ATTR_PRAMEN_TABLE_NAME -> AttributeValue.builder().s(request.tableName).build(),
          ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build()
        ).asJava)
        .build()

      dynamoDbClient.deleteItem(deleteRequest)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to rollback offsets in the offset table '$offsetTableFullName'.", ex)
    }
  }

  /**
    * Gets all offset records for a table and info date.
    */
  private[core] def getOffsetRecords(table: String, infoDate: LocalDate): Array[OffsetRecord] = {
    try {
      val queryRequest = QueryRequest.builder()
        .tableName(offsetTableFullName)
        .keyConditionExpression(s"${ATTR_PRAMEN_TABLE_NAME} = :table_name")
        .filterExpression(s"${ATTR_INFO_DATE} = :info_date")
        .expressionAttributeValues(Map(
          ":table_name" -> AttributeValue.builder().s(table).build(),
          ":info_date" -> AttributeValue.builder().s(infoDate.toString).build()
        ).asJava)
        .build()

      val result = dynamoDbClient.query(queryRequest)

      result.items().asScala
        .map(itemToOffsetRecord)
        .toArray
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to read offset records from the offset table '$offsetTableFullName'.", ex)
    }
  }

  /**
    * Gets the maximum information date for a table.
    */
  private[core] def getMaximumInfoDate(table: String): Option[LocalDate] = {
    try {
      val queryRequest = QueryRequest.builder()
        .tableName(offsetTableFullName)
        .keyConditionExpression(s"${ATTR_PRAMEN_TABLE_NAME} = :table_name")
        .expressionAttributeValues(Map(
          ":table_name" -> AttributeValue.builder().s(table).build()
        ).asJava)
        .projectionExpression(ATTR_INFO_DATE)
        .build()

      val result = dynamoDbClient.query(queryRequest)

      if (result.items().isEmpty) {
        None
      } else {
        // Use maxBy with compareTo to avoid needing implicit Ordering
        val maxInfoDate = result.items().asScala
          .map(item => LocalDate.parse(item.get(ATTR_INFO_DATE).s()))
          .maxBy(_.toEpochDay)

        Some(maxInfoDate)
      }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to read maximum info date from the offset table '$offsetTableFullName'.", ex)
    }
  }

  /**
    * Gets min/max offsets for a table and info date, with all offset records for that day.
    */
  private[core] def getMinMaxOffsets(table: String, infoDate: LocalDate): Option[DataOffsetAggregated] = {
    val offsets = getOffsetRecords(table, infoDate).filter(_.committedAtMilli.nonEmpty)

    if (offsets.isEmpty) {
      return None
    }

    validateOffsets(table, infoDate, offsets)

    val (minOffset, maxOffset) = getMinMaxOffsets(offsets)

    Some(DataOffsetAggregated(table, infoDate, minOffset, maxOffset, offsets.map(OffsetRecordConverter.toDataOffset)))
  }

  /**
    * Gets min/max offsets from an array of offset records.
    */
  private[core] def getMinMaxOffsets(offsets: Array[OffsetRecord]): (OffsetValue, OffsetValue) = {
    val offsetDataType = offsets.head.dataType
    val minOffset = offsets.flatMap(or => OffsetValue.fromString(offsetDataType, or.minOffset)).min
    val maxOffset = offsets.flatMap(or => OffsetValue.fromString(offsetDataType, or.maxOffset)).max

    (minOffset, maxOffset)
  }

  /**
    * Validates offsets for inconsistencies (e.g., inconsistent offset value types).
    */
  private[core] def validateOffsets(table: String, infoDate: LocalDate, offsets: Array[OffsetRecord]): Unit = {
    val inconsistentOffsets = offsets.groupBy(_.dataType).keys.toArray.sorted
    if (inconsistentOffsets.length > 1) {
      throw new RuntimeException(s"Inconsistent offset value types found for $table at $infoDate: ${inconsistentOffsets.mkString(", ")}")
    }
  }

  /**
    * Converts a DynamoDB item to an OffsetRecord.
    */
  private def itemToOffsetRecord(item: java.util.Map[String, AttributeValue]): OffsetRecord = {
    val pramenTableName = item.get(ATTR_PRAMEN_TABLE_NAME).s()
    val infoDate = item.get(ATTR_INFO_DATE).s()
    val dataType = item.get(ATTR_DATA_TYPE).s()
    val minOffset = item.get(ATTR_MIN_OFFSET).s()
    val maxOffset = item.get(ATTR_MAX_OFFSET).s()
    val batchId = item.get(ATTR_BATCH_ID).n().toLong
    val createdAtMilli = item.get(ATTR_CREATED_AT).n().toLong
    val committedAtMilli = Option(item.get(ATTR_COMMITTED_AT)).map(_.n().toLong)

    OffsetRecord(pramenTableName, infoDate, dataType, minOffset, maxOffset, batchId, createdAtMilli, committedAtMilli)
  }

  /**
    * Creates the offset table if it doesn't exist.
    */
  private def createOffsetTableIfNotExists(): Unit = {
    try {
      val describeRequest = DescribeTableRequest.builder()
        .tableName(offsetTableFullName)
        .build()

      dynamoDbClient.describeTable(describeRequest)
      log.info(s"Offset table '$offsetTableFullName' already exists")
    } catch {
      case _: ResourceNotFoundException =>
        log.info(s"Creating offset table '$offsetTableFullName'")
        createOffsetTable()
      case NonFatal(ex) =>
        log.error(s"Error checking if offset table exists", ex)
        throw ex
    }
  }

  /**
    * Creates the offset table in DynamoDB.
    */
  private def createOffsetTable(): Unit = {
    val createRequest = CreateTableRequest.builder()
      .tableName(offsetTableFullName)
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(ATTR_PRAMEN_TABLE_NAME)
          .attributeType(ScalarAttributeType.S)
          .build(),
        AttributeDefinition.builder()
          .attributeName(ATTR_COMPOSITE_KEY)
          .attributeType(ScalarAttributeType.S)
          .build()
      )
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(ATTR_PRAMEN_TABLE_NAME)
          .keyType(KeyType.HASH)
          .build(),
        KeySchemaElement.builder()
          .attributeName(ATTR_COMPOSITE_KEY)
          .keyType(KeyType.RANGE)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    dynamoDbClient.createTable(createRequest)
    waitForTableActive(offsetTableFullName)
    log.info(s"Offset table '$offsetTableFullName' created successfully")
  }

  /**
    * Waits for a table to become ACTIVE.
    */
  private def waitForTableActive(tableName: String, maxAttempts: Int = 30): Unit = {
    var attempts = 0
    var isActive = false

    while (attempts < maxAttempts && !isActive) {
      try {
        val describeRequest = DescribeTableRequest.builder()
          .tableName(tableName)
          .build()

        val response = dynamoDbClient.describeTable(describeRequest)
        val status = response.table().tableStatus()

        if (status == TableStatus.ACTIVE) {
          isActive = true
        } else {
          Thread.sleep(1000)
          attempts += 1
        }
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Error waiting for table '$tableName' to become active", ex)
          Thread.sleep(1000)
          attempts += 1
      }
    }

    if (!isActive) {
      throw new RuntimeException(s"Table '$tableName' did not become ACTIVE after $maxAttempts attempts")
    }
  }

  /**
    * Closes the DynamoDB client.
    */
  override def close(): Unit = {
    try {
      if (closesClient) {
        dynamoDbClient.close()
      }
    } catch {
      case NonFatal(ex) =>
        log.warn("Error closing DynamoDB client", ex)
    }
  }
}

object OffsetManagerDynamoDb {
  val DEFAULT_OFFSET_TABLE = "offsets"
  val DEFAULT_TABLE_PREFIX = "pramen"

  // Attribute names for offset table
  val ATTR_PRAMEN_TABLE_NAME = "pramenTableName"
  val ATTR_COMPOSITE_KEY = "compositeKey"  // Format: "infoDate#createdAtMilli"
  val ATTR_INFO_DATE = "infoDate"
  val ATTR_DATA_TYPE = "dataType"
  val ATTR_MIN_OFFSET = "minOffset"
  val ATTR_MAX_OFFSET = "maxOffset"
  val ATTR_BATCH_ID = "batchId"
  val ATTR_CREATED_AT = "createdAt"
  val ATTR_COMMITTED_AT = "committedAt"

  /**
    * Builder for creating OffsetManagerDynamoDb instances.
    */
  class OffsetManagerDynamoDbBuilder {
    private var region: Option[String] = None
    private var tableArn: Option[String] = None
    private var tablePrefix: String = DEFAULT_TABLE_PREFIX
    private var credentialsProvider: Option[AwsCredentialsProvider] = None
    private var endpoint: Option[String] = None
    private var batchId: Long = System.currentTimeMillis()

    def withRegion(region: String): OffsetManagerDynamoDbBuilder = {
      this.region = Some(region)
      this
    }

    def withTableArn(arn: String): OffsetManagerDynamoDbBuilder = {
      this.tableArn = Some(arn)
      this
    }

    def withTablePrefix(prefix: String): OffsetManagerDynamoDbBuilder = {
      this.tablePrefix = prefix
      this
    }

    def withCredentialsProvider(provider: AwsCredentialsProvider): OffsetManagerDynamoDbBuilder = {
      this.credentialsProvider = Some(provider)
      this
    }

    def withEndpoint(endpoint: String): OffsetManagerDynamoDbBuilder = {
      this.endpoint = Some(endpoint)
      this
    }

    def withBatchId(batchId: Long): OffsetManagerDynamoDbBuilder = {
      this.batchId = batchId
      this
    }

    def build(): OffsetManagerDynamoDb = {
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

      new OffsetManagerDynamoDb(
        dynamoDbClient = client,
        batchId = batchId,
        tableArn = tableArn,
        tablePrefix = tablePrefix,
        closesClient = true
      )
    }
  }

  def builder: OffsetManagerDynamoDbBuilder = new OffsetManagerDynamoDbBuilder

  /** Deletes all offsets for a given table. */
  def deleteAllOffsets(tableName: String, dynamoDbClient: DynamoDbClient): Int = {
    val log = LoggerFactory.getLogger(this.getClass)
    val offsetTableBaseName = s"${DEFAULT_TABLE_PREFIX}_${DEFAULT_OFFSET_TABLE}"
    val offsetTableFullName = BookkeeperDynamoDb.getFullTableName(None, offsetTableBaseName)

    try {
      var allItems = Seq.empty[java.util.Map[String, AttributeValue]]
      var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null

      // Query all offsets for the table with pagination
      do {
        val queryRequestBuilder = QueryRequest.builder()
          .tableName(offsetTableFullName)
          .keyConditionExpression(s"$ATTR_PRAMEN_TABLE_NAME = :table_name")
          .expressionAttributeValues(Map(
            ":table_name" -> AttributeValue.builder().s(tableName).build()
          ).asJava)

        if (lastEvaluatedKey != null) {
          queryRequestBuilder.exclusiveStartKey(lastEvaluatedKey)
        }

        val result = dynamoDbClient.query(queryRequestBuilder.build())
        allItems = allItems ++ result.items().asScala
        lastEvaluatedKey = result.lastEvaluatedKey()
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

      // Delete each item
      allItems.foreach { item =>
        val deleteRequest = DeleteItemRequest.builder()
          .tableName(offsetTableFullName)
          .key(Map(
            ATTR_PRAMEN_TABLE_NAME -> item.get(ATTR_PRAMEN_TABLE_NAME),
            ATTR_COMPOSITE_KEY -> item.get(ATTR_COMPOSITE_KEY)
          ).asJava)
          .build()

        dynamoDbClient.deleteItem(deleteRequest)
      }

      val deletedCount = allItems.size
      log.info(s"Deleted $deletedCount offset records for table '$tableName'")
      deletedCount
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error deleting offsets for table '$tableName' from '$offsetTableFullName'", ex)
        throw new RuntimeException(s"Unable to delete offsets for table '$tableName' from '$offsetTableFullName'", ex)
    }
  }
}
