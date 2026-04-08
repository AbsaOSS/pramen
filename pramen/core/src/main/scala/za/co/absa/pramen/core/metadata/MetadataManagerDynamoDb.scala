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

package za.co.absa.pramen.core.metadata

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.api.MetadataValue
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb.waitForTableActive

import java.net.URI
import java.time.{Instant, LocalDate}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * DynamoDB-based metadata manager for storing custom metadata.
  *
  * This manager stores metadata key-value pairs in a DynamoDB table with automatic table creation.
  *
  * @param dynamoDbClient The DynamoDB client to use
  * @param tableArn Optional ARN prefix for the metadata table
  * @param tablePrefix Prefix for the metadata table name (default: "pramen")
  */
class MetadataManagerDynamoDb private (
    dynamoDbClient: DynamoDbClient,
    tableArn: Option[String] = None,
    tablePrefix: String = MetadataManagerDynamoDb.DEFAULT_TABLE_PREFIX
) extends MetadataManagerBase(true) with AutoCloseable {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val metadataTableBaseName = s"${tablePrefix}_${MetadataManagerDynamoDb.DEFAULT_METADATA_TABLE}"
  private val metadataTableFullName = BookkeeperDynamoDb.getFullTableName(tableArn, metadataTableBaseName)

  // Initialize table on creation
  createMetadataTableIfNotExists()

  override def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    try {
      val compositeKey = s"$tableName#$infoDate"

      val getRequest = GetItemRequest.builder()
        .tableName(metadataTableFullName)
        .key(Map(
          MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build(),
          MetadataManagerDynamoDb.ATTR_METADATA_KEY -> AttributeValue.builder().s(key).build()
        ).asJava)
        .build()

      val result = dynamoDbClient.getItem(getRequest)

      if (result.hasItem) {
        val item = result.item()
        val value = item.get(MetadataManagerDynamoDb.ATTR_METADATA_VALUE).s()
        val lastUpdated = Instant.ofEpochSecond(item.get(MetadataManagerDynamoDb.ATTR_LAST_UPDATED).n().toLong)
        Some(MetadataValue(value, lastUpdated))
      } else {
        None
      }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to read from the metadata table '$metadataTableFullName'.", ex)
    }
  }

  override def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    try {
      val compositeKey = s"$tableName#$infoDate"

      val queryRequest = QueryRequest.builder()
        .tableName(metadataTableFullName)
        .keyConditionExpression(s"${MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY} = :composite_key")
        .expressionAttributeValues(Map(
          ":composite_key" -> AttributeValue.builder().s(compositeKey).build()
        ).asJava)
        .build()

      val result = dynamoDbClient.query(queryRequest)

      result.items().asScala.map { item =>
        val key = item.get(MetadataManagerDynamoDb.ATTR_METADATA_KEY).s()
        val value = item.get(MetadataManagerDynamoDb.ATTR_METADATA_VALUE).s()
        val lastUpdated = Instant.ofEpochSecond(item.get(MetadataManagerDynamoDb.ATTR_LAST_UPDATED).n().toLong)
        key -> MetadataValue(value, lastUpdated)
      }.toMap
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to read from the metadata table '$metadataTableFullName'.", ex)
    }
  }

  override def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, metadata: MetadataValue): Unit = {
    try {
      val compositeKey = s"$tableName#$infoDate"

      val putRequest = PutItemRequest.builder()
        .tableName(metadataTableFullName)
        .item(Map(
          MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build(),
          MetadataManagerDynamoDb.ATTR_METADATA_KEY -> AttributeValue.builder().s(key).build(),
          MetadataManagerDynamoDb.ATTR_METADATA_VALUE -> AttributeValue.builder().s(metadata.value).build(),
          MetadataManagerDynamoDb.ATTR_LAST_UPDATED -> AttributeValue.builder().n(metadata.lastUpdated.getEpochSecond.toString).build(),
          MetadataManagerDynamoDb.ATTR_TABLE_NAME -> AttributeValue.builder().s(tableName).build(),
          MetadataManagerDynamoDb.ATTR_INFO_DATE -> AttributeValue.builder().s(infoDate.toString).build()
        ).asJava)
        .build()

      dynamoDbClient.putItem(putRequest)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to write to the metadata table '$metadataTableFullName'.", ex)
    }
  }

  override def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit = {
    try {
      val compositeKey = s"$tableName#$infoDate"

      val deleteRequest = DeleteItemRequest.builder()
        .tableName(metadataTableFullName)
        .key(Map(
          MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY -> AttributeValue.builder().s(compositeKey).build(),
          MetadataManagerDynamoDb.ATTR_METADATA_KEY -> AttributeValue.builder().s(key).build()
        ).asJava)
        .build()

      dynamoDbClient.deleteItem(deleteRequest)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to delete from the metadata table '$metadataTableFullName'.", ex)
    }
  }

  override def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit = {
    try {
      val compositeKey = s"$tableName#$infoDate"

      // First, query all items with this composite key
      val queryRequest = QueryRequest.builder()
        .tableName(metadataTableFullName)
        .keyConditionExpression(s"${MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY} = :composite_key")
        .expressionAttributeValues(Map(
          ":composite_key" -> AttributeValue.builder().s(compositeKey).build()
        ).asJava)
        .build()

      val result = dynamoDbClient.query(queryRequest)

      // Delete each item
      result.items().asScala.foreach { item =>
        val key = item.get(MetadataManagerDynamoDb.ATTR_METADATA_KEY).s()
        deleteMetadataFromStorage(tableName, infoDate, key)
      }
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to delete from the metadata table '$metadataTableFullName'.", ex)
    }
  }

  /**
    * Creates the metadata table if it doesn't exist.
    */
  private def createMetadataTableIfNotExists(): Unit = {
    try {
      val describeRequest = DescribeTableRequest.builder()
        .tableName(metadataTableFullName)
        .build()

      dynamoDbClient.describeTable(describeRequest)
      log.info(s"Metadata table '$metadataTableFullName' already exists")
    } catch {
      case _: ResourceNotFoundException =>
        log.info(s"Creating metadata table '$metadataTableFullName'")
        createMetadataTable()
      case NonFatal(ex) =>
        log.error(s"Error checking if metadata table exists", ex)
        throw ex
    }
  }

  /**
    * Creates the metadata table in DynamoDB.
    */
  private def createMetadataTable(): Unit = {
    val createRequest = CreateTableRequest.builder()
      .tableName(metadataTableFullName)
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY)
          .attributeType(ScalarAttributeType.S)
          .build(),
        AttributeDefinition.builder()
          .attributeName(MetadataManagerDynamoDb.ATTR_METADATA_KEY)
          .attributeType(ScalarAttributeType.S)
          .build()
      )
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(MetadataManagerDynamoDb.ATTR_COMPOSITE_KEY)
          .keyType(KeyType.HASH)
          .build(),
        KeySchemaElement.builder()
          .attributeName(MetadataManagerDynamoDb.ATTR_METADATA_KEY)
          .keyType(KeyType.RANGE)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    dynamoDbClient.createTable(createRequest)
    waitForTableActive(metadataTableFullName, dynamoDbClient)
    log.info(s"Metadata table '$metadataTableFullName' created successfully")
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

object MetadataManagerDynamoDb {
  val DEFAULT_METADATA_TABLE = "metadata"
  val DEFAULT_TABLE_PREFIX = "pramen"

  // Attribute names for metadata table
  val ATTR_COMPOSITE_KEY = "compositeKey"  // tableName#infoDate
  val ATTR_METADATA_KEY = "metadataKey"
  val ATTR_METADATA_VALUE = "metadataValue"
  val ATTR_LAST_UPDATED = "lastUpdated"
  val ATTR_TABLE_NAME = "tableName"        // For filtering/queries
  val ATTR_INFO_DATE = "infoDate"          // For filtering/queries

  /**
    * Builder for creating MetadataManagerDynamoDb instances.
    */
  class MetadataManagerDynamoDbBuilder {
    private var region: Option[String] = None
    private var tableArn: Option[String] = None
    private var tablePrefix: String = DEFAULT_TABLE_PREFIX
    private var credentialsProvider: Option[AwsCredentialsProvider] = None
    private var endpoint: Option[String] = None

    def withRegion(region: String): MetadataManagerDynamoDbBuilder = {
      this.region = Some(region)
      this
    }

    def withTableArn(arn: String): MetadataManagerDynamoDbBuilder = {
      this.tableArn = Some(arn)
      this
    }

    def withTableArn(arnOpt: Option[String]): MetadataManagerDynamoDbBuilder = {
      this.tableArn = arnOpt
      this
    }

    def withTablePrefix(prefix: String): MetadataManagerDynamoDbBuilder = {
      this.tablePrefix = prefix
      this
    }

    def withCredentialsProvider(provider: AwsCredentialsProvider): MetadataManagerDynamoDbBuilder = {
      this.credentialsProvider = Some(provider)
      this
    }

    def withEndpoint(endpoint: String): MetadataManagerDynamoDbBuilder = {
      this.endpoint = Some(endpoint)
      this
    }

    def build(): MetadataManagerDynamoDb = {
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

      new MetadataManagerDynamoDb(
        dynamoDbClient = client,
        tableArn = tableArn,
        tablePrefix = tablePrefix
      )
    }
  }

  def builder: MetadataManagerDynamoDbBuilder = new MetadataManagerDynamoDbBuilder
}
