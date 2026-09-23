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

package za.co.absa.pramen.core.lock

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.api.lock.{TokenLock, TokenLockFactory}
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb.waitForTableActive

import java.net.URI
import scala.util.control.NonFatal

/**
  * Factory for creating DynamoDB-based distributed locks.
  *
  * This factory creates and manages a DynamoDB table for storing lock tickets.
  * The table is created automatically if it doesn't exist.
  *
  * @param dynamoDbClient The DynamoDB client to use
  * @param tableArn Optional ARN prefix for the locks table
  * @param tablePrefix Prefix for the locks table name (default: "pramen")
  */
class TokenLockFactoryDynamoDb private(
    dynamoDbClient: DynamoDbClient,
    tableArn: Option[String] = None,
    tablePrefix: String = "pramen"
) extends TokenLockFactory with AutoCloseable {
  import TokenLockFactoryDynamoDb._

  import TokenLockDynamoDb._

  private val log = LoggerFactory.getLogger(this.getClass)

  // Construct table name with prefix
  private val locksTableBaseName = s"${tablePrefix}_locks"
  private val locksTableName = BookkeeperDynamoDb.getFullTableName(tableArn, locksTableBaseName)

  // Initialize table on construction
  init()

  override def getLock(token: String): TokenLock = {
    new TokenLockDynamoDb(token, dynamoDbClient, locksTableName)
  }

  /**
    * Closes the DynamoDB client.
    * Should be called when the lock factory is no longer needed.
    */
  override def close(): Unit = {
    try {
      dynamoDbClient.close()
    } catch {
      case NonFatal(ex) =>
        log.warn("Error closing DynamoDB client", ex)
    }
  }

  /**
    * Initializes the DynamoDB locks table.
    * Checks if the table exists and creates it if it doesn't.
    */
  private def init(): Unit = {
    try {
      log.info(s"Initializing DynamoDB lock factory with table: '$locksTableName'")

      if (!tableExists(locksTableName)) {
        log.info(s"Creating DynamoDB locks table: $locksTableName")
        createLocksTable(locksTableName)
        log.info(s"Successfully created locks table: $locksTableName")
      } else {
        log.info(s"DynamoDB locks table already exists: $locksTableName")
      }

      log.info(s"DynamoDB lock factory initialization complete")
    } catch {
      case NonFatal(ex) =>
        log.error("Error initializing DynamoDB lock factory", ex)
        throw new RuntimeException("Failed to initialize DynamoDB lock factory", ex)
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
    * Creates the locks table with the appropriate schema.
    *
    * @param tableName The name of the table to create
    */
  private def createLocksTable(tableName: String): Unit = {
    val createTableRequest = CreateTableRequest.builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder()
          .attributeName(ATTR_TOKEN)
          .keyType(KeyType.HASH)
          .build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder()
          .attributeName(ATTR_TOKEN)
          .attributeType(ScalarAttributeType.S)
          .build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST) // On-demand billing
      .build()

    dynamoDbClient.createTable(createTableRequest)

    // Wait for table to become active
    waitForTableActive(tableName, dynamoDbClient)
  }
}

object TokenLockFactoryDynamoDb {
  /**
    * Builder for creating TokenLockFactoryDynamoDb instances.
    * Provides a fluent API for configuring DynamoDB lock factory.
    *
    * Example:
    * {{{
    * val lockFactory = TokenLockFactoryDynamoDb.builder
    *   .withRegion("us-east-1")
    *   .withTablePrefix("my_app")
    *   .build()
    * }}}
    */
  class TokenLockFactoryDynamoDbBuilder {
    private var region: Option[String] = None
    private var tableArn: Option[String] = None
    private var tablePrefix: String = BookkeeperDynamoDb.DEFAULT_TABLE_PREFIX
    private var credentialsProvider: Option[AwsCredentialsProvider] = None
    private var endpoint: Option[String] = None

    /**
      * Sets the AWS region for the DynamoDB client.
      *
      * @param region AWS region (e.g., "us-east-1", "eu-west-1")
      * @return this builder
      */
    def withRegion(region: String): TokenLockFactoryDynamoDbBuilder = {
      this.region = Some(region)
      this
    }

    /**
      * Sets the table ARN prefix for cross-account or cross-region access.
      *
      * @param arn ARN prefix (e.g., "arn:aws:dynamodb:us-east-1:123456789012:table/")
      * @return this builder
      */
    def withTableArn(arn: String): TokenLockFactoryDynamoDbBuilder = {
      this.tableArn = Some(arn)
      this
    }

    /**
      * Sets the table ARN prefix for cross-account or cross-region access.
      *
      * @param arnOpt ARN prefix (e.g., "arn:aws:dynamodb:us-east-1:123456789012:table/")
      * @return this builder
      */
    def withTableArn(arnOpt: Option[String]): TokenLockFactoryDynamoDbBuilder = {
      this.tableArn = arnOpt
      this
    }

    /**
      * Sets the table name prefix to allow multiple lock tables in the same account.
      *
      * @param prefix Table name prefix (default: "pramen")
      * @return this builder
      */
    def withTablePrefix(prefix: String): TokenLockFactoryDynamoDbBuilder = {
      this.tablePrefix = prefix
      this
    }

    /**
      * Sets custom AWS credentials provider.
      *
      * @param provider AWS credentials provider
      * @return this builder
      */
    def withCredentialsProvider(provider: AwsCredentialsProvider): TokenLockFactoryDynamoDbBuilder = {
      this.credentialsProvider = Some(provider)
      this
    }

    /**
      * Sets a custom DynamoDB endpoint (useful for testing with LocalStack or DynamoDB Local).
      *
      * @param endpoint Endpoint URI (e.g., "http://localhost:8000")
      * @return this builder
      */
    def withEndpoint(endpoint: String): TokenLockFactoryDynamoDbBuilder = {
      this.endpoint = Some(endpoint)
      this
    }

    /**
      * Builds the TokenLockFactoryDynamoDb instance.
      *
      * @return Configured TokenLockFactoryDynamoDb instance
      * @throws IllegalArgumentException if required parameters are missing
      */
    def build(): TokenLockFactoryDynamoDb = {
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

      try {
        new TokenLockFactoryDynamoDb(
          dynamoDbClient = client,
          tableArn = tableArn,
          tablePrefix = tablePrefix
        )
      } catch {
        case NonFatal(ex) =>
          client.close()
          throw ex
      }
    }
  }

  /**
    * Creates a new builder for TokenLockFactoryDynamoDb.
    *
    * @return A new builder instance
    */
  def builder: TokenLockFactoryDynamoDbBuilder = new TokenLockFactoryDynamoDbBuilder
}
