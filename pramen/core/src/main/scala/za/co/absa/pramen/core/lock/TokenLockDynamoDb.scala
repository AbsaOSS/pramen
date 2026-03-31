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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import za.co.absa.pramen.core.lock.model.LockTicket

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object TokenLockDynamoDb {
  val DEFAULT_TABLE_NAME = "pramen_locks"

  // Attribute names
  val ATTR_TOKEN = "job_token" // 'token' is a reserved word in DynamoDb and can't be used as an attribute
  val ATTR_OWNER = "job_owner" // 'owner' is a reserved word in DynamoDb and can't be used as an attribute
  val ATTR_EXPIRES = "expiresAt"
  val ATTR_CREATED_AT = "createdAt"

  val TICKETS_HARD_EXPIRE_DAYS = 1
}

/**
  * DynamoDB-based distributed lock implementation.
  *
  * This lock uses DynamoDB's conditional writes to implement distributed locking.
  * The lock is maintained by periodic updates to the expiration time.
  *
  * Table schema:
  * - Partition key: token (String)
  * - Attributes: owner (String), expires (Number), createdAt (Number)
  *
  * @param token The unique identifier for the lock
  * @param dynamoDbClient The DynamoDB client to use
  * @param tableName The name of the locks table
  */
class TokenLockDynamoDb(
    token: String,
    dynamoDbClient: DynamoDbClient,
    tableName: String = TokenLockDynamoDb.DEFAULT_TABLE_NAME
) extends TokenLockBase(token) {

  import TokenLockDynamoDb._

  private val log = LoggerFactory.getLogger(this.getClass)

  /** Invoked from a synchronized block. */
  override def tryAcquireGuardLock(retries: Int = 3, thisTry: Int = 0): Boolean = {
    def tryAcquireExistingTicket(): Boolean = {
      val ticketOpt = getTicket

      if (ticketOpt.isEmpty) {
        log.warn(s"No ticket for $escapedToken")
        tryAcquireGuardLock(retries - 1, thisTry + 1)
      } else {
        val ticket = ticketOpt.get
        val expires = ticket.expires
        val now = Instant.now().getEpochSecond

        if (expires < now) {
          log.warn(s"Taking over expired ticket $escapedToken ($expires < $now)")
          releaseGuardLock()
          tryAcquireGuardLock(retries - 1, thisTry + 1)
        } else {
          false
        }
      }
    }

    if (retries <= 0) {
      log.error(s"Cannot try acquire a lock after $thisTry retries.")
      false
    } else {
      val ok = Try(acquireGuardLock())

      ok match {
        case Success(_) =>
          true
        case Failure(_: ConditionalCheckFailedException) =>
          // Lock already exists
          tryAcquireExistingTicket()
        case Failure(ex) =>
          throw new IllegalStateException(s"Unable to acquire a lock by querying DynamoDB", ex)
      }
    }
  }

  /** Invoked from a synchronized block. */
  override def releaseGuardLock(): Unit = {
    try {
      val now = Instant.now()
      val nowEpoch = now.getEpochSecond
      val hardExpireTickets = now.minus(TICKETS_HARD_EXPIRE_DAYS, ChronoUnit.DAYS).getEpochSecond

      // Delete this ticket or any expired tickets
      val deleteRequest = DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map(
          ATTR_TOKEN -> AttributeValue.builder().s(escapedToken).build()
        ).asJava)
        .conditionExpression(s"$ATTR_OWNER = :jobOwner OR ($ATTR_EXPIRES < :now AND $ATTR_CREATED_AT < :hardExpire)")
        .expressionAttributeValues(Map(
          ":jobOwner" -> AttributeValue.builder().s(owner).build(),
          ":now" -> AttributeValue.builder().n(nowEpoch.toString).build(),
          ":hardExpire" -> AttributeValue.builder().n(hardExpireTickets.toString).build()
        ).asJava)
        .build()

      try {
        dynamoDbClient.deleteItem(deleteRequest)
      } catch {
        case _: ConditionalCheckFailedException =>
          // Item doesn't match condition, ignore
          log.debug(s"Could not delete ticket $escapedToken - condition not met")
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"An error occurred when trying to release the lock: $escapedToken.", ex)
    }
  }

  /** Invoked from a synchronized block. */
  override def updateTicket(): Unit = {
    val newTicket = getNewTicket

    try {
      log.debug(s"Update $escapedToken to $newTicket")

      val updateRequest = UpdateItemRequest.builder()
        .tableName(tableName)
        .key(Map(
          ATTR_TOKEN -> AttributeValue.builder().s(escapedToken).build()
        ).asJava)
        .updateExpression(s"SET $ATTR_EXPIRES = :expires")
        .expressionAttributeValues(Map(
          ":expires" -> AttributeValue.builder().n(newTicket.toString).build()
        ).asJava)
        .build()

      dynamoDbClient.updateItem(updateRequest)
    } catch {
      case NonFatal(ex) =>
        log.error(s"An error occurred when trying to update the lock: $escapedToken.", ex)
    }
  }

  /** Invoked from a synchronized block. */
  private def getTicket: Option[LockTicket] = {
    try {
      val getRequest = GetItemRequest.builder()
        .tableName(tableName)
        .key(Map(
          ATTR_TOKEN -> AttributeValue.builder().s(escapedToken).build()
        ).asJava)
        .build()

      val response = dynamoDbClient.getItem(getRequest)

      if (response.hasItem && !response.item().isEmpty) {
        val item = response.item()
        Some(LockTicket(
          token = item.get(ATTR_TOKEN).s(),
          owner = item.get(ATTR_OWNER).s(),
          expires = item.get(ATTR_EXPIRES).n().toLong,
          createdAt = Option(item.get(ATTR_CREATED_AT)).map(_.n().toLong)
        ))
      } else {
        None
      }
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error getting ticket for $escapedToken", ex)
        None
    }
  }

  /** Invoked from a synchronized block. */
  private def acquireGuardLock(): Unit = {
    val now = Instant.now().getEpochSecond
    val item = Map(
      ATTR_TOKEN -> AttributeValue.builder().s(escapedToken).build(),
      ATTR_OWNER -> AttributeValue.builder().s(owner).build(),
      ATTR_EXPIRES -> AttributeValue.builder().n(getNewTicket.toString).build(),
      ATTR_CREATED_AT -> AttributeValue.builder().n(now.toString).build()
    ).asJava

    val putRequest = PutItemRequest.builder()
      .tableName(tableName)
      .item(item)
      .conditionExpression(s"attribute_not_exists($ATTR_TOKEN)")
      .build()

    dynamoDbClient.putItem(putRequest)
  }
}
