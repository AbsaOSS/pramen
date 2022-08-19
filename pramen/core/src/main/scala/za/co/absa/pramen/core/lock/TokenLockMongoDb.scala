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

import java.time.Instant

import com.mongodb.MongoWriteException
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.{Filters, Updates}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.dao.model.{ASC, IndexField}
import za.co.absa.pramen.core.dao.{MongoDb, ScalaMongoImplicits}
import za.co.absa.pramen.core.lock.model.LockTicket
import za.co.absa.pramen.core.mongo.MongoDbConnection
import za.co.absa.pramen.core.utils.{JvmUtils, StringUtils}

import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

object TokenLockMongoDb {
  val collectionName = "locks"
}

class TokenLockMongoDb(token: String,
                       mongoDbConnection: MongoDbConnection) extends TokenLock {

  import ScalaMongoImplicits._
  import TokenLockMongoDb._

  protected val TOKEN_EXPIRES_SECONDS: Long = 10L * 60L

  private val log = LoggerFactory.getLogger(this.getClass)
  private val escapedToken = StringUtils.escapeNonAlphanumerics(token)

  // MongoDB
  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[LockTicket]), DEFAULT_CODEC_REGISTRY)
  private val db = mongoDbConnection.getDatabase

  // State
  private val owner: String = JvmUtils.jvmName + "_" + Random.nextInt().toString
  private var lockAcquired = false

  initCollection()

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (lockAcquired) {
        releaseGuardLock()
      }
    }
  }

  override def tryAcquire(): Boolean = synchronized {
    if (lockAcquired) {
      false
    } else {
      if (tryAcquireGuardLock()) {
        lockAcquired = true
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        startWatcherThread()
        log.info(s"Lock '$token' lock acquired: '$escapedToken'.")
        true
      } else {
        log.warn(s"Lock '$token' is acquired by another job.")
        false
      }
    }
  }

  override def release(): Unit = synchronized {
    if (lockAcquired) {
      lockAcquired = false
      releaseGuardLock()
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
      log.info(s"Lock released: '$escapedToken'.")
    }
  }

  override def close(): Unit = {}

  private def tryAcquireGuardLock(retries: Int = 3, thisTry: Int = 0): Boolean = synchronized {
    val c = getCollection

    def tryAcquireExistingTicket(): Boolean = {
      val ticket = c.find(getFilter).execute()

      if (ticket.isEmpty) {
        log.warn(s"No ticket for $escapedToken")
        tryAcquireGuardLock(retries - 1, thisTry + 1)
      } else {
        val expires = ticket.head.expires
        val now = Instant.now().getEpochSecond
        if (expires < now) {
          log.warn(s"Taking over expired ticket $escapedToken ($expires < $now)")
          releaseGuardLock()
          tryAcquireGuardLock(retries - 1, thisTry + 1)
          true
        } else {
          false
        }
      }
    }

    if (retries <= 0) {
      log.error(s"Cannot try acquire a lock after $thisTry retries.")
      false
    } else {
      val ok = Try(c.insertOne(LockTicket(escapedToken, owner, getNewTicket)).execute())

      ok match {
        case Success(_) =>
          true
        case Failure(_: MongoWriteException) =>
          tryAcquireExistingTicket()
        case Failure(ex) =>
          throw new IllegalStateException(s"Unable to acquire a lock by querying MongoDB", ex)
      }
    }
  }

  private def releaseGuardLock(): Unit = synchronized {
    try {
      val c = getCollection
      log.debug(s"Delete token $escapedToken")
      c.deleteOne(getFilter).execute()

    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to release the lock: $escapedToken.", ex)
    }
  }

  private def isAcquired: Boolean = synchronized {
    lockAcquired
  }

  private def initCollection(): Unit = synchronized {
    try {
      val d = new MongoDb(db)
      if (!d.doesCollectionExists(collectionName)) {
        d.createCollection(collectionName)
        d.createIndex(collectionName, IndexField("token", ASC) :: Nil, unique = true)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to connect to MongoDb instance: ${mongoDbConnection.getConnectionString}", ex)
    }
  }

  private def startWatcherThread(): Thread = {
    val thread = new Thread {
      override def run(): Unit = {
        lockWatcher()
      }
    }
    thread.start()
    thread
  }

  private def lockWatcher(): Unit = {
    var lastUpdateTime = Instant.now
    while (isAcquired) {
      Thread.sleep(1000)
      if (Instant.now().isAfter(lastUpdateTime.plusMillis((TOKEN_EXPIRES_SECONDS * 1000) / 5))) {
        this.synchronized {
          if (isAcquired) {
            updateTicket()
            lastUpdateTime = Instant.now
          }
        }
      }
    }
  }

  private def updateTicket(): Unit = {
    val newTicket = getNewTicket

    try {
      log.debug(s"Update $escapedToken to $newTicket")
      val c = getCollection
      c.updateOne(getFilter,
        Updates.set("expires", newTicket)).execute()
    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to update the lock: $escapedToken.", ex)
    }
  }

  private def getNewTicket: Long = {
    val now = Instant.now.getEpochSecond
    now + TOKEN_EXPIRES_SECONDS
  }

  private def getCollection: MongoCollection[LockTicket] =
    db.getCollection[LockTicket](collectionName).withCodecRegistry(codecRegistry)

  private def getFilter: Bson = Filters.eq("token", escapedToken)

}
