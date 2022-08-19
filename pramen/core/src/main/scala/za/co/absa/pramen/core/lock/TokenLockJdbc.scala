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
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.core.lock.model.{LockTicket, LockTickets}
import za.co.absa.pramen.core.utils.{JvmUtils, SlickUtils, StringUtils}

import java.sql.SQLIntegrityConstraintViolationException
import java.time.Instant
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

class TokenLockJdbc(token: String, db: Database) extends TokenLock {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  protected val TOKEN_EXPIRES_SECONDS: Long = 10L * 60L

  private val log = LoggerFactory.getLogger(this.getClass)
  private val escapedToken = StringUtils.escapeNonAlphanumerics(token)

  // State
  private val owner: String = JvmUtils.jvmName + "_" + Random.nextInt().toString
  private var lockAcquired = false

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (lockAcquired) {
        releaseGuardLock(owner)
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
      releaseGuardLock(owner)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
      log.info(s"Lock released: '$escapedToken'.")
    }
  }

  override def close(): Unit = {}

  private def tryAcquireGuardLock(retries: Int = 3, thisTry: Int = 0): Boolean = synchronized {

    def tryAcquireExistingTicket(): Boolean = {
      val ticket = getTicket

      if (ticket.isEmpty) {
        log.warn(s"No ticket for $escapedToken")
        tryAcquireGuardLock(retries - 1, thisTry + 1)
      } else {
        val expires = ticket.head.expires
        val now = Instant.now().getEpochSecond
        if (expires < now) {
          log.warn(s"Taking over expired ticket $escapedToken ($expires < $now)")
          releaseGuardLock(ticket.get.owner)
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
      val ok = Try(acquireGuardLock())

      ok match {
        case Success(_) =>
          true
        case Failure(_: SQLIntegrityConstraintViolationException) =>
          tryAcquireExistingTicket()
        case Failure(_: org.postgresql.util.PSQLException) =>
          tryAcquireExistingTicket()
        case Failure(ex) if ex.getMessage.contains("constraint") =>
          tryAcquireExistingTicket()
        case Failure(ex) =>
          throw new IllegalStateException(s"Unable to acquire a lock by querying the DB", ex)
      }
    }
  }

  private def getTicket: Option[LockTicket] = {
    val ticket = SlickUtils.executeQuery(db,
      LockTickets.lockTickets
      .filter(_.token === escapedToken))
    ticket.headOption
  }

  private def releaseGuardLock(owner: String): Unit = synchronized {
    try {
      db.run(LockTickets.lockTickets
        .filter(ticket => ticket.token === escapedToken && ticket.owner === owner)
        .delete)
        .execute()
    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to release the lock: $escapedToken.", ex)
    }
  }

  private def acquireGuardLock(): Unit = synchronized {
    db.run(DBIO.seq(
      LockTickets.lockTickets += LockTicket(escapedToken, owner, expires = getNewTicket)
    )).execute()
  }

  private def updateTicket(): Unit = {
    val newTicket = getNewTicket

    try {
      log.debug(s"Update $escapedToken to $newTicket")

      db.run(LockTickets.lockTickets
        .filter(_.token === escapedToken)
        .map(_.expires)
        .update(newTicket))
        .execute()

    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to update the lock: $escapedToken.", ex)
    }
  }

  private def getNewTicket: Long = {
    val now = Instant.now.getEpochSecond
    now + TOKEN_EXPIRES_SECONDS
  }


  private def isAcquired: Boolean = synchronized {
    lockAcquired
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
}
