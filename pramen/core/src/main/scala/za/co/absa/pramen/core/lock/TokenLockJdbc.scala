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
import za.co.absa.pramen.core.utils.SlickUtils

import java.sql.SQLIntegrityConstraintViolationException
import java.time.Instant
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class TokenLockJdbc(token: String, db: Database) extends TokenLockBase(token) {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def tryAcquireGuardLock(retries: Int = 3, thisTry: Int = 0): Boolean = synchronized {
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

  override def releaseGuardLock(): Unit = synchronized {
    try {
      db.run(LockTickets.lockTickets
        .filter(ticket => ticket.token === escapedToken && ticket.owner === owner)
        .delete)
        .execute()
    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to release the lock: $escapedToken.", ex)
    }
  }

  override def updateTicket(): Unit = {
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

  private def getTicket: Option[LockTicket] = {
    val ticket = SlickUtils.executeQuery(db,
      LockTickets.lockTickets
        .filter(_.token === escapedToken))
    ticket.headOption
  }

  private def acquireGuardLock(): Unit = synchronized {
    db.run(DBIO.seq(
      LockTickets.lockTickets += LockTicket(escapedToken, owner, expires = getNewTicket)
    )).execute()
  }
}
