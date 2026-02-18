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
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import za.co.absa.pramen.core.lock.model.{LockTicket, LockTicketTable}
import za.co.absa.pramen.core.utils.SlickUtils

import java.sql.SQLIntegrityConstraintViolationException
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class TokenLockJdbc(token: String, db: Database, slickProfile: JdbcProfile) extends TokenLockBase(token) {
  import slickProfile.api._
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val TICKETS_HARD_EXPIRE_DAYS = 1

  private val log = LoggerFactory.getLogger(this.getClass)
  private val slickUtils = new SlickUtils(slickProfile)

  private val lockTicketTable = new LockTicketTable {
    override val profile = slickProfile
  }

  /** Invoked from a synchronized block. */
  override def tryAcquireGuardLock(retries: Int = 3, thisTry: Int = 0): Boolean = {
    slickUtils.ensureDbConnected(db)

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
        case Failure(_: SQLIntegrityConstraintViolationException) => // HSQLDB and possible other DB engines
          tryAcquireExistingTicket()
        case Failure(_: org.postgresql.util.PSQLException) => // PostgreSQL
          tryAcquireExistingTicket()
        case Failure(_: org.sqlite.SQLiteException) => // SQLite
          tryAcquireExistingTicket()
        case Failure(sqlEx: java.sql.SQLException) if sqlEx.getSQLState != null && (sqlEx.getSQLState == "23505" || sqlEx.getSQLState == "23000") => // Conformant JDBC engines
          // 23505: unique violation; 23000: integrity constraint violation (common umbrella)
          tryAcquireExistingTicket()
        case Failure(ex) if ex.getMessage.contains("constraint") => // MS SQL Server
          tryAcquireExistingTicket()
        case Failure(ex) if ex.getMessage.toLowerCase.contains("duplicate entry") => // MySQL
          tryAcquireExistingTicket()
        case Failure(ex) =>
          throw new IllegalStateException(s"Unable to acquire a lock by querying the DB", ex)
      }
    }
  }

  /** Invoked from a synchronized block. */
  override def releaseGuardLock(): Unit = {
    try {
      val now = Instant.now()
      val nowEpoch = now.getEpochSecond
      val hardExpireTickets = now.minus(TICKETS_HARD_EXPIRE_DAYS, ChronoUnit.DAYS).getEpochSecond
      slickUtils.executeAction(
        db,
        lockTicketTable.records
          .filter(ticket => (ticket.token === escapedToken && ticket.owner === owner) ||
            (ticket.createdAt.isDefined && ticket.createdAt < hardExpireTickets && ticket.expires < nowEpoch)).delete
      )
    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to release the lock: $escapedToken.", ex)
    }
  }

  /** Invoked from a synchronized block. */
  override def updateTicket(): Unit = {
    val newTicket = getNewTicket

    try {
      log.debug(s"Update $escapedToken to $newTicket")

      db.run(lockTicketTable.records
          .filter(_.token === escapedToken)
          .map(_.expires)
          .update(newTicket))
        .execute()

    } catch {
      case NonFatal(ex) => log.error(s"An error occurred when trying to update the lock: $escapedToken.", ex)
    }
  }

  /** Invoked from a synchronized block. */
  private def getTicket: Option[LockTicket] = {
    val ticket = slickUtils.executeQuery(db,
      lockTicketTable.records
        .filter(_.token === escapedToken))
    ticket.headOption
  }

  /** Invoked from a synchronized block. */
  private def acquireGuardLock(): Unit = {
    val now = Instant.now().getEpochSecond
    db.run(DBIO.seq(
      lockTicketTable.records += LockTicket(escapedToken, owner, expires = getNewTicket, createdAt = Option(now))
    )).execute()
  }
}
