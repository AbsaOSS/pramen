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
import slick.jdbc.H2Profile.api._
import slick.jdbc.JdbcBackend.Database
import za.co.absa.pramen.core.bookkeeper.model._
import za.co.absa.pramen.core.utils.{SlickUtils, TimeUtils}

import java.time.{Duration, Instant, LocalDate}
import scala.util.control.NonFatal

class OffsetManagerJdbc(db: Database) {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  def getMaximumDateAndOffset(table: String, onlyForInfoDate: Option[LocalDate]): Option[DataOffsetAggregated] = {
    val maxInfoDateOpt = onlyForInfoDate.orElse(getMaximumInfoDate(table))

    try {
      maxInfoDateOpt.flatMap { infoDate =>
        val infoDateStr = infoDate.toString

        val query = OffsetRecords.records
          .filter(r => r.pramenTableName === table && r.infoDate === infoDateStr && r.committedAt.nonEmpty)
          .groupBy { _ => true }
          .map {
            case (_, group) => (group.map(_.dataType).max, group.map(_.minOffset).min, group.map(_.maxOffset).max)
          }

        val action = query.result
        val sql = action.statements.mkString("; ")

        val start = Instant.now
        val result = db.run(action).execute()
        val finish = Instant.now

        val elapsedTime = TimeUtils.prettyPrintElapsedTimeShort(finish.toEpochMilli - start.toEpochMilli)
        if (Duration.between(start, finish).toMillis > 1000L) {
          log.warn(s"Query execution time: $elapsedTime. SQL: $sql")
        } else {
          log.debug(s"Query execution time: $elapsedTime. SQL: $sql")
        }

        if (result.nonEmpty && result.head._1.nonEmpty && result.head._2.nonEmpty && result.head._3.nonEmpty) {
          val minOffset = OffsetValue.fromString(result.head._1.get, result.head._2.get)
          val maxOffset = OffsetValue.fromString(result.head._1.get, result.head._3.get)
          Some(DataOffsetAggregated(
            table, infoDate, minOffset, maxOffset
          ))
        } else {
          None
        }
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table.", ex)
    }
  }

  def getUncommittedOffsets(table: String, onlyForInfoDate: Option[LocalDate]): Seq[DataOffset] = {
    val query = onlyForInfoDate match {
      case Some(infoDate) =>
        val dateSte = infoDate.toString
        OffsetRecords.records
          .filter(r => r.pramenTableName === table && r.committedAt.isEmpty && r.infoDate === dateSte)
      case None =>
        OffsetRecords.records
          .filter(r => r.pramenTableName === table && r.committedAt.isEmpty)
    }

    try {
      SlickUtils.executeQuery[OffsetRecords, OffsetRecord](db, query)
        .map(DataOffset.fromOffsetRecord)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table.", ex)
    }
  }

  def startWriteOffsets(table: String, infoDate: LocalDate, minOffset: OffsetValue): DataOffsetRequest = {
    val createdAt = Instant.now().toEpochMilli

    val record = OffsetRecord(table, infoDate.toString, minOffset.dataTypeString, minOffset.valueString, "", createdAt, None)

    db.run(
      OffsetRecords.records += record
    ).execute()

    DataOffsetRequest(table, infoDate, minOffset, createdAt)
  }

  def commitOffsets(request: DataOffsetRequest, maxOffset: OffsetValue): Unit = {
    val committedAt = Instant.now().toEpochMilli

    db.run(
      OffsetRecords.records
        .filter(r => r.pramenTableName === request.tableName && r.infoDate === request.infoDate.toString && r.createdAt === request.createdAt)
        .map(r => (r.maxOffset, r.committedAt))
        .update((maxOffset.valueString, Some(committedAt)))
    ).execute()
  }

  def rollbackOffsets(request: DataOffsetRequest): Unit = {
    db.run(
      OffsetRecords.records
        .filter(r => r.pramenTableName === request.tableName && r.infoDate === request.infoDate.toString && r.createdAt === request.createdAt)
        .delete
    ).execute()
  }

  def getMaximumInfoDate(table: String): Option[LocalDate] = {
    val query = OffsetRecords.records
      .map(_.infoDate).max

    try {
      SlickUtils.executeMaxString(db, query)
        .map(LocalDate.parse)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table.", ex)
    }
  }

}
