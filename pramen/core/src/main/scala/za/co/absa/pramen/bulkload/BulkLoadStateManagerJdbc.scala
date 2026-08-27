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

package za.co.absa.pramen.bulkload

import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import za.co.absa.pramen.bulkload.model.{BulkLoadState, BulkLoadStateTable}
import za.co.absa.pramen.core.utils.SlickUtils

import java.time.LocalDate
import scala.util.control.NonFatal

class BulkLoadStateManagerJdbc(db: Database, slickProfile: JdbcProfile) extends BulkLoadStateManager {
  import slickProfile.api._
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val slickUtils = new SlickUtils(slickProfile)

  private val bulkLoadStateTable = new BulkLoadStateTable {
    override val profile = slickProfile
  }

  override def getState(pramenTableName: String, infoDate: LocalDate): Option[BulkLoadState] = {
    val infoDateStr = infoDate.toString

    val records = try {
      slickUtils.ensureDbConnected(db)
      db.run(
        bulkLoadStateTable.records
          .filter(r => r.pramenTableName === pramenTableName && r.outputInfoDate === infoDateStr)
          .take(1)
          .result
      ).execute()
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to read from the bulk_load_state table for the table '$pramenTableName' and the info date '$infoDateStr'.", ex)
        throw new RuntimeException(s"Unable to read from the bulk_load_state table for the table '$pramenTableName' and the info date '$infoDateStr'.", ex)
    }

    records.headOption.map(record => BulkLoadState.fromSerialized(record))
  }

  override def deleteState(pramenTableName: String, infoDate: LocalDate): Unit = {
    val infoDateStr = infoDate.toString

    try {
      slickUtils.ensureDbConnected(db)
      db.run(
        bulkLoadStateTable.records
          .filter(r => r.pramenTableName === pramenTableName && r.outputInfoDate === infoDateStr)
          .delete
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to delete from the bulk_load_state table for the table '$pramenTableName' and the info date '$infoDateStr'.", ex)
    }
  }

  override def addState(bulkLoadState: BulkLoadState): Unit = {
    val bulkLoadStateSerialized = BulkLoadState.toSerialized(bulkLoadState)

    try {
      slickUtils.ensureDbConnected(db)
      db.run(
        bulkLoadStateTable.records += bulkLoadStateSerialized
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to write to the bulk_load_state table.", ex)
    }
  }

  override def updatePhase(bulkLoadState: BulkLoadState): Unit = {
    val bulkLoadStateSerialized = BulkLoadState.toSerialized(bulkLoadState)

    try {
      slickUtils.ensureDbConnected(db)
      db.run(
        bulkLoadStateTable.records
          .filter(r => r.pramenTableName === bulkLoadStateSerialized.pramenTableName && r.outputInfoDate === bulkLoadStateSerialized.outputInfoDate)
          .map(_.phase)
          .update(bulkLoadStateSerialized.phase)
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to update the phase in the bulk_load_state table.", ex)
    }
  }
}
