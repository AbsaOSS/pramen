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

import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import za.co.absa.pramen.api.MetadataValue
import za.co.absa.pramen.core.bookkeeper.model.{MetadataRecord, MetadataTable}
import za.co.absa.pramen.core.utils.SlickUtils

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal

class MetadataManagerJdbc(db: Database, slickProfile: JdbcProfile) extends MetadataManagerBase(true) {
  import slickProfile.api._
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val slickUtils = new SlickUtils(slickProfile)
  private val metadataTable = new MetadataTable {
    override val profile = slickProfile
  }

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    val query = metadataTable.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)

    try {
      slickUtils.executeQuery(db, query)
        .headOption
        .map(r => MetadataValue(r.value, Instant.ofEpochSecond(r.lastUpdated)))
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the metadata table.", ex)
    }
  }

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    val query = metadataTable.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString)

    try {
      slickUtils.executeQuery(db, query)
        .map(r => r.key -> MetadataValue(r.value, Instant.ofEpochSecond(r.lastUpdated)))
        .toMap
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the metadata table.", ex)
    }
  }

  def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, metadata: MetadataValue): Unit = {
    val record = MetadataRecord(tableName, infoDate.toString, key, metadata.value, metadata.lastUpdated.getEpochSecond)

    try {
      slickUtils.ensureDbConnected(db)
      db.run(
        metadataTable.records
          .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)
          .delete
          .andThen(
            metadataTable.records += record
          )
          .transactionally
      ).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to write to the metadata table.", ex)
    }
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit = {
    val query = metadataTable.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)

    try {
      slickUtils.ensureDbConnected(db)
      db.run(query.delete).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete from the metadata table.", ex)
    }
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit = {
    val query = metadataTable.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString)

    try {
      slickUtils.ensureDbConnected(db)
      db.run(query.delete).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete from the metadata table.", ex)
    }
  }

  /** The implementation does not own DB connections, so it is not responsible for closing them. */
  override def close(): Unit = {}
}
