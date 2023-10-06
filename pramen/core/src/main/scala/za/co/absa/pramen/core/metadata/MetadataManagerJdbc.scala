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

import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.api.MetadataValue
import za.co.absa.pramen.core.bookkeeper.model.{MetadataRecord, MetadataRecords}

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal
import za.co.absa.pramen.core.utils.SlickUtils

class MetadataManagerJdbc(db: Database) extends MetadataManagerBase(true) {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    val query = MetadataRecords.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)

    try {
      SlickUtils.executeQuery[MetadataRecords, MetadataRecord](db, query)
        .headOption
        .map(r => MetadataValue(r.value, Instant.ofEpochSecond(r.lastUpdated)))
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the metadata table.", ex)
    }
  }

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    val query = MetadataRecords.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString)

    try {
      SlickUtils.executeQuery[MetadataRecords, MetadataRecord](db, query)
        .map(r => r.key -> MetadataValue(r.value, Instant.ofEpochSecond(r.lastUpdated)))
        .toMap
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the metadata table.", ex)
    }
  }

  def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, metadata: MetadataValue): Unit = {
    val record = MetadataRecord(tableName, infoDate.toString, key, metadata.value, metadata.lastUpdated.getEpochSecond)

    try {
      db.run(
        MetadataRecords.records
          .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)
          .delete
          .andThen(
            MetadataRecords.records += record
          )
          .transactionally
      ).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to write to the metadata table.", ex)
    }
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit = {
    val query = MetadataRecords.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString && r.key === key)

    try {
      db.run(query.delete).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete from the metadata table.", ex)
    }
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit = {
    val query = MetadataRecords.records
      .filter(r => r.pramenTableName === tableName && r.infoDate === infoDate.toString)

    try {
      db.run(query.delete).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete from the metadata table.", ex)
    }
  }
}
