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

package za.co.absa.pramen.core.mocks.metastore

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.{DataFormat, MetaTableDef, MetaTableRunInfo, MetadataManager, MetastoreReader}
import za.co.absa.pramen.core.metadata.MetadataManagerNull

import java.time.LocalDate

class MetastoreReaderMock(tables: Seq[(String, DataFrame)], infoDate: LocalDate) extends MetastoreReader {
  private val metadata = new MetadataManagerNull(false)

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    tables.find(_._1 == tableName) match {
      case Some((_, df)) => df
      case None          => throw new IllegalArgumentException(s"Table $tableName not found")
    }
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = {
    tables.find(_._1 == tableName) match {
      case Some((_, df)) => df
      case None          => throw new IllegalArgumentException(s"Table $tableName not found")
    }
  }

  override def getLatestAvailableDate(tableName: String, until: Option[LocalDate]): Option[LocalDate] = {
    tables.find(_._1 == tableName) match {
      case Some(_) => Some(infoDate)
      case None    => throw new IllegalArgumentException(s"Table $tableName not found")
    }
  }

  override def isDataAvailable(tableName: String, from: Option[LocalDate], until: Option[LocalDate]): Boolean = {
    tables.exists(_._1 == tableName)
  }

  override def getTableDef(tableName: String): MetaTableDef = {
    tables.find(_._1 == tableName) match {
      case Some((name, _)) => MetaTableDef(name, "", DataFormat.Null(), "pramen_info_date", "yyyy-MM-dd", None, None, null, Map.empty[String, String], Map.empty[String, String])
      case None          => throw new IllegalArgumentException(s"Table $tableName not found")
    }
  }

  override def getTableRunInfo(tableName: String, infoDate: LocalDate): Option[MetaTableRunInfo] = None

  override def metadataManager: MetadataManager = metadata
}
