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

package za.co.absa.pramen.core.metastore

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.utils.hive.HiveHelper

import java.time.LocalDate

trait Metastore {
  def getRegisteredTables: Seq[String]

  def getRegisteredMetaTables: Seq[MetaTable]

  def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean

  def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean

  def getTableDef(tableName: String): MetaTable

  def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame

  def getLatest(tableName: String, until: Option[LocalDate]): DataFrame

  def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long] = None): MetaTableStats

  def getHiveHelper(tableName: String): HiveHelper

  def repairOrCreateHiveTable(tableName: String,
                              infoDate: LocalDate,
                              schema: Option[StructType],
                              hiveHelper: HiveHelper,
                              recreate: Boolean): Unit

  def getStats(tableName: String, infoDate: LocalDate): MetaTableStats

  def getMetastoreReader(tables: Seq[String], infoDate: LocalDate): MetastoreReader
}
