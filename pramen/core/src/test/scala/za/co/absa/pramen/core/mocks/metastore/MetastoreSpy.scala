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
import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.api.{MetaTableDef, MetaTableRunInfo, MetadataManager, MetastoreReader}
import za.co.absa.pramen.core.metadata.MetadataManagerNull
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.{MetaTableStats, Metastore, TableNotConfigured}
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.utils.hive.QueryExecutorMock
import za.co.absa.pramen.core.utils.hive.{HiveHelper, HiveHelperSql, HiveQueryTemplates}

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class MetastoreSpy(registeredTables: Seq[String] = Seq("table1", "table2"),
                   availableDates: Seq[LocalDate] = Seq(LocalDate.of(2022, 2, 17)),
                   tableDf: DataFrame = null,
                   tableException: Throwable = null,
                   stats: MetaTableStats = MetaTableStats(0, None),
                   statsException: Throwable = null,
                   isTableAvailable: Boolean = true,
                   isTableEmpty: Boolean = false,
                   trackDays: Int = 0,
                   failHive: Boolean = false,
                   readOptions: Map[String, String] = Map.empty[String, String],
                   writeOptions: Map[String, String] = Map.empty[String, String]) extends Metastore {

  val saveTableInvocations = new ListBuffer[(String, LocalDate, DataFrame)]
  var hiveCreationInvocations = new ListBuffer[(String, LocalDate, Option[StructType], Boolean)]
  val queryExecutorMock = new QueryExecutorMock(true)
  val metadataManagerMock = new MetadataManagerNull(false)

  override def getRegisteredTables: Seq[String] = registeredTables

  override def getRegisteredMetaTables: Seq[MetaTable] = registeredTables
    .map(t => MetaTableFactory.getDummyMetaTable(t, readOptions = readOptions, writeOptions = writeOptions))

  override def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean = registeredTables.contains(tableName) && availableDates.contains(infoDate)

  override def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean = {
    if (infoDateFromOpt.isEmpty && infoDateToOpt.isEmpty)
      !isTableEmpty
    else
      isTableAvailable
  }

  override def getTableDef(tableName: String): MetaTable = MetaTableFactory.getDummyMetaTable(name = tableName, trackDays = trackDays)

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    if (tableException != null)
      throw tableException
    tableDf
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = null

  override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long]): MetaTableStats = {
    saveTableInvocations.append((tableName, infoDate, df))
    MetaTableStats(df.count(), None)
  }

  def getHiveHelper(tableName: String): HiveHelper = {
    val defaultQueryTemplates = HiveQueryTemplates.getDefaultQueryTemplates

    new HiveHelperSql(new QueryExecutorMock(isTableAvailable), defaultQueryTemplates, true)
  }

  override def repairOrCreateHiveTable(tableName: String,
                                       infoDate: LocalDate,
                                       schema: Option[StructType],
                                       hiveHelper: HiveHelper,
                                       recreate: Boolean): Unit = {
    if (failHive) {
      throw new RuntimeException("Test exception")
    } else
      hiveCreationInvocations.append((tableName, infoDate, schema, recreate))
  }

  override def getStats(tableName: String, infoDate: LocalDate): MetaTableStats = {
    if (statsException != null)
      throw statsException
    stats
  }

  override def getMetastoreReader(tables: Seq[String], infoDate: LocalDate): MetastoreReader = {
    val metastore = this

    new MetastoreReader {
      override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
        validateTable(tableName)
        val from = infoDateFrom.orElse(Option(infoDate))
        val to = infoDateTo.orElse(Option(infoDate))
        metastore.getTable(tableName, from, to)
      }

      override def getLatest(tableName: String, until: Option[LocalDate] = None): DataFrame = {
        validateTable(tableName)
        val untilDate = until.orElse(Option(infoDate))
        metastore.getLatest(tableName, untilDate)
      }

      override def getLatestAvailableDate(tableName: String, until: Option[LocalDate] = None): Option[LocalDate] = {
        validateTable(tableName)
        None
      }

      override def isDataAvailable(tableName: String, from: Option[LocalDate], until: Option[LocalDate]): Boolean = {
        validateTable(tableName)
        val fromDate = from.orElse(Option(infoDate))
        val untilDate = until.orElse(Option(infoDate))
        metastore.isDataAvailable(tableName, fromDate, untilDate)
      }

      override def getTableDef(tableName: String): MetaTableDef = {
        validateTable(tableName)

        val table = metastore.getTableDef(tableName)

        MetaTableDef(table.name,
          table.description,
          table.format,
          table.infoDateColumn,
          table.infoDateFormat,
          table.hiveTable,
          table.hivePath,
          table.infoDateStart,
          table.readOptions,
          table.writeOptions)
      }

      override def getTableRunInfo(tableName: String, infoDate: LocalDate): Option[MetaTableRunInfo] = None

      override def metadataManager: MetadataManager = metadataManagerMock

      private def validateTable(tableName: String): Unit = {
        if (!tables.contains(tableName)) {
          throw new TableNotConfigured(s"Attempt accessing non-dependent table: $tableName")
        }
      }
    }
  }
}
