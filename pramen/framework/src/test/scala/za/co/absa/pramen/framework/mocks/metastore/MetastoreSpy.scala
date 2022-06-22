/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.mocks.metastore

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.metastore.{MetaTable, MetaTableStats, Metastore, MetastoreReader}
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.metastore.TableNotConfigured
import za.co.absa.pramen.framework.mocks.MetaTableFactory

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class MetastoreSpy(registeredTables: Seq[String] = Seq("table1", "table2"),
                   availableDates: Seq[LocalDate] = Seq(LocalDate.of(2022, 2, 17)),
                   tableDf: DataFrame = null,
                   tableException: Throwable = null,
                   stats: MetaTableStats = MetaTableStats(0, None),
                   statsException: Throwable = null,
                   isTableAvailable: Boolean = true) extends Metastore {

  val saveTableInvocations = new ListBuffer[(String, LocalDate, DataFrame)]

  override def getRegisteredTables: Seq[String] = registeredTables

  override def getRegisteredMetaTables: Seq[MetaTable] = registeredTables.map(t => MetaTableFactory.getDummyMetaTable(t))

  override def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean = registeredTables.contains(tableName) && availableDates.contains(infoDate)

  override def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean = isTableAvailable

  override def getTableDef(tableName: String): MetaTable = null

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    if (tableException != null)
      throw tableException
    tableDf
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = null

  override def getReader(tableName: String): TableReader = null

  override def getWriter(tableName: String): TableWriter = null

  override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long]): MetaTableStats = {
    saveTableInvocations.append((tableName, infoDate, df))
    MetaTableStats(df.count(), None)
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

      private def validateTable(tableName: String): Unit = {
        if (!tables.contains(tableName)) {
          throw new TableNotConfigured(s"Attempt accessing non-dependent table: $tableName")
        }
      }
    }
  }
}
