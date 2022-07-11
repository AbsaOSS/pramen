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

package za.co.absa.pramen.framework.metastore

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{MetastoreReader, TableReader, TableWriter}
import za.co.absa.pramen.framework.app.config.RuntimeConfig.UNDERCOVER
import za.co.absa.pramen.framework.bookkeeper.Bookkeeper
import za.co.absa.pramen.framework.config.Keys.TEMPORARY_DIRECTORY
import za.co.absa.pramen.framework.metastore.MetastoreImpl.DEFAULT_RECORDS_PER_PARTITION
import za.co.absa.pramen.framework.metastore.model.{DataFormat, MetaTable}
import za.co.absa.pramen.framework.metastore.peristence.MetastorePersistence
import za.co.absa.pramen.framework.reader.{TableReaderDelta, TableReaderParquet}
import za.co.absa.pramen.framework.utils.ConfigUtils
import za.co.absa.pramen.framework.writer.{TableWriterDelta, TableWriterParquet}

import java.time.{Instant, LocalDate}

class MetastoreImpl(tableDefs: Seq[MetaTable],
                    bookkeeper: Bookkeeper,
                    tempPath: String,
                    skipBookKeepingUpdates: Boolean)(implicit spark: SparkSession) extends Metastore {
  override def getRegisteredTables: Seq[String] = tableDefs.map(_.name)

  override def getRegisteredMetaTables: Seq[MetaTable] = tableDefs

  override def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean = {
    bookkeeper.getDataChunks(tableName, infoDate, infoDate).nonEmpty
  }

  override def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean = {
    bookkeeper.getDataChunksCount(tableName, infoDateFromOpt, infoDateToOpt) > 0
  }

  override def getTableDef(tableName: String): MetaTable = {
    tableDefs.find(mt => mt.name.equalsIgnoreCase(tableName))
      .getOrElse(throw new NoSuchTable(tableName))
  }

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    val mt = getTableDef(tableName)

    MetastorePersistence.fromMetaTable(mt).loadTable(infoDateFrom, infoDateTo)
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = {
    bookkeeper.getLatestProcessedDate(tableName, until) match {
      case Some(infoDate) => getTable(tableName, Some(infoDate), Some(infoDate))
      case None => throw new NoDataInTable(tableName)
    }
  }

  override def getReader(tableName: String): TableReader = {
    val mt = getTableDef(tableName)

    mt.format match {
      case DataFormat.Parquet(path, _) =>
        new TableReaderParquet(path, hasInfoDateColumn = true, infoDateColumn = mt.infoDateColumn, infoDateFormat = mt.infoDateFormat)
      case DataFormat.Delta(query, _)  =>
        new TableReaderDelta(query, mt.infoDateColumn, mt.infoDateFormat)
    }
  }

  override def getWriter(tableName: String): TableWriter = {
    val mt = getTableDef(tableName)

    mt.format match {
      case DataFormat.Parquet(path, recordsPerPartition) =>
        new TableWriterParquet(mt.infoDateColumn, mt.infoDateFormat, path, tempPath, recordsPerPartition.getOrElse(DEFAULT_RECORDS_PER_PARTITION), None)
      case DataFormat.Delta(query, recordsPerPartition)  =>
        new TableWriterDelta(mt.infoDateColumn, mt.infoDateFormat, query, recordsPerPartition.getOrElse(DEFAULT_RECORDS_PER_PARTITION), Map.empty[String, String])
    }
  }

  override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long]): MetaTableStats = {
    val mt = getTableDef(tableName)

    val start = Instant.now.getEpochSecond
    val stats = MetastorePersistence.fromMetaTable(mt).saveTable(infoDate, df, inputRecordCount)
    val finish = Instant.now.getEpochSecond

    if (!skipBookKeepingUpdates) {
      bookkeeper.setRecordCount(tableName, infoDate, infoDate, infoDate, inputRecordCount.getOrElse(stats.recordCount), stats.recordCount, start, finish)
    }

    stats
  }

  override def getStats(tableName: String, infoDate: LocalDate): MetaTableStats = {
    val mt = getTableDef(tableName)

    MetastorePersistence.fromMetaTable(mt).getStats(infoDate)
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
        val untilDate = until.orElse(Option(infoDate))
        bookkeeper.getLatestProcessedDate(tableName, untilDate)
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

object MetastoreImpl {
  val METASTORE_KEY = "pramen.metastore.tables"
  val DEFAULT_RECORDS_PER_PARTITION = 500000

  def fromConfig(conf: Config, bookkeeper: Bookkeeper)(implicit spark: SparkSession): MetastoreImpl = {
    val tempPath = conf.getString(TEMPORARY_DIRECTORY)
    val tableDefs = MetaTable.fromConfig(conf, METASTORE_KEY)

    val isUndercover = ConfigUtils.getOptionBoolean(conf, UNDERCOVER).getOrElse(false)

    new MetastoreImpl(tableDefs, bookkeeper, tempPath, isUndercover)
  }
}

