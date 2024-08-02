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

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.app.config.InfoDateConfig.DEFAULT_DATE_FORMAT
import za.co.absa.pramen.core.app.config.RuntimeConfig.UNDERCOVER
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.peristence.{MetastorePersistence, TransientJobManager}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.{HiveFormat, HiveHelper}

import java.time.{Instant, LocalDate}

class MetastoreImpl(appConfig: Config,
                    tableDefs: Seq[MetaTable],
                    bookkeeper: Bookkeeper,
                    metadata: MetadataManager,
                    skipBookKeepingUpdates: Boolean)(implicit spark: SparkSession) extends Metastore {
  import MetastoreImpl._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def getRegisteredTables: Seq[String] = tableDefs.map(_.name)

  override def getRegisteredMetaTables: Seq[MetaTable] = tableDefs

  override def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean = {
    isDataAvailable(tableName, Option(infoDate), Option(infoDate))
  }

  override def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean = {
    val mt = getTableDef(tableName)
    val isLazy = mt.format.isLazy

    if (isLazy) {
      (infoDateFromOpt, infoDateToOpt) match {
        case (Some(infoDateFrom), Some(infoDateTo)) =>
          TransientJobManager.selectInfoDatesToExecute(tableName, infoDateFrom, infoDateTo).nonEmpty
        case _ =>
          true // always has data in a half interval
      }
    } else {
      bookkeeper.getDataChunksCount(tableName, infoDateFromOpt, infoDateToOpt) > 0
    }
  }

  override def getTableDef(tableName: String): MetaTable = {
    tableDefs.find(mt => mt.name.equalsIgnoreCase(tableName))
      .getOrElse(throw new NoSuchTable(tableName))
  }

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    val mt = getTableDef(tableName)

    MetastorePersistence.fromMetaTable(mt, appConfig).loadTable(infoDateFrom, infoDateTo)
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = {
    val mt = getTableDef(tableName)
    val isLazy = mt.format.isLazy
    if (isLazy) {
      MetastorePersistence.fromMetaTable(mt, appConfig).loadTable(None, until)
    } else {
      bookkeeper.getLatestProcessedDate(tableName, until) match {
        case Some(infoDate) => getTable(tableName, Some(infoDate), Some(infoDate))
        case None           => throw new NoDataInTable(tableName)
      }
    }
  }

  override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long]): MetaTableStats = {
    val mt = getTableDef(tableName)
    val isTransient = mt.format.isTransient
    val start = Instant.now.getEpochSecond

    var stats = MetaTableStats(0, None)

    withSparkConfig(mt.sparkConfig) {
      stats = MetastorePersistence.fromMetaTable(mt, appConfig).saveTable(infoDate, df, inputRecordCount)
    }

    val finish = Instant.now.getEpochSecond

    if (!skipBookKeepingUpdates) {
      bookkeeper.setRecordCount(tableName, infoDate, infoDate, infoDate, inputRecordCount.getOrElse(stats.recordCount), stats.recordCount, start, finish, isTransient)
    }

    stats
  }

  override def getHiveHelper(tableName: String): HiveHelper = {
    val mt = getTableDef(tableName)

    HiveHelper.fromHiveConfig(mt.hiveConfig)
  }

  override def repairOrCreateHiveTable(tableName: String,
                                       infoDate: LocalDate,
                                       schema: Option[StructType],
                                       hiveHelper: HiveHelper,
                                       recreate: Boolean): Unit = {
    val mt = getTableDef(tableName)
    val hiveTable = mt.hiveTable match {
      case Some(t) =>
        t
      case None    =>
        log.warn(s"Hive table is not defined for '$tableName'. Skipping Hive table repair/creation.")
        return
    }

    val baseSchema = schema.getOrElse(getTable(tableName, Some(infoDate), Some(infoDate)).schema)
    val effectiveSchema = prepareHiveSchema(baseSchema, mt)

    val path = mt.format match {
      case f: DataFormat.Delta   =>
        f.query match {
          case Query.Path(path) => path
          case q                => throw new IllegalArgumentException(s"Unsupported query type '${q.name}' for Delta format")
        }
      case f: DataFormat.Parquet =>
        f.path
      case _ => throw new IllegalArgumentException(s"Hive tables are not supported for metastore tables that are not backed by storage.")
    }

    val format: HiveFormat = mt.format match {
      case _: DataFormat.Delta   => HiveFormat.Delta
      case _: DataFormat.Parquet => HiveFormat.Parquet
      case _                     => throw new IllegalArgumentException(s"Hive tables are not supported for metastore tables that are not backed by storage.")
    }

    val fullTableName = HiveHelper.getFullTable(mt.hiveConfig.database, hiveTable)
    val effectivePath = mt.hivePath.getOrElse(path)

    if (recreate) {
      log.info(s"Recreating Hive table '$fullTableName'")
      hiveHelper.createOrUpdateHiveTable(effectivePath, format, effectiveSchema, Seq(mt.infoDateColumn), mt.hiveConfig.database, hiveTable)
    } else {
      if (hiveHelper.doesTableExist(mt.hiveConfig.database, hiveTable)) {
        if (mt.hivePreferAddPartition && mt.format.isInstanceOf[DataFormat.Parquet]) {
          val location = new Path(effectivePath, s"${mt.infoDateColumn}=${infoDate}")
          log.info(s"The table '$fullTableName' exists. Adding partition '$location'...")
          hiveHelper.repairHiveTable(mt.hiveConfig.database, hiveTable, format)
        } else {
          log.info(s"The table '$fullTableName' exists. Repairing it.")
          hiveHelper.repairHiveTable(mt.hiveConfig.database, hiveTable, format)
        }
      } else {
        log.info(s"The table '$fullTableName' does not exist. Creating it.")
        hiveHelper.createOrUpdateHiveTable(effectivePath, format, effectiveSchema, Seq(mt.infoDateColumn), mt.hiveConfig.database, hiveTable)
      }
    }
  }

  override def getStats(tableName: String, infoDate: LocalDate): MetaTableStats = {
    val mt = getTableDef(tableName)

    MetastorePersistence.fromMetaTable(mt, appConfig).getStats(infoDate)
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

      override def getTableDef(tableName: String): MetaTableDef = {
        validateTable(tableName)

        MetaTable.getMetaTableDef(metastore.getTableDef(tableName))
      }

      override def getTableRunInfo(tableName: String, infoDate: LocalDate): Option[MetaTableRunInfo] = {
        bookkeeper.getLatestDataChunk(tableName, infoDate, infoDate)
          .map(chunk =>
            MetaTableRunInfo(tableName, LocalDate.parse(chunk.infoDate), chunk.inputRecordCount, chunk.outputRecordCount, Instant.ofEpochSecond(chunk.jobStarted), Instant.ofEpochSecond(chunk.jobFinished))
          )
      }

      override def metadataManager: MetadataManager = metadata

      private def validateTable(tableName: String): Unit = {
        if (!tables.contains(tableName)) {
          throw new TableNotConfigured(s"Attempt accessing non-dependent table: $tableName")
        }
      }
    }
  }

  private[core] def prepareHiveSchema(schema: StructType, mt: MetaTable): StructType = {
    val fieldType = if (mt.infoDateFormat == DEFAULT_DATE_FORMAT) DateType else StringType

    val fieldsWithoutPartitionColumn = schema.fields.filterNot(_.name.equalsIgnoreCase(mt.infoDateColumn))

    val fieldsWithPartitionColumn = fieldsWithoutPartitionColumn :+ StructField(mt.infoDateColumn, fieldType, nullable = false)

    StructType(fieldsWithPartitionColumn)
  }
}

object MetastoreImpl {
  private val log = LoggerFactory.getLogger(this.getClass)

  val METASTORE_KEY = "pramen.metastore.tables"
  val DEFAULT_RECORDS_PER_PARTITION = 500000

  def fromConfig(conf: Config,
                 infoDateConfig: InfoDateConfig,
                 bookkeeper: Bookkeeper,
                 metadataManager: MetadataManager)(implicit spark: SparkSession): MetastoreImpl = {
    val tableDefs = MetaTable.fromConfig(conf, infoDateConfig, METASTORE_KEY)

    val isUndercover = ConfigUtils.getOptionBoolean(conf, UNDERCOVER).getOrElse(false)

    new MetastoreImpl(conf, tableDefs, bookkeeper, metadataManager, isUndercover)
  }

  private[core] def withSparkConfig(sparkConfig: Map[String, String])
                                   (action: => Unit)
                                   (implicit spark: SparkSession): Unit = {
    val savedConfig = sparkConfig.map {
      case (k, _) => (k, spark.conf.getOption(k))
    }

    sparkConfig.foreach {
      case (k, v) =>
        val redactedValue = ConfigUtils.renderRedactedKeyValue(k, v, Keys.KEYS_TO_REDACT)
        log.info(s"Setting $redactedValue...")
        spark.conf.set(k, v)
    }

    try {
      action
    } finally {
      savedConfig.foreach {
        case (k, opt) => opt match {
          case Some(v) =>
            val redactedValue = ConfigUtils.renderRedactedKeyValue(k, v, Keys.KEYS_TO_REDACT)
            log.info(s"Restoring $redactedValue...")
            spark.conf.set(k, v)
          case None =>
            log.info(s"Unsetting '$k'...")
            spark.conf.unset(k)
        }
      }
    }
  }
}

