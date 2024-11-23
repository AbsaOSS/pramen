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
import org.apache.spark.sql.functions.{col, lit, max, min}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.offset.{DataOffset, OffsetType, OffsetValue}
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.app.config.InfoDateConfig.DEFAULT_DATE_FORMAT
import za.co.absa.pramen.core.app.config.{InfoDateConfig, RuntimeConfig}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.metastore.model.{MetaTable, TrackingTable}
import za.co.absa.pramen.core.metastore.peristence.{MetastorePersistence, TransientJobManager}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.{HiveFormat, HiveHelper}

import java.time.{Instant, LocalDate}
import scala.collection.mutable.ListBuffer

class MetastoreImpl(appConfig: Config,
                    tableDefs: Seq[MetaTable],
                    bookkeeper: Bookkeeper,
                    metadata: MetadataManager,
                    batchId: Long,
                    isRerun: Boolean,
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

    MetastorePersistence.fromMetaTable(mt, appConfig, batchId).loadTable(infoDateFrom, infoDateTo)
  }

  override def getBatch(tableName: String, infoDate: LocalDate, batchIdOpt: Option[Long]): DataFrame = {
    val mt = getTableDef(tableName)
    val effectiveBatchId = batchIdOpt.getOrElse(batchId)

    val df = MetastorePersistence.fromMetaTable(mt, appConfig, batchId = effectiveBatchId).loadTable(Option(infoDate), Option(infoDate))

    if (df.schema.fields.exists(_.name.equalsIgnoreCase(mt.batchIdColumn))) {
      df.filter(col(mt.batchIdColumn) === lit(batchId))
    } else {
      df
    }
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = {
    val mt = getTableDef(tableName)
    val isLazy = mt.format.isLazy
    if (isLazy) {
      MetastorePersistence.fromMetaTable(mt, appConfig, batchId = batchId).loadTable(None, until)
    } else {
      bookkeeper.getLatestProcessedDate(tableName, until) match {
        case Some(infoDate) => getTable(tableName, Some(infoDate), Some(infoDate))
        case None           => throw new NoDataInTable(tableName)
      }
    }
  }

  override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long], saveModeOverride: Option[SaveMode]): MetaTableStats = {
    val mt = getTableDef(tableName)
    val isTransient = mt.format.isTransient
    val start = Instant.now.getEpochSecond

    var stats = MetaTableStats(Some(0), None, None)

    withSparkConfig(mt.sparkConfig) {
      stats = MetastorePersistence.fromMetaTable(mt, appConfig, batchId, saveModeOverride).saveTable(infoDate, df, inputRecordCount)
    }

    val finish = Instant.now.getEpochSecond

    val nothingAppended = stats.recordCountAppended.contains(0)

    stats.recordCount.foreach{recordCount =>
      if (!skipBookKeepingUpdates && !nothingAppended) {
        bookkeeper.setRecordCount(tableName, infoDate, infoDate, infoDate, inputRecordCount.getOrElse(recordCount), recordCount, start, finish, isTransient)
      }
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

    MetastorePersistence.fromMetaTable(mt, appConfig, batchId = batchId).getStats(infoDate, onlyForCurrentBatchId = false)
  }

  override def getMetastoreReader(tables: Seq[String], outputTable: String, infoDate: LocalDate, runReason: TaskRunReason, isIncremental: Boolean, incrementalDryRun: Boolean, isPostProcessing: Boolean): MetastoreReader = {
    val metastore = this

    new MetastoreReaderCore {
      private val trackingTables = new ListBuffer[TrackingTable]

      override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
        validateTable(tableName)
        val from = infoDateFrom.orElse(Option(infoDate))
        val to = infoDateTo.orElse(Option(infoDate))
        metastore.getTable(tableName, from, to)
      }

      override def getCurrentBatch(tableName: String): DataFrame = {
        validateTable(tableName)
        if (isPostProcessing && isIncremental) {
          metastore.getBatch(tableName, infoDate, None)
        } else if (isIncremental && !isRerun && !isPostProcessing) {
          getIncremental(tableName, outputTable, infoDate)
        } else
          metastore.getTable(tableName, Option(infoDate), Option(infoDate))
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

      override def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset] = {
        val om = bookkeeper.getOffsetManager

        om.getOffsets(table, infoDate)
      }

      override def getTableDef(tableName: String): MetaTableDef = {
        validateTable(tableName) // ToDo Consider removing

        MetaTable.getMetaTableDef(metastore.getTableDef(tableName))
      }

      override def getTableRunInfo(tableName: String, infoDate: LocalDate): Option[MetaTableRunInfo] = {
        bookkeeper.getLatestDataChunk(tableName, infoDate, infoDate)
          .map(chunk =>
            MetaTableRunInfo(tableName, LocalDate.parse(chunk.infoDate), chunk.inputRecordCount, chunk.outputRecordCount, Instant.ofEpochSecond(chunk.jobStarted), Instant.ofEpochSecond(chunk.jobFinished))
          )
      }

      override def getRunReason: TaskRunReason = runReason

      override def metadataManager: MetadataManager = metadata

      override def commitIncremental(): Unit = {
        val om = if (trackingTables.nonEmpty) bookkeeper.getOffsetManager else null

        trackingTables.foreach { trackingTable =>
          log.info(s"Committing offsets for table '${trackingTable.trackingName}' for '$infoDate'")

          val df = metastore.getTable(trackingTable.inputTable, Option(infoDate), Option(infoDate))

          getMinMaxOffsetFromDf(df, trackingTable.batchIdColumn, trackingTable.currentMaxOffset) match {
            case Some((minOffset, maxOffset)) =>
              val offsetType = if (df.schema.fields.find(_.name == trackingTable.batchIdColumn).get.dataType == StringType) OffsetType.StringType else OffsetType.IntegralType
              val req = om.startWriteOffsets(trackingTable.trackingName, infoDate, offsetType)
              om.commitOffsets(req, minOffset, maxOffset)
              log.info(s"Commited offsets for table '${trackingTable.trackingName}' for '$infoDate' with min='${minOffset.valueString}', max='${maxOffset.valueString}'.")
            case None =>
              log.info(s"No new data processed that requires offsets update of table '${trackingTable.trackingName}' for '$infoDate'.")
          }
        }
      }

      private def getMinMaxOffsetFromDf(dfIn: DataFrame, batchIdColumn: String, currentMax: Option[OffsetValue]): Option[(OffsetValue, OffsetValue)] = {
        val df = currentMax match {
          case Some(currentMax) =>
            dfIn.filter(col(batchIdColumn) > currentMax.getSparkLit)
          case None =>
            dfIn
        }

        if (df.isEmpty) {
          None
        } else {
          val offsetType = if (df.schema.fields.find(_.name == batchIdColumn).get.dataType == StringType) OffsetType.StringType else OffsetType.IntegralType
          val row = df.agg(min(offsetType.getSparkCol(col(batchIdColumn)).cast(StringType)),
              max(offsetType.getSparkCol(col(batchIdColumn))).cast(StringType))
            .collect()(0)

          val minValue = OffsetValue.fromString(offsetType.dataTypeString, row(0).asInstanceOf[String]).getOrElse(throw new IllegalArgumentException(s"Can't parse offset: ${row(0)}"))
          val maxValue = OffsetValue.fromString(offsetType.dataTypeString, row(1).asInstanceOf[String]).getOrElse(throw new IllegalArgumentException(s"Can't parse offset: ${row(1)}"))
          Some(minValue, maxValue)
        }
      }

      private def validateTable(tableName: String): Unit = {
        if (!tables.contains(tableName)) {
          throw new TableNotConfigured(s"Attempt accessing non-dependent table: $tableName")
        }
      }

      private def getIncremental(tableName: String, transformationOutputTable: String, infoDate: LocalDate): DataFrame = {
        val trackingName = s"$tableName->$transformationOutputTable"
        val tableDef = getTableDef(tableName)
        val offsetType = if (tableDef.format.isInstanceOf[DataFormat.Raw]) OffsetType.StringType else OffsetType.IntegralType
        val needsToCommit = !isPostProcessing && !incrementalDryRun
        val om = bookkeeper.getOffsetManager
        val tableDf = metastore.getTable(tableName, Option(infoDate), Option(infoDate))

        if (!tableDf.schema.exists(_.name == tableDef.batchIdColumn)) {
          throw new IllegalArgumentException(s"Table '$tableName' does not contain column '${tableDef.batchIdColumn}' needed for incremental processing.")
        }

        // ToDo Handle uncommitted offsets

        val offsets = om.getMaxInfoDateAndOffset(trackingName, Option(infoDate))

        val df = offsets match {
          case Some(values) =>
            log.info(s"Getting incremental table '$trackingName' for '$infoDate', column '${tableDef.batchIdColumn}' > ${values.maximumOffset.valueString}")
            tableDf.filter(col(tableDef.batchIdColumn) > values.maximumOffset.getSparkLit)
          case None =>
            log.info(s"Getting incremental table '$trackingName' for '$infoDate''")
            tableDf
        }

        if (needsToCommit && !trackingTables.exists(t => t.trackingName == trackingName && t.infoDate == infoDate)) {
          log.info(s"Starting offset commit for table '$trackingName' for '$infoDate''")

          val trackingTable = TrackingTable(
            tableName,
            outputTable,
            trackingName,
            tableDef.batchIdColumn,
            offsets.map(_.maximumOffset),
            infoDate
          )

          trackingTables += trackingTable
        }

        df
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
                 runtimeConfig: RuntimeConfig,
                 infoDateConfig: InfoDateConfig,
                 bookkeeper: Bookkeeper,
                 metadataManager: MetadataManager,
                 batchId: Long)(implicit spark: SparkSession): MetastoreImpl = {
    val tableDefs = MetaTable.fromConfig(conf, infoDateConfig, METASTORE_KEY)

    new MetastoreImpl(conf,
      tableDefs,
      bookkeeper,
      metadataManager,
      batchId,
      runtimeConfig.isRerun,
      runtimeConfig.isUndercover)
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

