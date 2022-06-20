/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.builtin

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.plugins.{IngestionContext, PostProcessingPlugin}
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.builtin.infofile.InfoFileGeneration
import za.co.absa.pramen.builtin.infofile.InfoFileGeneration.PLUGINS_GENERATE_INFO_FILE
import za.co.absa.pramen.builtin.model.JdbcSource.{Query, Table}
import za.co.absa.pramen.builtin.model.JdbcToParquetSyncTable
import za.co.absa.pramen.builtin.utils.JobConfigUtils.getTableDefs
import za.co.absa.pramen.config.ConfigKeys
import za.co.absa.pramen.config.ConfigKeys.JDBC_SYNC_PREFIX
import za.co.absa.pramen.framework.app.config.GeneralConfig.APPLICATION_VERSION_KEY
import za.co.absa.pramen.framework.config.Keys
import za.co.absa.pramen.framework.config.Keys.TIMEZONE
import za.co.absa.pramen.framework.reader.{TableReaderJdbc, TableReaderJdbcNative}
import za.co.absa.pramen.framework.utils.ConfigUtils
import za.co.absa.pramen.framework.writer.TableWriterParquet

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}

class JDBCToParquetSyncJob(jobName: String,
                           schedule: Schedule,
                           tablesToSync: List[JdbcToParquetSyncTable])
                          (implicit spark: SparkSession, conf: Config) extends SourceJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val infoDateColumn: String = conf.getString(Keys.INFORMATION_DATE_COLUMN)
  private val infoDateFormatApp: String = conf.getString(Keys.INFORMATION_DATE_FORMAT_APP)
  private val tempDirectory: String = conf.getString(Keys.TEMPORARY_DIRECTORY)

  private val outputInfoDateDelay: Int = conf.getInt(ConfigKeys.JDBC_SYNC_TABLE_OUTPUT_INFO_DATE_DELAY)

  private val processingTimestampCol = ConfigUtils.getOptionString(conf, ConfigKeys.JDBC_SYNC_PROCESSING_TIMESTAMP_COL)
  private val snapshotCol = ConfigUtils.getOptionString(conf, ConfigKeys.JDBC_SYNC_SNAPSHOT_DATE)
  private val snapshotDelayDays = ConfigUtils.getOptionInt(conf, ConfigKeys.JDBC_SYNC_SNAP_FIELD_DELAY)

  private val customPartitionPattern = ConfigUtils.getOptionString(conf, Keys.PARQUET_CUSTOM_PATTERN)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def transformOutputInfoDate(infoDate: LocalDate): LocalDate = infoDate.minusDays(outputInfoDateDelay)

  override def getReader(tableName: String): TableReader = {
    tablesToSync.find(t => t.nameInSyncWatcher == tableName) match {
      case Some(tbl) =>
        val parentPath = s"$JDBC_SYNC_PREFIX.reader"
        tbl.source match {
          case Table(tableName) =>
            TableReaderJdbc(tableName, tbl.columns, conf.getConfig(parentPath), parentPath)
          case Query(sql) =>
            TableReaderJdbcNative(sql, conf.getConfig(parentPath), parentPath)
        }
      case None =>
        throw new IllegalArgumentException(s"Cannot find DB table name from the bookkeeping alias $tableName.")
    }
  }

  override def getWriter(tableName: String): TableWriter = {
    val table = tablesToSync.find(t => t.nameInSyncWatcher == tableName).get

    val writerConfig = ConfigUtils.getOptionConfig(conf, ConfigKeys.PARQUET_WRITER_PREFIX)

    val generateInfoFile = conf.getBoolean(PLUGINS_GENERATE_INFO_FILE)

    val plugins = if (generateInfoFile) {
      Seq(getInfoFileGenerationPlugin)
    } else {
      Seq.empty[PostProcessingPlugin]
    }

    TableWriterParquet(infoDateColumn,
      infoDateFormatApp,
      table.outputPath,
      tempDirectory,
      table.recordsPerPartition,
      writerConfig,
      ConfigKeys.PARQUET_WRITER_PREFIX,
      customPartitionPattern,
      plugins)
  }

  override def getTables: Seq[String] = tablesToSync.map(_.nameInSyncWatcher)

  override def runTask(inputTable: TableDataFrame, infoDateBegin: LocalDate, infoDateEnd: LocalDate, infoDateOutput: LocalDate): DataFrame = {
    addProcessingTimestamp(
      addSnapshotDateField(inputTable.dataFrame, infoDateEnd)
    )
  }

  private def addProcessingTimestamp(df: DataFrame): DataFrame = {
    processingTimestampCol match {
      case Some(timestampCol) =>
        if (df.schema.exists(_.name == timestampCol)) {
          log.warn(s"Column $timestampCol already exists. Won't add it.")
          df
        } else {
          // Actual processing timestamp
          val u = udf((a: Long) => Instant.now().getEpochSecond)
          df.withColumn(timestampCol, u(unix_timestamp()))
        }
      case None =>
        df
    }
  }

  private def addSnapshotDateField(df: DataFrame, infoDateEnd: LocalDate): DataFrame = {
    snapshotCol match {
      case Some(snapColumn) =>
        val snapshotDate = snapshotDelayDays match {
          case Some(delayDays) => infoDateEnd.minusDays(delayDays)
          case None => infoDateEnd
        }
        val dateStr = snapshotDate.format(DateTimeFormatter.ofPattern(infoDateFormatApp))
        log.info(s"with $snapColumn = $dateStr")
        df.withColumn(snapColumn, lit(dateStr))
      case None =>
        df
    }
  }

  private def getInfoFileGenerationPlugin: PostProcessingPlugin = {
    new PostProcessingPlugin {
      override def postProcess(context: IngestionContext, rawDf: DataFrame): Unit = {
        val syncWatcherVersion = conf.getString(APPLICATION_VERSION_KEY)
        val timezoneId = getZoneId(conf)

        InfoFileGeneration.generateInfoFile(syncWatcherVersion,
          timezoneId,
          context.sourceCount,
          rawDf,
          context.outputPartitionPath,
          context.infoDate,
          context.sourceStart,
          context.rawStart)(spark, conf.getConfig("pramen.plugins"))
      }
    }
  }

  def getZoneId(implicit conf: Config): ZoneId = {
    ConfigUtils.getOptionString(conf, TIMEZONE) match {
      case Some(tz) => ZoneId.of(tz)
      case None => ZoneId.systemDefault()
    }
  }
}

object JDBCToParquetSyncJob extends JobFactory[JDBCToParquetSyncJob] {

  import ConfigKeys._

  override def apply(conf: Config, spark: SparkSession): JDBCToParquetSyncJob = {
    val syncJobName = ConfigUtils.getOptionString(conf, JDBC_SYNC_JOB_NAME).getOrElse("JDBC to Parquet Sync")
    val schedule = Schedule.fromConfig(conf.getConfig(JDBC_SYNC_PREFIX))
    val syncTables = getTableDefs(conf)

    new JDBCToParquetSyncJob(syncJobName, schedule, syncTables)(spark, conf)
  }

}
