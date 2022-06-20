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
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.builtin.model.ParquetKafkaSyncTable
import za.co.absa.pramen.config.ConfigKeys
import za.co.absa.pramen.framework.config.Keys
import za.co.absa.pramen.framework.reader.TableReaderParquet
import za.co.absa.pramen.framework.utils.ConfigUtils
import za.co.absa.pramen.writer.TableWriterKafka

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ParquetToKafkaSyncJob(jobName: String,
                            schedule: Schedule,
                            tablesToSync: List[ParquetKafkaSyncTable])
                           (implicit spark: SparkSession, conf: Config) extends SourceJob {
  import ConfigKeys._

  private val infoDateColumn: String = conf.getString(Keys.INFORMATION_DATE_COLUMN)
  private val infoDateFormatApp: String = conf.getString(Keys.INFORMATION_DATE_FORMAT_APP)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getReader(tableName: String): TableReader = {
    val pathOpt = tablesToSync.find(_.outputTable == tableName).map(_.path)
    pathOpt match {
      case Some(path) =>
        new TableReaderParquet(
          path,
          hasInfoDateColumn = true,
          infoDateColumn = infoDateColumn,
          infoDateFormat = infoDateFormatApp)
      case None =>
        throw new IllegalStateException(s"Path to table $tableName is not defined.")
    }
  }

  override def getWriter(tableName: String): TableWriter = {
    val topicOpt = tablesToSync.find(_.outputTable == tableName).map(_.topic)

    topicOpt match {
      case Some(topic) =>
        TableWriterKafka(topic, conf.getConfig(PARQUET_TO_KAFKA_SYNC_PREFIX))
      case None =>
        throw new IllegalStateException(s"Output topic for table $tableName is not defined.")
    }
  }

  // Note table names for analyzing dependencies are actually topic names
  override def getTables: Seq[String] = tablesToSync.map(_.outputTable)
}

object ParquetToKafkaSyncJob extends JobFactory[ParquetToKafkaSyncJob] {
  import ConfigKeys._

  override def apply(conf: Config, spark: SparkSession): ParquetToKafkaSyncJob = {
    val syncJobName = ConfigUtils.getOptionString(conf, PARQUET_TO_KAFKA_SYNC_JOB_NAME).getOrElse("JDBC to Parquet Sync")
    val schedule = Schedule.fromConfig(conf.getConfig(PARQUET_TO_KAFKA_SYNC_PREFIX))
    val syncTables = getTables(conf)

    new ParquetToKafkaSyncJob(syncJobName, schedule, syncTables)(spark, conf)
  }

  private def getTables(conf: Config): List[ParquetKafkaSyncTable] = {
    var i = 1
    val syncTables = new ListBuffer[ParquetKafkaSyncTable]
    while (conf.hasPath(s"$PARQUET_TO_KAFKA_SYNC_TABLE_PREFIX.$i")) {
      syncTables += getSyncTable(conf, i)
      i += 1
    }
    if (syncTables.isEmpty) {
      throw new IllegalArgumentException("No tables defined for synchronization. " +
        s"Please set $PARQUET_TO_KAFKA_SYNC_TABLE_PREFIX.1, etc.")
    }
    syncTables.toList
  }

  private def getSyncTable(conf: Config, n: Int): ParquetKafkaSyncTable = {
    val tableDefinition = conf.getStringList(s"$PARQUET_TO_KAFKA_SYNC_TABLE_PREFIX.$n").asScala.toArray

    if (tableDefinition.length == 3) {
      ParquetKafkaSyncTable(tableDefinition(0), tableDefinition(1), tableDefinition(2), tableDefinition(2))
    } else if (tableDefinition.length == 4) {
      ParquetKafkaSyncTable(tableDefinition(0), tableDefinition(1), tableDefinition(2), tableDefinition(3))
    } else {
      val lstStr = tableDefinition.mkString(", ")
      throw new IllegalArgumentException(s"Table definition for table $n is invalid. " +
        s"Expected: name, path, topic name [, outout_table]. Got $lstStr")
    }
  }
}
