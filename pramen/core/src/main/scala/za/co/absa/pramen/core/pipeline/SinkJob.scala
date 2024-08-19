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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{Reason, Sink}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.{MetaTableStats, Metastore}
import za.co.absa.pramen.core.pipeline.JobPreRunStatus.Ready
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}
import scala.util.Try
import scala.util.control.NonFatal

class SinkJob(operationDef: OperationDef,
              metastore: Metastore,
              bookkeeper: Bookkeeper,
              notificationTargets: Seq[JobNotificationTarget],
              outputTable: MetaTable,
              sink: Sink,
              sinkTable: SinkTable)
             (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, outputTable) {
  import JobBase._

  private val inputTables = operationDef.dependencies.flatMap(_.tables).distinct

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    val alreadyRanStatus = preRunTransformationCheck(infoDate, dependencyWarnings)

    alreadyRanStatus.status match {
      case JobPreRunStatus.Ready => JobPreRunResult(Ready, Some(getDataDf(infoDate).count()), dependencyWarnings, alreadyRanStatus.warnings)
      case _                     => alreadyRanStatus
    }
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    val minimumRecordsOpt = ConfigUtils.getOptionInt(sink.config, MINIMUM_RECORDS_KEY)

    minimumRecordsOpt.foreach(n => log.info(s"Minimum records to send: $n"))

    val df = getDataDf(infoDate)

    val inputRecordCount = df.count()

    minimumRecordsOpt match {
      case Some(min) =>
        if (inputRecordCount >= min) {
          Reason.Ready
        } else {
          Reason.NotReady(s"Not enough records to send. Got $inputRecordCount, expected at least $min records.")
        }
      case None      =>
        if (inputRecordCount > 0) {
          Reason.Ready
        } else {
          Reason.Skip("No records to send")
        }
    }
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    RunResult(getDataDf(infoDate))
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    try {
      val dfTransformed = applyTransformations(df, sinkTable.transformations)

      val (from, to) = getInfoDateRange(infoDate, sinkTable.rangeFromExpr, sinkTable.rangeToExpr)

      val dfFiltered = applyFilters(dfTransformed, sinkTable.filters, infoDate, from, to)

      val dfFinal = if (sinkTable.columns.nonEmpty) {
        dfFiltered.select(sinkTable.columns.head, sinkTable.columns.tail: _*)
      } else
        dfFiltered

      dfFinal
    } catch {
      case NonFatal(ex) => throw new IllegalStateException("Preprocessing failed on the sink.", ex)
    }
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    try {
      sink.connect()
    } catch {
      case NonFatal(ex) => throw new IllegalStateException("Unable to connect to the sink.", ex)
    }

    try {
      val sinkResult = sink.send(df,
        sinkTable.metaTableName,
        metastore.getMetastoreReader(List(sinkTable.metaTableName) ++ inputTables, infoDate),
        infoDate,
        sinkTable.options
      )
      val jobFinished = Instant.now

      val isTransient = outputTable.format.isTransient

      val tooLongWarnings = getTookTooLongWarnings(jobStarted, jobFinished, sinkTable.warnMaxExecutionTimeSeconds)

      bookkeeper.setRecordCount(outputTable.name,
        infoDate,
        infoDate,
        infoDate,
        inputRecordCount.getOrElse(sinkResult.recordsSent),
        sinkResult.recordsSent,
        jobStarted.getEpochSecond,
        jobFinished.getEpochSecond,
        isTransient
      )

      val stats = MetaTableStats(sinkResult.recordsSent, None)
      SaveResult(stats, sinkResult.filesSent, sinkResult.hiveTables, sinkResult.warnings ++ tooLongWarnings)
    } catch {
      case NonFatal(ex) => throw new IllegalStateException("Unable to write to the sink.", ex)
    } finally {
      Try {
        sink.close()
      }
    }
  }

  private def getDataDf(infoDate: LocalDate): DataFrame = {
    try {
      val (from, to) = getInfoDateRange(infoDate, sinkTable.rangeFromExpr, sinkTable.rangeToExpr)
      metastore.getTable(sinkTable.metaTableName, Option(from), Option(to))
    } catch {
      case NonFatal(ex) => throw new IllegalStateException(s"Unable to read input table ${sinkTable.metaTableName} for $infoDate.", ex)
    }
  }
}
