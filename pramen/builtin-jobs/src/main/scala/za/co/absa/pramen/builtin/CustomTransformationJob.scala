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

package za.co.absa.pramen.builtin

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.builtin.model.{InputParquetTable, OutputParquetTable}
import za.co.absa.pramen.framework.config.Keys.{INFORMATION_DATE_COLUMN, INFORMATION_DATE_FORMAT_APP}
import za.co.absa.pramen.framework.config.WatcherConfig.OUTPUT_INFO_DATE_EXPR
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.reader.TableReaderParquet
import za.co.absa.pramen.framework.utils.{ClassLoaderUtils, ConfigUtils}
import za.co.absa.pramen.framework.writer.TableWriterParquet

import java.time.LocalDate
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

class CustomTransformationJob(job: SyncWatcherTransformationJob,
                              jobName: String,
                              schedule: Schedule,
                              inputTables: List[InputParquetTable],
                              outputTable: OutputParquetTable)
                             (implicit spark: SparkSession, conf: Config) extends za.co.absa.pramen.api.TransformationJob {
  def getUnderlyingJob: SyncWatcherTransformationJob = job

  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getDependencies: Seq[JobDependency] = JobDependency(inputTables.map(_.name), outputTable.name) :: Nil

  override def transformOutputInfoDate(infoDate: LocalDate): LocalDate = {
    outputTable.outputInfoDateExpression match {
      case Some(expr) =>
        val evaluator = new DateExprEvaluator
        evaluator.setValue("infoDate", infoDate)
        val result = evaluator.evalDate(expr)
        val q = "\""
        log.info(s"Given @infoDate = '$infoDate', $q$expr$q => '$result'")
        result
      case None =>
        infoDate
    }
  }

  override def getReader(tableName: String): TableReader = {
    inputTables.find(_.name == tableName) match {
      case Some(table) =>
        new TableReaderParquet(table.path,
          hasInfoDateColumn = true,
          infoDateColumn = table.infoDate.name,
          infoDateFormat = table.infoDate.format)
      case None =>
        throw new IllegalArgumentException(s"Input table $tableName is not defined.")
    }
  }

  override def getWriter(tableName: String): Option[TableWriter] = {
    val writer = new TableWriterParquet(outputTable.infoDate.name,
      outputTable.infoDate.format,
      outputTable.path,
      conf.getString("pramen.temporary.directory"),
      outputTable.recordsPerPartition)
    Option(writer)
  }

  override def runTask(inputTables: Seq[TableDataFrame],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): DataFrame = {
    job.runTask(inputTables, infoDateBegin, infoDateEnd, infoDateOutput) match {
      case Right(df) => df
      case Left(reason)       => reason match {
        case Reason.NotReady(msg) => throw new ReasonException(reason, msg)
        case Reason.Skip(msg)     => throw new ReasonException(reason, msg)
        case _                    => throw new ReasonException(reason, "Unexpected validation error")
      }
    }
  }
}

object CustomTransformationJob extends JobFactory[CustomTransformationJob] {
  private val log = LoggerFactory.getLogger(this.getClass)

  val PREFIX = "pramen.transformation"
  val INPUT_TABLE_PREFIX = s"$PREFIX.input.tables.table"
  val OUTPUT_TABLE_PREFIX = s"$PREFIX.output.table"
  val JOB_NAME = s"$PREFIX.job.name"
  val FACTORY_CLASS = s"$PREFIX.factory.class"
  val DEFAULT_INFO_DATE_COL_NAME = s"$PREFIX.default.information.date.column"
  val DEFAULT_INFO_DATE_COL_FORMAT = s"$PREFIX.default.information.date.format"

  override def apply(conf: Config, spark: SparkSession): CustomTransformationJob = {
    val defaultInfoColumnName = if (conf.hasPath(DEFAULT_INFO_DATE_COL_NAME)) {
      conf.getString(DEFAULT_INFO_DATE_COL_NAME)
    } else if (conf.hasPath(INFORMATION_DATE_COLUMN)) {
      conf.getString(INFORMATION_DATE_COLUMN)
    } else {
      throw new IllegalArgumentException(s"Either $DEFAULT_INFO_DATE_COL_NAME or $INFORMATION_DATE_COLUMN must be defined.")
    }

    val defaultInfoColumnFormat = if (conf.hasPath(DEFAULT_INFO_DATE_COL_FORMAT)) {
      conf.getString(DEFAULT_INFO_DATE_COL_FORMAT)
    } else if (conf.hasPath(INFORMATION_DATE_FORMAT_APP)) {
      conf.getString(INFORMATION_DATE_FORMAT_APP)
    } else {
      throw new IllegalArgumentException(s"Either $DEFAULT_INFO_DATE_COL_FORMAT or $INFORMATION_DATE_FORMAT_APP must be defined.")
    }

    val jobName = ConfigUtils.getOptionString(conf, JOB_NAME).getOrElse("Custom transformation job")
    val factoryClass = conf.getString(FACTORY_CLASS)
    val schedule = Schedule.fromConfig(conf.getConfig(PREFIX))
    val job = loadJobFromFactory(factoryClass, conf, spark)
    val inputTables = getInputTables(conf, defaultInfoColumnName, defaultInfoColumnFormat)
    val outputInfoDateExpr = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE_EXPR)
    val outputTable = OutputParquetTable.load(conf.getConfig(OUTPUT_TABLE_PREFIX), OUTPUT_TABLE_PREFIX, defaultInfoColumnName, defaultInfoColumnFormat, outputInfoDateExpr)

    new CustomTransformationJob(job, jobName, schedule, inputTables, outputTable)(spark, conf)
  }

  private def loadJobFromFactory(factoryName: String, conf: Config, spark: SparkSession): SyncWatcherTransformationJob = {
    val factory = ClassLoaderUtils.loadSingletonClassOfType[SyncWatcherJobFactory[SyncWatcherTransformationJob]](factoryName)
    val jobOpt = try {
      Option(factory.apply(conf, spark))
    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException(s"Unable to build a job using its factory: $factoryName", ex)
    }

    jobOpt match {
      case Some(job) => job
      case None => throw new IllegalArgumentException(s"Job factory returned Null: $factoryName")
    }
  }

  private def getInputTables(conf: Config, defaultInfoColumnName: String, defaultInfoColumnFormat: String): List[InputParquetTable] = {
    var i = 1
    val inputTables = new ListBuffer[InputParquetTable]
    while (conf.hasPath(s"$INPUT_TABLE_PREFIX.$i")) {
      inputTables += getInputTable(conf, i, defaultInfoColumnName, defaultInfoColumnFormat)
      i += 1
    }
    inputTables.toList
  }

  private def getInputTable(conf: Config, n: Int, defaultInfoColumnName: String, defaultInfoColumnFormat: String): InputParquetTable = {
    val path = s"$INPUT_TABLE_PREFIX.$n"
    val tableConfig = conf.getConfig(path)

    val table = InputParquetTable.load(tableConfig, path, defaultInfoColumnName, defaultInfoColumnFormat)
    log.info(s"Input table #$n: $table")
    table
  }


}
