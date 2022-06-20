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
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.utils.{ClassLoaderUtils, ConfigUtils}

import java.time.LocalDate
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class CustomAggregationJob(job: SyncWatcherAggregationJob,
                           jobName: String,
                           schedule: Schedule,
                           inputTableNames: Seq[String],
                           outputTableName: String,
                           outputInfoDateExpression: Option[String],
                           bk: SyncBookKeeper)
                          (implicit spark: SparkSession, conf: Config) extends za.co.absa.pramen.api.AggregationJob {
  def getUnderlyingJob: SyncWatcherAggregationJob = job

  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getDependencies: Seq[JobDependency] = JobDependency(inputTableNames, outputTableName) :: Nil

  override def transformOutputInfoDate(infoDate: LocalDate): LocalDate = {
    outputInfoDateExpression match {
      case Some(expr) =>
        val evaluator = new DateExprEvaluator
        evaluator.setValue("infoDate", infoDate)
        val result = evaluator.evalDate(expr)
        val q = "\""
        log.info(s"Given @activeDate = '$infoDate', $q$expr$q => '$result'")
        result
      case None       =>
        infoDate
    }
  }

  override def runTask(inputTables: Seq[String],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): Option[Long] = {
    val dep = inputTables.map(table => {
      TaskDependency(table, bk.getLatestProcessedDate(table, Some(infoDateEnd))
        .getOrElse(LocalDate.of(1970, 1, 1)))
    })
    job.runTask(dep, infoDateBegin, infoDateEnd, infoDateOutput) match {
      case Right(recordCount) => Some(recordCount)
      case Left(reason)       => reason match {
        case Reason.NotReady(msg) => throw new ReasonException(reason, msg)
        case Reason.Skip(msg)     => throw new ReasonException(reason, msg)
        case _                    => throw new ReasonException(reason, "Unexpected validation error")
      }
    }
  }

}

object CustomAggregationJob extends JobFactory[CustomAggregationJob] {
  private val log = LoggerFactory.getLogger(this.getClass)

  val PREFIX = "pramen.aggregation"
  val INPUT_TABLES = s"$PREFIX.input.tables"
  val OUTPUT_TABLE = s"$PREFIX.output.table"
  val OUTPUT_INFO_DATE = "pramen.output.info.date.expr"
  val JOB_NAME = s"$PREFIX.job.name"
  val FACTORY_CLASS = s"$PREFIX.factory.class"

  override def apply(conf: Config, spark: SparkSession): CustomAggregationJob = {
    val jobName = ConfigUtils.getOptionString(conf, JOB_NAME).getOrElse("Custom transformation job")
    val factoryClass = conf.getString(FACTORY_CLASS)
    val schedule = Schedule.fromConfig(conf.getConfig(PREFIX))
    val job = loadJobFromFactory(factoryClass, conf, spark)
    val inputTables = conf.getStringList(INPUT_TABLES).asScala
    val outputTable = conf.getString(OUTPUT_TABLE)
    val outputInfoDate = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE)

    val bk: SyncBookKeeper = AppContextFactory.get.bookkeeper

    new CustomAggregationJob(job,
      jobName,
      schedule,
      inputTables,
      outputTable,
      outputInfoDate,
      bk)(spark, conf)
  }

  private def loadJobFromFactory(factoryName: String, conf: Config, spark: SparkSession): SyncWatcherAggregationJob = {
    val factory = ClassLoaderUtils.loadSingletonClassOfType[SyncWatcherJobFactory[SyncWatcherAggregationJob]](factoryName)
    val jobOpt = try {
      Option(factory.apply(conf, spark))
    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException(s"Unable to build a job using its factory: $factoryName", ex)
    }

    jobOpt match {
      case Some(job) => job
      case None      => throw new IllegalArgumentException(s"Job factory returned Null: $factoryName")
    }
  }

}

