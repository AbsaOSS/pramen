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
import za.co.absa.pramen.builtin.process.ProcessRunner
import za.co.absa.pramen.config.ConfigKeys
import za.co.absa.pramen.framework.exceptions.{CmdFailedException, ReasonException}
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.utils.{ConfigUtils, StringUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CmdLineJob(jobName: String,
                 schedule: Schedule,
                 inputTables: List[String],
                 outputTable: String,
                 outputInfoDateDelay: Int,
                 outputRecordCountRegEx: String,
                 zeroRecordSuccessRegEx: Option[String],
                 noDataFailureRegEx: Option[String],
                 outputFiltersRegEx: Seq[String],
                 cmd: String,
                 numberOfLogLinesToInclude: Int)
                (implicit spark: SparkSession, conf: Config) extends AggregationJob {

  import CmdLineJob._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getDependencies: Seq[JobDependency] = JobDependency(inputTables, outputTable) :: Nil

  override def transformOutputInfoDate(infoDate: LocalDate): LocalDate = infoDate.minusDays(outputInfoDateDelay)

  override def runTask(inputTables: Seq[String],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): Option[Long] = {

    // This is the former way
    val substitutedCmd1 = StringUtils.substituteVars(cmd,
      "pramen.period.start" -> infoDateBegin.format(dateFormatter) ::
        "pramen.period.end" -> infoDateEnd.format(dateFormatter) ::
        "pramen.period.output" -> infoDateOutput.format(dateFormatter) ::
        "pramen.output.month" -> infoDateOutput.format(monthFormatter) ::
        Nil)

    val evaluator = new DateExprEvaluator
    evaluator.setValue("infoDate", infoDateOutput)
    evaluator.setValue("infoDateBegin", infoDateBegin)
    evaluator.setValue("infoDateEnd", infoDateEnd)
    evaluator.setValue("infoDateOutput", infoDateEnd)

    // This is the new way
    val substitutedCmd2 = StringUtils.substituteVarsNew(substitutedCmd1, evaluator)

    val runner = new ProcessRunner(substitutedCmd2,
      outputRecordCountRegEx,
      zeroRecordSuccessRegEx,
      noDataFailureRegEx,
      outputFiltersRegEx,
      numberOfLogLinesToInclude)

    val exitStatus = runner.run()

    val recordCountOpt = runner.getRecordCount

    if (runner.isFailedNoData) {
      val msg = runner.getFailedLine
      throw new ReasonException(Reason.NotReady(msg), msg)
    }

    if (exitStatus != 0) {
      throw CmdFailedException(s"The job exited with exit status $exitStatus.", runner.getCmdLog)
    }

    if (recordCountOpt.isEmpty && outputRecordCountRegEx.nonEmpty) {
      throw CmdFailedException(s"The job returned 0 exit status, but didn't contain the number of records written. " +
        s"The job has possibly failed.", runner.getCmdLog)
    }

    recordCountOpt
  }

}

object CmdLineJob extends JobFactory[CmdLineJob] {
  import ConfigKeys._

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM")

  override def apply(conf: Config, spark: SparkSession): CmdLineJob = {
    val syncJobName = conf.getString(CMD_LINE_JOB_NAME)
    val schedule = Schedule.fromConfig(conf.getConfig(CMD_LINE_PREFIX))
    val inputTables = conf.getStringList(CMD_LINE_INPUT_TABLES).asScala.toList
    val outputTable = conf.getString(CMD_LINE_OUTPUT_TABLE)
    val command = conf.getString(CMD_LINE_COMMAND)
    val outputInfoDateDelay = conf.getInt(CMD_LINE_OUTPUT_INFO_DATE_DELAY)
    val logLinesToInclude = conf.getInt(CMD_LINE_LOGS_TO_INCLUDE)
    val outputRecordCountRegEx = conf.getString(CMD_LINE_OUTPUT_RECORDS_REGEX)
    val zeroRecordSuccessRegEx = ConfigUtils.getOptionString(conf, CMD_LINE_OUTPUT_ZERO_RECORD_SUCCESS_REGEX)
    val noDataFailureRegEx = ConfigUtils.getOptionString(conf, CMD_LINE_OUTPUT_FAILURE_NO_DATA)

    val outputFilters = new ListBuffer[String]
    var i = 1
    while (conf.hasPath(s"$CMD_LINE_OUTPUT_FILTER_REGEX.$i")) {
      outputFilters += conf.getString(s"$CMD_LINE_OUTPUT_FILTER_REGEX.$i")
      i += 1
    }

    new CmdLineJob(syncJobName,
      schedule,
      inputTables,
      outputTable,
      outputInfoDateDelay,
      outputRecordCountRegEx,
      zeroRecordSuccessRegEx,
      noDataFailureRegEx,
      outputFilters,
      command,
      logLinesToInclude)(spark, conf)
  }
}




