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

package za.co.absa.pramen.framework.validator

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.config.WatcherConfig.TRACK_UPDATES
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.model.DataChunk
import za.co.absa.pramen.framework.utils.ScheduleUtils

import java.time.{LocalDate, ZonedDateTime}
import scala.collection.mutable.ListBuffer

class DataAvailabilityValidator(checks: Seq[ValidationCheck], bk: SyncBookKeeper, trackUpdates: Boolean) extends SyncJobValidator {
  private val log = LoggerFactory.getLogger(this.getClass)

  def isEmpty: Boolean = checks.isEmpty

  def validateTask(infoDateBegin: LocalDate,
                   infoDateEnd: LocalDate,
                   infoDateOutput: LocalDate): Unit = {
    log.info(s"Checking if all necessary data to run the task is present for @infoDate=$infoDateOutput, @infoDateBegin=$infoDateBegin, @infoDateEnd=$infoDateEnd")

    val outdatedTables =
      checks.flatMap(check => getOutDatedTables(check, bk, infoDateBegin, infoDateEnd, infoDateOutput)).distinct

    if (outdatedTables.nonEmpty) {
      throw new IllegalStateException(s"Outdated tables: ${outdatedTables.mkString(", ")}")
    }
  }

  def decideRunTask(infoDateBegin: LocalDate,
                    infoDateEnd: LocalDate,
                    infoDateOutput: LocalDate,
                    outputTableName: String,
                    forceRun: Boolean): RunDecisionSummary = {
    log.info(s"Checking if need to run the task for @infoDate=$infoDateOutput, @infoDateBegin=$infoDateBegin, @infoDateEnd=$infoDateEnd")

    val inputDataChunks = checks.flatMap(check => getInputDataChunks(check, bk, infoDateBegin, infoDateEnd, infoDateOutput)).distinct

    bk.getLatestDataChunk(outputTableName, infoDateOutput, infoDateOutput) match {
      case Some(chunk) =>
        val updatedTables = getTablesHavingRecentUpdates(inputDataChunks, chunk.jobFinished)
        if (forceRun) {
          log.info(s"Forced table update.")
          RunDecisionSummary(RunDecision.RunUpdates,
            inputDataChunks.map(_.outputRecordCount).sum,
            Option(chunk.inputRecordCount),
            Option(chunk.outputRecordCount),
            Option(ScheduleUtils.fromEpochSecondToLocalDate(chunk.jobFinished)),
            updatedTables,
            Nil
          )
        } else if (updatedTables.isEmpty || !trackUpdates) {
          if (updatedTables.isEmpty) {
            log.info(s"The input tables haven't been changed since the last time.")
          } else {
            log.info(s"The following tables have changed, but tracking of updates is disabled, so the task will be skipped: ${updatedTables.mkString(", ")}")
          }

          RunDecisionSummary(RunDecision.SkipUpToDate,
            inputDataChunks.map(_.outputRecordCount).sum,
            Option(chunk.inputRecordCount),
            Option(chunk.outputRecordCount),
            Option(ScheduleUtils.fromEpochSecondToLocalDate(chunk.jobFinished)),
            Nil,
            Nil
          )
        } else {
          log.info(s"The following tables have changed: ${updatedTables.mkString(", ")}")
          RunDecisionSummary(RunDecision.RunUpdates,
            inputDataChunks.map(_.outputRecordCount).sum,
            Option(chunk.inputRecordCount),
            Option(chunk.outputRecordCount),
            Option(ScheduleUtils.fromEpochSecondToLocalDate(chunk.jobFinished)),
            updatedTables,
            Nil
          )
        }
      case None        =>
        log.info(s"This task hasn't been ran yet.")

        val outdatedTables =
          checks.flatMap(check => getOutDatedTables(check, bk, infoDateBegin, infoDateEnd, infoDateOutput)).distinct

        if (outdatedTables.isEmpty) {
          RunDecisionSummary(RunDecision.RunNew,
            inputDataChunks.map(_.outputRecordCount).sum,
            None,
            None,
            None,
            Nil,
            Nil
          )
        } else {
          log.info(s"No data for the following tables in order to run the task: ${outdatedTables.mkString(", ")}")
          RunDecisionSummary(RunDecision.SkipNoData,
            inputDataChunks.map(_.outputRecordCount).sum,
            None,
            None,
            None,
            Nil,
            outdatedTables
          )
        }
    }
  }

  def getOutDatedTables(check: ValidationCheck,
                        bk: SyncBookKeeper,
                        infoDateBegin: LocalDate,
                        infoDateEnd: LocalDate,
                        infoDateOutput: LocalDate): Seq[String] = {
    val evaluator = getEvaluator(infoDateBegin, infoDateEnd, infoDateOutput)

    val dateFrom = check.dataFromExpr.map(expr => logEval(evaluator, expr)).get
    val dateTo = check.dataToExpr.map(expr => logEval(evaluator, expr)).getOrElse(LocalDate.now())

    log.info(s"Validating date.from=>$dateFrom, date.to=$dateTo")

    val mandatoryTablesOutDated = check.tablesAll.filter(table => {
      val isUpToDate = bk.getDataChunks(table, dateFrom, dateTo).nonEmpty
      log.info(s"Table $table => ${statusOf(isUpToDate)}")
      !isUpToDate
    })

    val optionTablePresent = check.tablesOneOf.filter(table => {
      val isUpToDate = bk.getDataChunks(table, dateFrom, dateTo).nonEmpty
      log.info(s"Table $table => ${statusOf(isUpToDate)}")
      isUpToDate
    })

    val optionalTablesOutdated = if (optionTablePresent.isEmpty && check.tablesOneOf.nonEmpty) {
      check.tablesOneOf
    } else {
      Nil
    }

    mandatoryTablesOutDated ++ optionalTablesOutdated
  }

  def getTablesHavingRecentUpdates(inputTableChunks: Seq[DataChunk],
                                   outputTableLastUpdated: Long): Seq[(String, ZonedDateTime)] = {
    inputTableChunks.filter(chunk => {
      chunk.jobFinished > outputTableLastUpdated
    }).map(chunk => (chunk.tableName, ScheduleUtils.fromEpochSecondToLocalDate(chunk.jobFinished)))
  }

  def getInputDataChunks(check: ValidationCheck,
                         bk: SyncBookKeeper,
                         infoDateBegin: LocalDate,
                         infoDateEnd: LocalDate,
                         infoDateOutput: LocalDate): Seq[DataChunk] = {
    val evaluator = getEvaluator(infoDateBegin, infoDateEnd, infoDateOutput)

    val tables = check.tablesAll ++ check.tablesOneOf

    val dateFrom = check.dataFromExpr.map(expr => logEval(evaluator, expr)).get
    val dateTo = check.dataToExpr.map(expr => logEval(evaluator, expr)).getOrElse(LocalDate.now())

    log.info(s"Checking recent updates for date.from=>$dateFrom, date.to=$dateTo")

    tables.flatMap(table =>
      bk.getLatestDataChunk(table, dateFrom, dateTo)
    )
  }


  private def getEvaluator(infoDateBegin: LocalDate,
                           infoDateEnd: LocalDate,
                           infoDateOutput: LocalDate): DateExprEvaluator = {
    val evaluator = new DateExprEvaluator
    evaluator.setValue("infoDateBegin", infoDateBegin)
    evaluator.setValue("infoDateEnd", infoDateEnd)
    evaluator.setValue("infoDateOutput", infoDateOutput)
    evaluator.setValue("infoDate", infoDateOutput)
    evaluator
  }

  private def logEval(evaluator: DateExprEvaluator, expr: String): LocalDate = {
    val result = evaluator.evalDate(expr)
    log.info(s"Expr: '$expr' = '$result'")
    result
  }

  private def statusOf(isUpToDate: Boolean): String = {
    if (isUpToDate) {
      "Up to date"
    } else {
      "OUTDATED"
    }
  }
}

object DataAvailabilityValidator {
  val VALIDATION_CONFIG_PREFIX = "pramen.input.data.availability.check"

  def fromConfig(conf: Config, bk: SyncBookKeeper): DataAvailabilityValidator = {
    val checks = getValidationChecks(conf)
    val trackUpdates = conf.getBoolean(TRACK_UPDATES)
    new DataAvailabilityValidator(checks, bk, trackUpdates)
  }

  private def getValidationChecks(conf: Config): Seq[ValidationCheck] = {
    var i = 1
    val checks = new ListBuffer[ValidationCheck]
    while (conf.hasPath(s"$VALIDATION_CONFIG_PREFIX.$i")) {
      val parent = s"$VALIDATION_CONFIG_PREFIX.$i"
      checks += ValidationCheck.load(conf.getConfig(parent), parent)
      i += 1
    }
    checks
  }
}
