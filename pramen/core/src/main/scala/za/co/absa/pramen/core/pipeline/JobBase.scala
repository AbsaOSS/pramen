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
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.{MetaTable, MetastoreDependency}
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.utils.Emoji._

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

abstract class JobBase(operationDef: OperationDef,
                       metastore: Metastore,
                       bookkeeper: Bookkeeper,
                       jobNotificationTargets: Seq[JobNotificationTarget],
                       outputTableDef: MetaTable
                      ) extends Job {
  private val log = LoggerFactory.getLogger(this.getClass)

  override val name: String = operationDef.name

  override val outputTable: MetaTable = outputTableDef

  override val operation: OperationDef = operationDef

  override val allowRunningTasksInParallel: Boolean = operationDef.allowParallel && !hasSelfDependencies

  override def notificationTargets: Seq[JobNotificationTarget] = jobNotificationTargets

  def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult

  final override def preRunCheck(infoDate: LocalDate,
                                 conf: Config): JobPreRunResult = {
    val validationFailures = operationDef.dependencies.flatMap(dependency => {
      checkDependency(dependency, infoDate)
    })

    val dependencyErrors = validationFailures.filter(!_.dep.isOptional)

    val dependencyWarnings = validationFailures
      .filter(_.dep.isOptional)
      .flatMap(failure => failure.failedTables)
      .sortBy(identity)
      .map(table => DependencyWarning(table))

    if (dependencyErrors.nonEmpty) {
      log.warn(s"Job for table ${outputTableDef.name} at $infoDate has validation failures.")
      val isFailure = dependencyErrors.exists(!_.dep.isPassive)
      JobPreRunResult(JobPreRunStatus.FailedDependencies(isFailure, dependencyErrors), None, dependencyWarnings)
    } else {
      if (dependencyWarnings.nonEmpty) {
        log.info(s"Job for table ${outputTableDef.name} at $infoDate has validation warnings: ${dependencyWarnings.map(_.table).mkString(", ")}.")
      } else {
        log.info(s"Job for table ${outputTableDef.name} at $infoDate has no validation failures.")
      }

      preRunCheckJob(infoDate, conf, dependencyWarnings)
    }
  }

  protected def preRunTransformationCheck(infoDate: LocalDate, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    validateTransformationAlreadyRanCases(infoDate, dependencyWarnings) match {
      case Some(result) => result
      case None => JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings)
    }
  }

  protected def validateTransformationAlreadyRanCases(infoDate: LocalDate, dependencyWarnings: Seq[DependencyWarning]): Option[JobPreRunResult] = {
    if (bookkeeper.getLatestDataChunk(outputTableDef.name, infoDate, infoDate).isDefined) {
      log.info(s"Job for table ${outputTableDef.name} as already ran for $infoDate.")
      Some(JobPreRunResult(JobPreRunStatus.AlreadyRan, None, dependencyWarnings))
    } else {
      log.info(s"Job for table ${outputTableDef.name} has not yet ran $infoDate.")
      None
    }
  }

  protected def checkDependency(dep: MetastoreDependency, infoDate: LocalDate): Option[DependencyFailure] = {
    val evaluator = new DateExprEvaluator
    evaluator.setValue("infoDate", infoDate)

    val dateFrom = evaluator.evalDate(dep.dateFromExpr)
    val dateUntilOpt = dep.dateUntilExpr.map(dateUntilExpr => evaluator.evalDate(dateUntilExpr))

    val q = '\"'
    log.info(s"Given @infoDate = '$infoDate', $q${dep.dateFromExpr}$q => infoDate = '$dateFrom'")
    dateUntilOpt.foreach(dateUntil => log.info(s"Given @infoDate = '$infoDate', $q${dep.dateUntilExpr.get}$q => infoDate = '$dateUntil'"))

    val range = dateUntilOpt match {
      case Some(dateUntil) => s"from '$dateFrom' to '$dateUntil''"
      case None            => s"from '$dateFrom'"
    }

    log.info(s"Validating @infoDate $range")

    val failures = dep.tables.flatMap(table => {
      val isAvailable = metastore.isDataAvailable(table, Option(dateFrom), dateUntilOpt)
      if (!isAvailable) {
        if (metastore.isDataAvailable(table, None, None)) {
          log.warn(s"$WARNING No data found for '$table' $range.")
          Some(Some(table), None)
        } else {
          log.warn(s"$STAR Empty input table (no bookkeeping information) for '$table'.")
          Some(None, Some(table))
        }
      } else {
        None
      }
    })

    val failedTables = failures.flatMap(_._1)
    val emptyTables = failures.flatMap(_._2)
    val failedDateRanges = failedTables.map(_ => range)

    if (failedTables.isEmpty && emptyTables.isEmpty) {
      None
    } else {
      Some(pipeline.DependencyFailure(dep, emptyTables, failedTables, failedDateRanges))
    }
  }

  private[core] def hasSelfDependencies: Boolean = {
    operationDef.dependencies.exists(_.tables.contains(outputTableDef.name))
  }

  private[core] def getInfoDateRange(infoDate: LocalDate, fromExpr: Option[String], toExpr: Option[String]): (LocalDate, LocalDate) = {
    val evaluator = new DateExprEvaluator
    evaluator.setValue("infoDate", infoDate)
    evaluator.setValue("date", infoDate)

    val fromDate = fromExpr.map(expr => {
      evaluator.evalDate(expr)
    })

    val fromTo = toExpr.map(expr => {
      evaluator.evalDate(expr)
    })

    val (effectiveFrom, effectiveTo) = (fromDate, fromTo) match {
      case (None, None) => (infoDate, infoDate)
      case (Some(from), None) => (from, infoDate)
      case (None, Some(to)) => (infoDate, to)
      case (Some(from), Some(to)) => (from, to)
    }

    if (effectiveTo.isBefore(effectiveFrom)) {
      throw new IllegalArgumentException(s"Incorrect date range specified for ${outputTable.name}: from=$effectiveFrom > to=$effectiveTo.")
    }

    log.info(s"Input date range for ${outputTable.name}: from $effectiveFrom to $effectiveTo")

    (effectiveFrom, effectiveTo)
  }

  override def createOrRefreshHiveTable(schema: StructType, infoDate: LocalDate, recreate: Boolean): Seq[String] = {
    if (outputTableDef.hiveTable.isEmpty)
      return Seq.empty

    val hiveHelper = metastore.getHiveHelper(outputTableDef.name)

    val attempt = Try {
      metastore.repairOrCreateHiveTable(outputTableDef.name, infoDate, Option(schema), hiveHelper, recreate)
    }

    attempt match {
      case Success(_)  => Seq.empty
      case Failure(ex) =>
        if (outputTableDef.hiveConfig.ignoreFailures) {
          val cause = if (ex.getCause != null) s" ${ex.getCause.getMessage}" else ""
          val msg = s"Failed to create or repair Hive table '${outputTableDef.hiveTable.get}': ${ex.getMessage}$cause"
          log.error(s"$FAILURE $msg")
          Seq(msg)
        } else {
          throw ex
        }
    }
  }
}

object JobBase {
  val MINIMUM_RECORDS_KEY = "minimum.records"
  val FAIL_NO_DATA_KEY = "fail.if.no.data"
  val MINIMUM_RECORDS_DEFAULT = 1
}
