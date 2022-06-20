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

package za.co.absa.pramen.framework.runner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.config.WatcherConfig
import za.co.absa.pramen.framework.config.WatcherConfig.{CURRENT_DATE, EXPECTED_DELAY_DAYS, TRACK_DAYS}
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.lock.{TokenLock, TokenLockFactory}
import za.co.absa.pramen.framework.model._
import za.co.absa.pramen.framework.notify.{SchemaDifference, TaskCompleted}
import za.co.absa.pramen.framework.state.RunState
import za.co.absa.pramen.framework.utils.{ScheduleUtils, SparkUtils, TimeUtils}
import za.co.absa.pramen.framework.validator.{DataAvailabilityValidator, RunDecision, SyncJobValidator, ValidationCheck}

import java.time._
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class JobCoordinator(pramenConfig: WatcherConfig,
                     tokenFactory: TokenLockFactory,
                     bookKeeper: SyncBookKeeper,
                     runState: RunState,
                     validator: SyncJobValidator,
                     activeInfoDate: LocalDate = LocalDate.now())
                    (implicit spark: SparkSession) extends JobRunner {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val dateFormatter = DateTimeFormatter.ofPattern(pramenConfig.infoDateFormat)
  private val timestampFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm Z")
  private val zoneId = ZoneId.systemDefault()
  private val isForcedRerun = pramenConfig.rerunInfoDate.nonEmpty
  private val attemptDelaySeconds = 10

  override def runJob(job: Job): Unit = {
    log.info(s"Running ${job.name} at $activeInfoDate...")
    job match {
      case job: SourceJob if pramenConfig.loadDateTo.nonEmpty => runSourceHistoryJob(job)
      case job: SourceJob => runSourceJob(job)
      case job: TransformationJob => runTransformationJob(job)
      case job: AggregationJob => runAggregationJob(job)
      case job => throw new IllegalArgumentException(s"Unsupported job type for class ${job.getClass.getCanonicalName}.")
    }
  }

  def runSourceJob(job: SourceJob): Unit = {
    val tasks = getFilteredDependencies(job)
    tasks.foreach(task => {
      val datesForEachTable = getInfoDatesToCheck(job, task)

      if (datesForEachTable.nonEmpty) {
        val tableReaders = task
          .inputTables
          .map(table => {
            (table, job.getReader(table))
          })

        val lock = acquireTableLock(task.outputTable)

        try {
          val writer = job.getWriter(task.outputTable)
          val subTasks = getSubTasksBasedOnRecordCount(job, datesForEachTable, task, tableReaders.map(_._2))
          val latestCalculated = bookKeeper.getLatestProcessedDate(task.outputTable, Option(activeInfoDate))
          val datesToProcess = job.selectInfoDates(subTasks.map(_.infoDateBegin), latestCalculated)

          runTransformationSubTasks(job, task, subTasks, tableReaders, datesToProcess, Option(writer))
        } finally {
          lock.release()
          lock.close()
        }
      }
    })
  }

  def runSourceHistoryJob(job: SourceJob): Unit = {
    val tasks = getFilteredDependencies(job)
    tasks.foreach(task => {
      val datesForEachTable = getInfoDatesToCheck(job, task).reverse

      val tableReaders = task
        .inputTables
        .map(table => {
          (table, job.getReader(table))
        })

      val lock = acquireTableLock(task.outputTable)

      try {
        val writer = job.getWriter(task.outputTable)

        datesForEachTable.foreach(day => {
          val datesToProcess = Array(day)
          val subTasks = getSubTasksBasedOnRecordCount(job, datesToProcess, task, tableReaders.map(_._2))

          runTransformationSubTasks(job, task, subTasks, tableReaders, datesToProcess, Option(writer))
        })
      } finally {
        lock.release()
        lock.close()
      }

    })
  }

  def runAggregationJob(job: AggregationJob): Unit = {
    val tasks = getFilteredDependencies(job)
    tasks.foreach(task => {
      val datesForEachTable = getInfoDatesToCheck(job, task)
      val subTasks = getSubTasksBasedOnDates(job, datesForEachTable, task)
      val latestCalculated = bookKeeper.getLatestProcessedDate(task.outputTable, Option(activeInfoDate))
      val datesToProcess = job.selectInfoDates(subTasks.map(_.infoDateBegin), latestCalculated)

      if (subTasks.nonEmpty) {
        val lock = acquireTableLock(task.outputTable)

        try {
          subTasks.foreach(subTask => {
            runAggregationTask(job, task, datesToProcess, subTask)
          })
        } finally {
          lock.release()
          lock.close()
        }
      } else {
        log.info(s"Nothing to do. Skipping the job.")
      }
    })
  }

  private def runAggregationTask(job: AggregationJob, task: JobDependency, datesToProcess: Array[LocalDate], subTask: SubTask): Unit = {
    val dayStr = dateFormatter.format(subTask.infoDateOutput)

    val startDepsCheck = Instant.now
    if (!datesToProcess.contains(subTask.infoDateBegin)) {
      log.info(s"For job '${job.name}' skipping OUTDATED data from $dayStr according to the job configuration...")
      val now = Instant.now

      if (!pramenConfig.dryRun && !pramenConfig.undercover) {
        bookKeeper.setRecordCount(task.outputTable,
          subTask.infoDateOutput,
          subTask.infoDateBegin,
          subTask.infoDateEnd,
          subTask.inputRecordCount,
          0,
          now.getEpochSecond,
          now.getEpochSecond)
      }

      val finishDepsCheck = Instant.now

      runState.addCompletedTask(TaskCompleted(job.name,
        task.outputTable,
        subTask.infoDateBegin,
        subTask.infoDateEnd,
        subTask.infoDateOutput,
        subTask.inputRecordCount,
        0L,
        None,
        None,
        None,
        startDepsCheck.getEpochSecond,
        finishDepsCheck.getEpochSecond,
        TaskStatus.SKIPPED.toString,
        None))

    } else {
      log.info(s"Running job '${job.name}' for ${task.outputTable} at $dayStr...")

      val jobValidationResult = Try(job.validateTask(subTask.dependencies,
        subTask.infoDateBegin,
        subTask.infoDateEnd,
        subTask.infoDateOutput))

      val validationResult: Try[Unit] = if (jobValidationResult.isSuccess) {
        Try(validator.validateTask(subTask.infoDateBegin, subTask.infoDateEnd, subTask.infoDateOutput))
      } else {
        jobValidationResult
      }

      var outputRecordCountOpt: Option[Long] = None

      val start = Instant.now
      val runResult: Try[Unit] = if (validationResult.isSuccess) {
        if (pramenConfig.dryRun) {
          log.warn(s"Dry run mode - the job won't be actually run.")
          Success(None)
        } else {
          val result = Try(job.runTask(subTask.dependencies.map(_.inputTable),
            subTask.infoDateBegin, subTask.infoDateEnd, subTask.infoDateOutput))
          result match {
            case Success(recordCountWrittenOpt) =>
              outputRecordCountOpt = recordCountWrittenOpt
              Success[Unit](())
            case Failure(ex) =>
              ex match {
                case e: ReasonException =>
                  // Pass the reason for further processing
                  Failure[Unit](e)
                case NonFatal(e) =>
                  // Rethrow all other exception
                  throw e
              }
          }
        }
      } else {
        validationResult
      }
      val finish = Instant.now

      runResult match {
        case _: Success[_] =>
          val elapsedTimeStr = s"Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}"

          outputRecordCountOpt match {
            case Some(n) => log.info(s"The job '${job.name}' for ${task.outputTable} at $dayStr has succeeded with $n output records. $elapsedTimeStr")
            case None => log.info(s"The job '${job.name}' for ${task.outputTable} at $dayStr has succeeded. $elapsedTimeStr")
          }

          if (!pramenConfig.dryRun && !pramenConfig.undercover) {
            bookKeeper.setRecordCount(task.outputTable,
              subTask.infoDateOutput,
              subTask.infoDateBegin,
              subTask.infoDateEnd,
              subTask.inputRecordCount,
              outputRecordCountOpt.getOrElse(0L),
              start.getEpochSecond,
              finish.getEpochSecond)
          }

          runState.addCompletedTask(TaskCompleted(job.name,
            task.outputTable,
            subTask.infoDateBegin,
            subTask.infoDateEnd,
            subTask.infoDateOutput,
            subTask.inputRecordCount,
            subTask.inputRecordCountOld,
            outputRecordCountOpt,
            subTask.outputRecordCountOld,
            None,
            start.getEpochSecond,
            finish.getEpochSecond,
            subTask.status,
            None))
        case Failure(ex) =>
          log.info(s"Job '${job.name}' for ${task.outputTable} at $dayStr - NOT READY, skipping for now...")
          val finishDepsCheck = Instant.now

          runState.addCompletedTask(TaskCompleted(job.name,
            task.outputTable,
            subTask.infoDateBegin,
            subTask.infoDateEnd,
            subTask.infoDateOutput,
            subTask.inputRecordCount,
            0,
            None,
            None,
            None,
            startDepsCheck.getEpochSecond,
            finishDepsCheck.getEpochSecond,
            TaskStatus.NOT_READY.toString,
            Option(ex.getMessage)))
      }
    }
  }

  def runTransformationJob(job: TransformationJob): Unit = {
    val tasks = getFilteredDependencies(job)
    tasks.foreach(task => {
      val datesForEachTable = getInfoDatesToCheck(job, task)

      val lazyRead = job.isInstanceOf[LazyReadJob]

      val tableReaders = if (lazyRead) {
        Seq.empty[(String, TableReader)]
      } else {
        task
          .inputTables
          .map(table => {
            (table, job.getReader(table))
          })
      }

      val lock = acquireTableLock(task.outputTable)

      try {
        val writerOpt = job.getWriter(task.outputTable)
        val subTasks = getSubTasksBasedOnDates(job, datesForEachTable, task)
        val latestCalculated = bookKeeper.getLatestProcessedDate(task.outputTable, Option(activeInfoDate))
        val datesToProcess = job.selectInfoDates(subTasks.map(_.infoDateBegin), latestCalculated)

        runTransformationSubTasks(job, task, subTasks, tableReaders, datesToProcess, writerOpt)
      } finally {
        lock.release()
        lock.close()
      }
    })
  }

  private def getInfoDatesToCheck(job: Job, task: JobDependency): Array[LocalDate] = {
    val schedule = job.getSchedule

    val dates = pramenConfig.rerunInfoDate match {
      case Some(rerunDate) =>
        Array(rerunDate)
      case None =>
        List(task.outputTable)
          .flatMap(inputTable => {
            val lastUpdateOpt = if (pramenConfig.ignoreLastUpdatedDate) {
              val eldestDate = Some(activeInfoDate.minusDays(pramenConfig.trackDays).minusDays(pramenConfig.expectedDelayDays).minusDays(1))
              log.info(s"Table: $inputTable, the most recent date considered to have data: ${eldestDate.get} = $CURRENT_DATE($activeInfoDate) - " +
                s"$EXPECTED_DELAY_DAYS(${pramenConfig.expectedDelayDays}) - $TRACK_DAYS(${pramenConfig.trackDays}) - 1 day")
              eldestDate
            } else {
              val lastUpdatedDate = bookKeeper.getLatestProcessedDate(inputTable, Option(activeInfoDate))
              log.info(s"Table: $inputTable, last update: $lastUpdatedDate")
              lastUpdatedDate
            }

            val dates = if (pramenConfig.loadDateTo.isDefined) {
              log.info(s"Loading historical data from ${pramenConfig.infoDateStart} to ${pramenConfig.loadDateTo.get}...")
              ScheduleUtils.getActiveDatesForPeriod(schedule, pramenConfig.infoDateStart, pramenConfig.loadDateTo.get)
            } else if (pramenConfig.checkOnlyNewData) {
              log.info(s"Checking only new and late data...")
              ScheduleUtils.getInformationDatesToCheck(schedule,
                activeInfoDate,
                lastUpdateOpt.getOrElse(pramenConfig.infoDateStart),
                pramenConfig.infoDateStart,
                pramenConfig.expectedDelayDays,
                0,
                bookKeeper.bookkeepingEnabled)
            } else if (pramenConfig.checkOnlyLateData) {
              log.info(s"Checking only late data...")
              ScheduleUtils.getLateInformationDatesToCheck(schedule,
                activeInfoDate,
                lastUpdateOpt.getOrElse(pramenConfig.infoDateStart),
                pramenConfig.infoDateStart,
                pramenConfig.expectedDelayDays)
            } else {
              ScheduleUtils.getInformationDatesToCheck(schedule,
                activeInfoDate,
                lastUpdateOpt.getOrElse(pramenConfig.infoDateStart),
                pramenConfig.infoDateStart,
                pramenConfig.expectedDelayDays,
                pramenConfig.trackDays,
                bookKeeper.bookkeepingEnabled)
            }

            val latestDate = ScheduleUtils.getRecentActiveDay(schedule, activeInfoDate)
            if (pramenConfig.alwaysOverwriteLastChunk && !dates.contains(latestDate)) {
              log.info(s"Adding $latestDate...")
              dates :+ latestDate
            } else {
              dates
            }
          })
          .filter(day => job.getSchedule.isEnabled(day))
          .toArray
          .distinct
          .sortBy(_.toEpochDay)
    }

    if (dates.isEmpty) {
      log.info(s"Dates to check: NOTHING (out of scheduled period)")
    } else {
      log.info(s"Dates to check: ${dates.mkString(",")}")
    }

    dates
  }

  private def runTransformationSubTasks(job: Job,
                                        task: JobDependency,
                                        subTasks: Array[SubTask],
                                        tableReaders: Seq[(String, TableReader)],
                                        datesToProcess: Array[LocalDate],
                                        writerOpt: Option[TableWriter]): Unit = {
    subTasks.foreach(subTask => {
      runTransformationTasks(job, task, subTask, tableReaders, datesToProcess, writerOpt)
    })
  }

  private def runTransformationTasks(job: Job,
                                     task: JobDependency,
                                     subTask: SubTask,
                                     tableReaders: Seq[(String, TableReader)],
                                     datesToProcess: Array[LocalDate],
                                     writerOpt: Option[TableWriter]): Unit = {

    val dayStr = dateFormatter.format(subTask.infoDateOutput)
    val startDepsCheck = Instant.now

    if (!datesToProcess.contains(subTask.infoDateBegin) && pramenConfig.loadDateTo.isEmpty) {
      log.info(s"For job '${job.name}' skipping $dayStr according to the job configuration...")
      val now = Instant.now

      if (!pramenConfig.dryRun && !pramenConfig.undercover) {
        bookKeeper.setRecordCount(task.outputTable,
          subTask.infoDateOutput,
          subTask.infoDateBegin,
          subTask.infoDateEnd,
          subTask.inputRecordCount,
          0,
          now.getEpochSecond,
          now.getEpochSecond)
      }

      val finishDepsCheck = Instant.now

      runState.addCompletedTask(TaskCompleted(job.name,
        task.outputTable,
        subTask.infoDateBegin,
        subTask.infoDateEnd,
        subTask.infoDateOutput,
        subTask.inputRecordCount,
        0,
        None,
        None,
        None,
        startDepsCheck.getEpochSecond,
        finishDepsCheck.getEpochSecond,
        TaskStatus.SKIPPED.toString,
        None))
    } else {
      log.info(s"Running job '${job.name}' for ${task.outputTable} at $dayStr...")
      val start = Instant.now

      val inputTables = tableReaders.flatMap {
        case (tableName, rd) =>
          rd.getData(subTask.infoDateBegin, subTask.infoDateEnd).map(data => TableDataFrame(tableName, data))
      }

      val lazyRead = job.isInstanceOf[LazyReadJob]

      val failureReason = Try(job.validateTask(subTask.dependencies,
        subTask.infoDateBegin,
        subTask.infoDateEnd,
        subTask.infoDateOutput)) match {
        case _: Success[Unit] =>
          if (inputTables.isEmpty && task.inputTables.nonEmpty && !lazyRead) {
            Some(s"Cannot read input table(s): ${task.inputTables.mkString(", ")}")
          } else None
        case Failure(ex) =>
          Option(ex.getMessage)
      }

      if (failureReason.nonEmpty) {
        log.info(s"Job '${job.name}' for ${task.outputTable} at $dayStr - NOT READY, skipping for now...")
        val finishDepsCheck = Instant.now

        runState.addCompletedTask(TaskCompleted(job.name,
          task.outputTable,
          subTask.infoDateBegin,
          subTask.infoDateEnd,
          subTask.infoDateOutput,
          subTask.inputRecordCount,
          0,
          None,
          None,
          None,
          startDepsCheck.getEpochSecond,
          finishDepsCheck.getEpochSecond,
          TaskStatus.NOT_READY.toString,
          failureReason))
      } else {
        val dfOut = job match {
          case j: SourceJob =>
            if (inputTables.isEmpty) {
              throw new IllegalStateException(s"No input tables provided for the synchronization job (info date = $dayStr).")
            } else if (inputTables.size > 1) {
              val tables = inputTables.map(_.tableName).mkString(", ")
              throw new IllegalStateException(s"More than one input table provided for the synchronization job ($tables) " +
                s"info date = $dayStr.")
            }
            j.runTask(inputTables.head, subTask.infoDateBegin, subTask.infoDateEnd, subTask.infoDateOutput)
          case j: TransformationJob =>
            j.runTask(inputTables, subTask.infoDateBegin, subTask.infoDateEnd, subTask.infoDateOutput)
          case x => throw new IllegalStateException(s"Unexpected job type: ${x.getClass.getCanonicalName}.")
        }

        val isSourcingTask = job.isInstanceOf[SourceJob]

        validateOutputSchema(task.outputTable, dfOut, subTask.infoDateOutput)

        val dfOutWithSchema = job match {
          case tj: SchemaTransformJob =>
            val df = tj.schemaTransformation(
              TableDataFrame(task.outputTable, dfOut),
              subTask.infoDateOutput)
            validateOutputSchema(s"${task.outputTable}(transformed)", df, subTask.infoDateOutput)
            df
          case _                      => dfOut
        }

        val outputRecordCount = multiTryWrite(dfOutWithSchema, writerOpt, subTask.infoDateOutput, subTask.inputRecordCount, isSourcingTask)

        val outputSize = writerOpt.flatMap(_.getMetadata(Constants.METADATA_LAST_SIZE_WRITTEN).map(_.asInstanceOf[Long]))
        log.info(s"outputSize=$outputSize")

        val finish = Instant.now

        val elapsedTimeStr = s"Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}"

        val (status, reason) = if (isSourcingTask && outputRecordCount == 0L && subTask.inputRecordCount > 0L) {
          log.warn(s"The job '${job.name}' for ${task.outputTable} at $dayStr was failed doe to source table read error. $elapsedTimeStr")
          (TaskStatus.NOT_READY.toString, Some("Failed loading data from the table"))
        } else {
          log.info(s"The job '${job.name}' for ${task.outputTable} at $dayStr has succeeded. $elapsedTimeStr")
          (subTask.status, None)
        }

        val actualInputRecords = if (isSourcingTask) outputRecordCount else subTask.inputRecordCount

        if (!pramenConfig.dryRun && status != TaskStatus.NOT_READY.toString && !pramenConfig.undercover) {
          bookKeeper.setRecordCount(task.outputTable,
            subTask.infoDateOutput,
            subTask.infoDateBegin,
            subTask.infoDateEnd,
            actualInputRecords,
            outputRecordCount,
            start.getEpochSecond,
            finish.getEpochSecond)
        }

        runState.addCompletedTask(TaskCompleted(job.name,
          task.outputTable,
          subTask.infoDateBegin,
          subTask.infoDateEnd,
          subTask.infoDateOutput,
          actualInputRecords,
          subTask.inputRecordCountOld,
          Option(outputRecordCount),
          subTask.outputRecordCountOld,
          outputSize,
          start.getEpochSecond,
          finish.getEpochSecond,
          status,
          reason))
      }
    }
  }

  private def multiTryWrite(dfOut: DataFrame,
                            writerOpt: Option[TableWriter],
                            infoDatOutput: LocalDate,
                            inputCount: Long,
                            isSourcingJob: Boolean,
                            attemptLeft: Int = 3): Long = {
    val outputRecordCount = if (pramenConfig.dryRun) {
      log.warn(s"Dry run mode - the job won't be actually run.")
      inputCount
    } else {
      writerOpt match {
        case Some(writer) =>
          val outputRecordCountEstimate = if (isSourcingJob) inputCount else dfOut.count()
          val outputRecordCount = if (outputRecordCountEstimate > 0L) {
            writer.write(dfOut, infoDatOutput, Option(outputRecordCountEstimate))
          } else {
            0L
          }
          if (isSourcingJob && outputRecordCount == 0L && inputCount != 0L) {
            log.error(s"The source database provides inconsistent results ($outputRecordCount != $inputCount). Attempts left = ${attemptLeft - 1}")
            if (attemptLeft > 0) {
              log.warn(s"Attempting to run again after $attemptDelaySeconds seconds...")
              Thread.sleep(attemptDelaySeconds * 1000)
              multiTryWrite(dfOut, writerOpt, infoDatOutput, inputCount, isSourcingJob, attemptLeft - 1)
            } else {
              log.error(s"No attempts left. Failing the job.")
              0L
            }
          } else {
            outputRecordCount
          }
        case None => inputCount
      }
    }
    outputRecordCount
  }

  private def getSubTasksBasedOnRecordCount(job: Job,
                                            infoDates: Array[LocalDate],
                                            task: JobDependency,
                                            tableReaders: Seq[TableReader]): Array[SubTask] = {
    val subTasks = infoDates.flatMap(infoDate => {
      val (day0in, day1in) = ScheduleUtils.getDatesRangeWithShift(job.getSchedule, infoDate, pramenConfig.inputPeriodShiftDays)

      val evaluator = new DateExprEvaluator
      evaluator.setValue("infoDate", infoDate)

      val (day0, day1) = (pramenConfig.inputPeriodFromExpr, pramenConfig.inputPeriodToExpr) match {
        case (Some(fromExpr), Some(toExpr)) =>
          val from = evaluator.evalDate(fromExpr)
          val to = evaluator.evalDate(toExpr)
          log.info(s"Input period from: '$fromExpr' = '$from' to '$toExpr' = '$to'")
          (from, to)
        case (Some(fromExpr), None)         =>
          val from = evaluator.evalDate(fromExpr)
          log.info(s"Input period from: '$fromExpr' = '$from' to '$day1in'")
          (evaluator.evalDate(fromExpr), day1in)
        case (None, Some(toExpr))           =>
          val to = evaluator.evalDate(toExpr)
          log.info(s"Input period from: '$day0in' to '$toExpr' = '$to'")
          (day0in, evaluator.evalDate(toExpr))
        case (None, None)                   =>
          (day0in, day1in)
      }

      log.info(s"Input Period: $day0 .. $day1")

      val outputInfoDate = getOutputInfoDate(job, day0, day1)
      val infoDateStr = dateFormatter.format(infoDate)

      val isLastDayOverwrite = infoDate == infoDates.last && pramenConfig.alwaysOverwriteLastChunk

      val start = Instant.now()
      val targetChunk = bookKeeper.getLatestDataChunk(task.outputTable, day0, day1)
      val inputRecordCount = if (pramenConfig.trackUpdates || targetChunk.isEmpty) {
        tableReaders.foldLeft(0L)((acc, r) => acc + r.getRecordCount(day0, day1))
      } else {
        targetChunk.get.inputRecordCount
      }
      val finish = Instant.now()

      if (inputRecordCount == 0L) {
        if (targetChunk.nonEmpty) {
          log.info(s"For ${task.outputTable} at $infoDateStr: the data has been DELETED at the source. Skipping...")
          if (!pramenConfig.ignoreSourceDeletions) {
            runState.addCompletedTask(TaskCompleted(job.name,
              task.outputTable,
              day0,
              day1,
              outputInfoDate,
              0,
              targetChunk.map(_.inputRecordCount).getOrElse(0L),
              None,
              targetChunk.map(_.outputRecordCount),
              None,
              start.getEpochSecond,
              finish.getEpochSecond,
              TaskStatus.NOT_READY.toString,
              Some("Data was deleted from the source")))
          }
        } else {
          log.info(s"For ${task.outputTable} at $infoDateStr: no new data yet. Skipping...")
          if (pramenConfig.warnNoData || pramenConfig.loadDateTo.isDefined) {
            runState.addCompletedTask(TaskCompleted(job.name,
              task.outputTable,
              day0,
              day1,
              outputInfoDate,
              0,
              targetChunk.map(_.inputRecordCount).getOrElse(0L),
              None,
              targetChunk.map(_.outputRecordCount),
              None,
              start.getEpochSecond,
              finish.getEpochSecond,
              TaskStatus.NOT_READY.toString,
              Some("No data at the source")))
          }
        }
        None
      } else {
        targetChunk match {
          case None =>
            log.warn(s"For ${task.outputTable} at $infoDateStr: NEW $inputRecordCount records arrived. Adding to the processing list...")
            Some(SubTask(getTaskDependencies(task, infoDate), day0, day1, outputInfoDate, inputRecordCount, 0L, None, getDelayStatus(infoDate)))
          case Some(chunk) if chunk.inputRecordCount == inputRecordCount && isForcedRerun =>
            log.info(s"For ${task.outputTable} at $infoDateStr: FORCED RERUN. " +
              s"Adding to the processing list...")
            Some(SubTask(getTaskDependencies(task, infoDate), day0, day1, outputInfoDate, inputRecordCount, chunk.inputRecordCount, Option(chunk.outputRecordCount), TaskStatus.RERUN.toString))
          case Some(chunk) if chunk.inputRecordCount == inputRecordCount && isLastDayOverwrite =>
            log.info(s"For ${task.outputTable} at $infoDateStr: Updating according to the configuration. " +
              s"Adding to the processing list...")
            Some(SubTask(getTaskDependencies(task, infoDate), day0, day1, outputInfoDate, inputRecordCount, chunk.inputRecordCount, Option(chunk.outputRecordCount), TaskStatus.RENEW.toString))
          case Some(chunk) if chunk.inputRecordCount == inputRecordCount && pramenConfig.loadDateTo.isDefined && pramenConfig.trackUpdates =>
            log.info(s"For ${task.outputTable} at $infoDateStr: reloading data as part of range load job. " +
              s"Adding to the processing list...")
            Some(SubTask(getTaskDependencies(task, infoDate), day0, day1, outputInfoDate, inputRecordCount, chunk.inputRecordCount, Option(chunk.outputRecordCount), TaskStatus.UPDATE.toString))
          case Some(chunk) if chunk.inputRecordCount == inputRecordCount =>
            log.info(s"For ${task.outputTable} at $infoDateStr: input data hasn't changed. Skipping...")
            None
          case Some(chunk) if pramenConfig.trackUpdates =>
            log.info(s"For ${task.outputTable} at $infoDateStr: record count MISMATCH $inputRecordCount != ${chunk.inputRecordCount}. " +
              s"UPDATE has arrived. Adding to the processing list...")
            Some(SubTask(getTaskDependencies(task, infoDate), day0, day1, outputInfoDate, inputRecordCount, chunk.inputRecordCount, Option(chunk.outputRecordCount), TaskStatus.UPDATE.toString))
          case Some(chunk) =>
            log.info(s"For ${task.outputTable} at $infoDateStr: record count MISMATCH $inputRecordCount != ${chunk.inputRecordCount}. " +
              s"UPDATE has arrived. But updates tracking is turned off. Skipping..")
            None
        }
      }
    })
    subTasks
  }

  private def getSubTasksBasedOnDates(job: Job,
                                      infoDates: Array[LocalDate],
                                      task: JobDependency): Array[SubTask] = {
    val subTasks = infoDates.flatMap(infoDate => {
      val start = Instant.now()
      val (inputDay0, inputDay1) = ScheduleUtils.getDatesRangeWithShift(job.getSchedule, infoDate, pramenConfig.inputPeriodShiftDays)
      val (outputDay0, outputDay1) = ScheduleUtils.getDatesRange(job.getSchedule, infoDate)
      log.info(s"Input Period: $inputDay0 .. $inputDay1")
      log.info(s"Output Period: $outputDay0 .. $outputDay1")

      val outputInfoDate = getOutputInfoDate(job, outputDay0, outputDay1)
      val infoDateStr = dateFormatter.format(infoDate)

      val isLastDayOverwrite = infoDate == infoDates.last && pramenConfig.alwaysOverwriteLastChunk

      val summary = if (task.inputTables.isEmpty || !validator.isEmpty) {
        validator.decideRunTask(outputDay0, outputDay1, outputInfoDate, task.outputTable, pramenConfig.loadDateTo.nonEmpty && pramenConfig.trackUpdates)
      } else {
        val date0Str = inputDay0.format(dateFormatter)
        val date1Str = inputDay1.format(dateFormatter)
        val check = new ValidationCheck(Nil, task.inputTables, Some(s"'$date0Str'"), Some(s"'$date1Str'"))
        val validatorOldWay = new DataAvailabilityValidator(check :: Nil, bookKeeper, pramenConfig.trackUpdates)
        validatorOldWay.decideRunTask(outputDay0, outputDay1, outputInfoDate, task.outputTable, pramenConfig.loadDateTo.nonEmpty && pramenConfig.trackUpdates)
      }

      val inputRecordCount = if (summary.inputRecordCount > 0) summary.inputRecordCount.toString else ""

      summary.decision match {
        case RunDecision.SkipNoData =>
          val finish = Instant.now()
          if (pramenConfig.warnNoData) {
            runState.addCompletedTask(TaskCompleted(job.name,
              task.outputTable,
              inputDay0,
              inputDay1,
              outputInfoDate,
              0L,
              0L,
              None,
              None,
              None,
              start.getEpochSecond,
              finish.getEpochSecond,
              TaskStatus.NOT_READY.toString,
              Some(s"No recent data to calculate $outputInfoDate in ${summary.noDataTables.mkString(", ")}")))
          }
          None
        case RunDecision.RunNew if infoDate.isBefore(activeInfoDate.minusDays(pramenConfig.expectedDelayDays)) =>
          log.warn(s"For ${task.outputTable} at $infoDateStr: LATE $inputRecordCount records arrived. Adding to the processing list...")
          Some(SubTask(getTaskDependencies(task, infoDate), inputDay0, inputDay1, outputInfoDate, summary.inputRecordCount, 0L, None, TaskStatus.LATE.toString))
        case RunDecision.RunNew =>
          log.warn(s"For ${task.outputTable} at $infoDateStr: NEW $inputRecordCount records arrived. Adding to the processing list...")
          Some(SubTask(getTaskDependencies(task, infoDate), inputDay0, inputDay1, outputInfoDate, summary.inputRecordCount, 0L, None, TaskStatus.NEW.toString))
        case _ if isForcedRerun =>
          log.info(s"For ${task.outputTable} at $infoDateStr: FORCED RERUN. " +
            s"Adding to the processing list...")
          Some(SubTask(getTaskDependencies(task, infoDate), inputDay0, inputDay1, outputInfoDate, summary.inputRecordCount, summary.inputRecordCountOld.getOrElse(0L), summary.outputRecordCountOld, TaskStatus.RERUN.toString))
        case _ if isLastDayOverwrite =>
          log.info(s"For ${task.outputTable} at $infoDateStr: Updating according to the configuration. " +
            s"Adding to the processing list...")
          Some(SubTask(getTaskDependencies(task, infoDate), inputDay0, inputDay1, outputInfoDate, summary.inputRecordCount, summary.inputRecordCountOld.getOrElse(0L), summary.outputRecordCountOld, TaskStatus.RENEW.toString))
        case RunDecision.SkipUpToDate =>
          log.info(s"For ${task.outputTable} at $infoDateStr: no recent updates. Skipping...")
          None
        case RunDecision.RunUpdates =>
          val outputLandedStr = summary.outputTableLastUpdated.get.format(timestampFmt)
          val recentTablesStr = summary.updatedTables
            .map { case (name, time) => s"$name -> ${time.format(timestampFmt)}" }
            .mkString("[ ", ", ", " ]")
          log.info(s"For ${task.outputTable} at $infoDateStr: latest data landed $outputLandedStr while the following input tables are more recent: " +
            s"$recentTablesStr. Adding to the processing list...")
          Some(SubTask(getTaskDependencies(task, infoDate), inputDay0, inputDay1, outputInfoDate, summary.inputRecordCount, summary.inputRecordCountOld.getOrElse(0L), summary.outputRecordCountOld, TaskStatus.UPDATE.toString))
      }
    })
    subTasks
  }

  private def getDelayStatus(infoDate: LocalDate): String = {
    if (infoDate.isBefore(activeInfoDate.minusDays(pramenConfig.expectedDelayDays))) {
      TaskStatus.LATE.toString
    } else {
      TaskStatus.NEW.toString
    }
  }

  private def getTaskDependencies(jobDependency: JobDependency, infoDate: LocalDate): Seq[TaskDependency] = {
    jobDependency.inputTables.flatMap(inputTable =>
      bookKeeper.getLatestProcessedDate(inputTable, Some(infoDate)).map(
        date => TaskDependency(inputTable, date)
      )
    )
  }

  private def getUpdatedTables(inputChunks: Seq[TableChunk], outputChunk: DataChunk): TableUpdates = {
    TableUpdates(ZonedDateTime.ofInstant(Instant.ofEpochSecond(outputChunk.jobFinished), zoneId),
      inputChunks.flatMap(inputChunk => {
        if (inputChunk.dataChunk.jobFinished >= outputChunk.jobFinished) {
          Some((inputChunk.tableName, ZonedDateTime.ofInstant(Instant.ofEpochSecond(inputChunk.dataChunk.jobFinished), zoneId)))
        } else {
          None
        }
      })
    )
  }

  private def acquireTableLock(tableName: String): TokenLock = {
    val lock = tokenFactory.getLock(tableName)

    var acquired = lock.tryAcquire()

    if (!acquired && pramenConfig.waitForTableEnabled) {
      log.warn(s"Lock for $tableName is acquired by another job. Waining for the table to ba available for writing")
      val started = Instant.now().getEpochSecond
      var now = started
      while (!acquired && (pramenConfig.waitForTableSeconds <= 0 || (now - started) > pramenConfig.waitForTableSeconds)) {
        Thread.sleep(5000)
        acquired = lock.tryAcquire()
        now = Instant.now().getEpochSecond
      }
    }

    if (!acquired) {
      throw new IllegalStateException(s"Lock for $tableName is acquired by another job. Cannot write to the table.")
    }
    lock
  }

  private def getFilteredDependencies(job: Job): Seq[JobDependency] = {
    val originalDependencies = job.getDependencies
    pramenConfig.runOnlyTableNum match {
      case Some(tableNum) => originalDependencies(tableNum - 1) :: Nil
      case None => originalDependencies
    }
  }

  private def getOutputInfoDate(job: Job, jobDate0: LocalDate, jobDate1: LocalDate): LocalDate = {
    pramenConfig.outputInfoDateExpr match {
      case Some(dateExpr) =>
        val evaluator = new DateExprEvaluator
        evaluator.setValue("infoDateBegin", jobDate0)
        evaluator.setValue("infoDateEnd", jobDate1)
        evaluator.setValue("infoDate", jobDate1)
        val outputInfoDate = logEval(evaluator, dateExpr)
        log.info(s"Output information date determined by the expression: $outputInfoDate")
        outputInfoDate
      case None =>
        val outputInfoDate = job.transformOutputInfoDate(jobDate1)
        log.info(s"Output information date determined by the job: $outputInfoDate")
        outputInfoDate
    }

  }

  private def validateOutputSchema(tableName: String, df: DataFrame, infoDate: LocalDate): Unit = {
    val lastSchema = bookKeeper.getLatestSchema(tableName, infoDate.minusDays(1))

    lastSchema match {
      case Some((oldSchema, oldInfoDate)) =>
        val diff = SparkUtils.compareSchemas(oldSchema, df.schema)
        if (diff.nonEmpty) {
          log.warn(s"SCHEMA CHANGE for $tableName from $oldInfoDate to $infoDate: ${diff.map(_.toString).mkString("; ")}")
          runState.addSchemaDifference(SchemaDifference(tableName, oldInfoDate, infoDate, diff))
          bookKeeper.saveSchema(tableName, infoDate, df.schema)
        }
      case None => bookKeeper.saveSchema(tableName, infoDate, df.schema)
    }
  }

  private def logEval(evaluator: DateExprEvaluator, expr: String): LocalDate = {
    val result = evaluator.evalDate(expr)
    log.info(s"Expr: '$expr' = '$result'")
    result
  }
}
