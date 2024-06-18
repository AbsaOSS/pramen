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

package za.co.absa.pramen.core.runner.orchestrator

import com.github.yruslan.channel.Channel
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.api.status.{RunStatus, TaskResult}
import za.co.absa.pramen.core.app.AppContext
import za.co.absa.pramen.core.exceptions.{FatalErrorWrapper, ValidationException}
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.pipeline.{Job, JobDependency, OperationType}
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunner
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils.evaluateRunDate
import za.co.absa.pramen.core.state.PipelineState
import za.co.absa.pramen.core.utils.Emoji._

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class OrchestratorImpl extends Orchestrator {
  private val log = LoggerFactory.getLogger(this.getClass)

  private var pendingJobs = List.empty[Job]
  private val runningJobs: mutable.Set[Job] = mutable.HashSet.empty[Job]

  @throws[ValidationException]
  override def validateJobs(jobs: Seq[Job])(implicit appContext: AppContext,
                                            spark: SparkSession): Unit = {
    val enableMultipleJobsPerTable = appContext.appConfig.generalConfig.enableMultipleJobsPerTable
    val dependencies = getDependencies(jobs)
    val dependencyResolver = new DependencyResolverImpl(dependencies, enableMultipleJobsPerTable)

    log.info(s"Validating dependencies...")

    dependencyResolver.validate()
    validateJobs(jobs, appContext.appConfig.runtimeConfig.runDate, enableMultipleJobsPerTable)
  }

  override def runJobs(jobs: Seq[Job])(implicit conf: Config,
                                       state: PipelineState,
                                       appContext: AppContext,
                                       jobRunner: ConcurrentJobRunner,
                                       spark: SparkSession): Unit = {
    val applicationId = spark.sparkContext.applicationId
    val allOutputTables = jobs.map(_.outputTable.name)

    if (jobs.isEmpty) {
      log.warn(s"No jobs defined in the pipeline. The pipeline will be SKIPPED.")
      return
    }

    pendingJobs = jobs.toList

    val enableMultipleJobsPerTable = appContext.appConfig.generalConfig.enableMultipleJobsPerTable
    val dependencies = getDependencies(jobs)
    val dependencyResolver = new DependencyResolverImpl(dependencies, enableMultipleJobsPerTable)

    log.info(s"Starting execution of the pipeline: \n${dependencyResolver.getDag(allOutputTables)}")

    val runJobChannel = Channel.make[Job](jobs.length)
    val completedJobsChannel = jobRunner.getCompletedJobsChannel
    jobRunner.startWorkerLoop(runJobChannel)

    val atLeastOneStarted = sendPendingJobs(runJobChannel, dependencyResolver)
    var hasFatalErrors = false

    if (atLeastOneStarted) {
      completedJobsChannel.foreach { case (finishedJob, taskResults, isSucceeded) =>
        runningJobs.remove(finishedJob)

        hasFatalErrors = hasFatalErrors || taskResults.exists(status => isFatalFailure(status.runStatus))

        val hasAnotherUnfinishedJob = hasAnotherJobWithSameOutputTable(finishedJob.outputTable.name)
        if (hasAnotherUnfinishedJob) {
          log.info(s"There is another job outputting to ${finishedJob.outputTable.name}. Waiting for it to finish before marking the table as finished.")
        }

        val isLazy = finishedJob.outputTable.format.isLazy

        if (!hasAnotherUnfinishedJob || !isSucceeded) {
          updateDependencyResolver(dependencyResolver, finishedJob, isSucceeded, isLazy)
        }

        state.addTaskCompletion(taskResults)

        if (hasFatalErrors) {
          // In case of a fatal error we either need to interrupt running threads, or wait for them to return.
          // In the current implementation we wait for threads to finish, but not start new jobs in running threads.
          // This can be also reconsidered, if there are issues with the current solutions observed.
          if (runningJobs.isEmpty) {
            runJobChannel.close()
          }
        } else {
          val jobStarted = sendPendingJobs(runJobChannel, dependencyResolver)
          if (!jobStarted && runningJobs.isEmpty) {
            runJobChannel.close()
          }
        }
      }
    }

    pendingJobs.foreach(job => {
      log.warn(s"$WARNING Job '${job.name}' outputting to '${job.outputTable.name}' is SKIPPED.")
      log.warn(s"Dependencies: ${dependencyResolver.getDag(job.outputTable.name :: Nil)}")

      val missingTables = dependencyResolver.getMissingDependencies(job.outputTable.name)

      val isTransient = job.outputTable.format.isTransient
      val isFailure = hasNonPassiveNonOptionalDeps(job, missingTables)

      val taskResult = TaskResult(
        job.name,
        MetaTable.getMetaTableDef(job.outputTable),
        RunStatus.MissingDependencies(isFailure, missingTables),
        None,
        applicationId,
        isTransient,
        job.outputTable.format.isInstanceOf[DataFormat.Raw],
        Nil,
        Nil,
        Nil,
        job.operation.extraOptions
      )

      state.addTaskCompletion(taskResult :: Nil)
    })

    jobRunner.shutdown()
  }

  private def isFatalFailure(runStatus: RunStatus): Boolean = {
    runStatus match {
      case RunStatus.Failed(ex) if ex.isInstanceOf[FatalErrorWrapper] => true
      case _ => false
    }
  }

  private def hasNonPassiveNonOptionalDeps(job: Job, missingTables: Seq[String]): Boolean = {
    missingTables.exists(table =>
      job.operation.dependencies.exists(d => !d.isPassive && !d.isOptional && d.tables.contains(table))
    )
  }

  private def hasAnotherJobWithSameOutputTable(outputTableName: String): Boolean = {
    runningJobs.exists(_.outputTable.name.equalsIgnoreCase(outputTableName)) ||
      pendingJobs.exists(_.outputTable.name.equalsIgnoreCase(outputTableName))
  }

  def sendPendingJobs(runJobsChannel: Channel[Job], dependencyResolver: DependencyResolver): Boolean = {
    val newPendingJobs = new ListBuffer[Job]

    var atLeastOneStarted = false
    pendingJobs.foreach { job =>
      if (dependencyResolver.canRun(job.outputTable.name, job.operation.alwaysAttempt)) {
        runningJobs += job
        atLeastOneStarted = true
        log.info(s"Job '${job.name}' outputting to '${job.outputTable.name}' is selected for execution.")
        log.info(s"Dependencies: ${dependencyResolver.getDag(job.outputTable.name :: Nil)}")
        runJobsChannel.send(job)
      } else {
        newPendingJobs += job
      }
    }

    pendingJobs = newPendingJobs.toList
    atLeastOneStarted
  }

  def getDependencies(jobs: Seq[Job]): Seq[JobDependency] = {
    jobs.flatMap(job => {
      val inputTables = job.operation.dependencies
        .flatMap(_.tables)
        .distinct
      job.operation.operationType match {
        case s: OperationType.Sink =>
          s.sinkTables.map(table => JobDependency(Seq(table.metaTableName), table.outputTableName.getOrElse(s"${table.metaTableName}->${s.sinkName}")))
        case _ =>
          Seq(JobDependency(inputTables, job.outputTable.name))
      }
    })
  }

  private def updateDependencyResolver(dependencyResolver: DependencyResolver,
                                       job: Job,
                                       isSucceeded: Boolean,
                                       isLazy: Boolean): Unit = {
    val outputTable = job.outputTable

    if (isSucceeded) {
      if (isLazy) {
        log.info(s"Lazy job '${job.name}' outputting to '${outputTable.name}' has been registered for the future use.")
      } else {
        log.info(s"$SUCCESS Job '${job.name}' outputting to '${outputTable.name}' has SUCCEEDED.")
      }

      dependencyResolver.setAvailableTable(outputTable.name)
    } else {
      log.warn(s"$FAILURE Job '${job.name}' outputting to '${outputTable.name}' has FAILED.")
      dependencyResolver.setFailedTable(outputTable.name)
    }
  }

  @throws[ValidationException]
  private[core] def validateJobs(jobs: Seq[Job],
                                 runDate: LocalDate,
                                 allowMultipleJobsPerTable: Boolean): Unit = {
    if (allowMultipleJobsPerTable) {
      val duplicateJobs = jobs
        .groupBy(_.outputTable.name)
        .filter(_._2.length > 1)
        .values
        .toList

      val issues = duplicateJobs.flatMap(duplicateJobs => validateOverlapInfoDates(duplicateJobs, runDate))

      if (issues.nonEmpty) {
        throw new ValidationException(s"Job validation issues found: ${issues.mkString("; ")}")
      }
    }
  }

  private[core] def validateOverlapInfoDates(jobs: Seq[Job], runDate: LocalDate): Seq[String] = {
    val infoDates = jobs.flatMap { job =>
      if (job.operation.schedule.isEnabled(runDate)) {
        val infoDateExpression = job.operation.outputInfoDateExpression
        val infoDate = evaluateRunDate(runDate, infoDateExpression).toString
        Option(infoDate)
      } else {
        None
      }
    }

    val duplicateInfoDates = infoDates
      .groupBy(identity)
      .filter(_._2.length > 1)
      .keys
      .toList

    if (duplicateInfoDates.isEmpty) {
      Seq.empty[String]
    } else {
      Seq(s"More than one job outputting to ${jobs.head.outputTable.name} output to the same info date (partition): ${duplicateInfoDates.mkString(", ")}")
    }
  }
}
