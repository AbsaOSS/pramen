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

package za.co.absa.pramen.framework.runner.orchestrator

import com.github.yruslan.channel.Channel
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.app.AppContext
import za.co.absa.pramen.framework.pipeline.{Job, JobDependency, OperationType}
import za.co.absa.pramen.framework.runner.jobrunner.ConcurrentJobRunner
import za.co.absa.pramen.framework.runner.task.{RunStatus, TaskResult}
import za.co.absa.pramen.framework.state.PipelineState

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class OrchestratorImpl extends Orchestrator {
  private val log = LoggerFactory.getLogger(this.getClass)

  private var pendingJobs = List.empty[Job]
  private val runningJobs: mutable.Set[Job] = mutable.HashSet.empty[Job]

  def runJobs(jobs: Seq[Job])(implicit conf: Config,
                              state: PipelineState,
                              appContext: AppContext,
                              jobRunner: ConcurrentJobRunner,
                              spark: SparkSession): Unit = {
    val allOutputTables = jobs.map(_.outputTable.name)

    if (jobs.isEmpty) {
      log.warn(s"No jobs defined in the pipeline. The pipeline will be SKIPPED.")
      return
    }

    pendingJobs = jobs.toList

    val dependencies = getDependencies(jobs)
    val dependencyResolver = new DependencyResolverImpl(dependencies)

    log.info(s"Starting execution of the pipeline: \n${dependencyResolver.getDag(allOutputTables)}")

    val runJobChannel = Channel.make[Job](jobs.length)
    val completedJobsChannel = jobRunner.getCompletedJobsChannel
    jobRunner.startWorkerLoop(runJobChannel)

    val atLeastOneStarted = sendPendingJobs(runJobChannel, dependencyResolver)

    if (atLeastOneStarted) {
      completedJobsChannel.foreach{ case(finishedJob, taskResults, isSucceeded) =>
        runningJobs.remove(finishedJob)

        updateDependencyResolver(dependencyResolver, finishedJob, isSucceeded)

        state.addTaskCompletion(taskResults)

        val jobStarted = sendPendingJobs(runJobChannel, dependencyResolver)
        if (!jobStarted && runningJobs.isEmpty) {
          runJobChannel.close
        }
      }
    }

    pendingJobs.foreach(job => {
      log.warn(s"Job '${job.name}' outputting to '${job.outputTable.name}' is SKIPPED.")
      log.warn(s"Dependencies: ${dependencyResolver.getDag(job.outputTable.name :: Nil)}")

      val missingTables = dependencyResolver.getMissingDependencies(job.outputTable.name)

      val taskResult = TaskResult(job, RunStatus.MissingDependencies(missingTables), None, Nil, Nil)

      state.addTaskCompletion(taskResult :: Nil)
    })

    jobRunner.shutdown()
  }

  def sendPendingJobs(runJobsChannel: Channel[Job], dependencyResolver: DependencyResolver): Boolean = {
    val newPendingJobs = new ListBuffer[Job]

    var atLeastOneStarted = false
    pendingJobs.foreach { job =>
      if (dependencyResolver.canRun(job.outputTable.name)) {
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

  private def updateDependencyResolver(dependencyResolver: DependencyResolver, job: Job, isSucceeded: Boolean): Unit = {
    val outputTable = job.outputTable

    if (isSucceeded) {
      log.info(s"Job '${job.name}' outputting to '${outputTable.name}' is SUCCEEDED.")
      dependencyResolver.setAvailableTable(outputTable.name)
    } else {
      log.warn(s"Job '${job.name}' outputting to '${outputTable.name}' is FAILED.")
      dependencyResolver.setFailedTable(outputTable.name)
    }
  }
}
