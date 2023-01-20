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

package za.co.absa.pramen.core.runner

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.AppContextFactory
import za.co.absa.pramen.core.app.AppContext
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.pipeline.{Job, OperationSplitter, PipelineDef}
import za.co.absa.pramen.core.runner.jobrunner.{ConcurrentJobRunner, ConcurrentJobRunnerImpl}
import za.co.absa.pramen.core.runner.orchestrator.OrchestratorImpl
import za.co.absa.pramen.core.runner.task.{TaskRunner, TaskRunnerMultithreaded}
import za.co.absa.pramen.core.state.{PipelineState, PipelineStateImpl}
import za.co.absa.pramen.core.utils.Emoji._
import za.co.absa.pramen.core.utils.{BuildPropertyUtils, ResourceUtils}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object AppRunner {
  val ERROR_CODE_MAJOR_FAILURE = 2
  val ERROR_CODE_FAILURE = 1

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Runs the pipeline defined in the given config.
    */
  def runPipeline(implicit conf: Config): Int = {
    implicit val state: PipelineState = createPipelineState match {
      case Success(st) => st
      case Failure(ex)  =>
        log.error(s"An error has occurred before notification configuration is available. No notifications will be sent.", ex)
        return ERROR_CODE_MAJOR_FAILURE
    }

    val exitCodeTry = for {
      spark      <- getSparkSession
      _          <- logBanner(spark)
      appContext <- createAppContext(conf, state, spark)
      taskRunner <- createTaskRunner(conf, state, appContext)
      pipeline   <- getPipelineDef(conf, state, appContext)
      jobsOrig   <- splitJobs(conf, pipeline, state, appContext, spark)
      jobs       <- filterJobs(state, jobsOrig, appContext.appConfig.runtimeConfig)
      _          <- runPipeline(conf, jobs, state, appContext, taskRunner, spark)
      _          <- shutdown(taskRunner, state)
    } yield {
      val exitCode = state.getExitCode
      val emoji = if (exitCode == 0) SUCCESS else FAILURE
      log.info(s"$emoji The pipeline has finished. Exit code = $exitCode")
      exitCode
    }

    exitCodeTry match {
      case Success(exitCode)      =>
        exitCode
      case Failure(ex: Throwable) =>
        log.error(s"$FAILURE The pipeline has failed with an exception.", ex)
        ERROR_CODE_FAILURE
    }
  }

  private[core] def handleFailure[T](t: Try[T], state: PipelineState, stage: String): Try[T] = {
    t.recoverWith { case NonFatal(ex) =>
      state.setFailure(stage, ex)
      Failure(new RuntimeException(s"An error occurred during $stage.", ex))
    }
  }

  private[core] def createPipelineState(implicit conf: Config): Try[PipelineState] = {
    Try {
      new PipelineStateImpl
    }
  }

  private[core] def createAppContext(implicit conf: Config,
                                          state: PipelineState,
                                          spark: SparkSession): Try[AppContext] = {
    handleFailure(Try {
      AppContextFactory.getOrCreate(conf)
    }, state, "initialization of the pipeline")
  }

  private[core] def createTaskRunner(implicit conf: Config,
                                          state: PipelineState,
                                          appContext: AppContext): Try[TaskRunner] = {
    handleFailure(Try {
      new TaskRunnerMultithreaded(conf, appContext.bookkeeper, state, appContext.appConfig.runtimeConfig)
    }, state, "initialization of the task runner")
  }

  private[core] def getSparkSession(implicit conf: Config,
                                         state: PipelineState): Try[SparkSession] = {
    handleFailure(Try {
      PipelineSparkSessionBuilder.buildSparkSession(conf)
    }, state, "Spark Session creation")
  }

  private[core] def logBanner(implicit spark: SparkSession): Try[Unit] = {
    Try {
      val version = BuildPropertyUtils.instance.getFullVersion
      val banner = ResourceUtils.getResourceString("/pramen_banner.txt")
        .replaceAll("""project_version""", version)
      log.info(s"\n$banner")

      spark.sparkContext.uiWebUrl.foreach(url => log.info(s"Spark URL: $url"))
    }
  }

  private[core] def getPipelineDef(implicit conf: Config, state: PipelineState, appContext: AppContext): Try[PipelineDef] = {
    handleFailure(Try {
      PipelineDef.fromConfig(conf, appContext.appConfig.infoDateDefaults)
    }, state, "reading of the pipeline configuration")
  }

  private[core] def splitJobs(implicit conf: Config,
                                   pipelineDef: PipelineDef,
                                   state: PipelineState,
                                   appContext: AppContext,
                                   spark: SparkSession): Try[Seq[Job]] = {
    handleFailure(Try {
      val splitter = new OperationSplitter(conf, appContext.metastore, appContext.bookkeeper)

      pipelineDef.operations.flatMap(op => splitter.createJobs(op))
    }, state, "splitting of the pipeline into jobs")
  }

  private[core] def filterJobs(state: PipelineState,
                                    jobs: Seq[Job],
                                    runtimeConfig: RuntimeConfig): Try[Seq[Job]] = {
    handleFailure(Try {
      if (runtimeConfig.runTables.isEmpty) {
        jobs
      } else {
        val pipelineTables = jobs
          .map(_.outputTable.name)
          .toSet

        val selectedTablesSet = runtimeConfig
          .runTables
          .toSet

        val notFoundJobs = runtimeConfig.runTables
          .filterNot(pipelineTables.contains)
          .sortBy(identity)

        if (notFoundJobs.nonEmpty) {
          throw new IllegalArgumentException(s"Non-existent or disabled jobs selected for execution. Output tables: ${notFoundJobs.mkString(", ")}")
        }

        jobs.filter(job => selectedTablesSet.contains(job.outputTable.name))
      }
    }, state, "selecting jobs for execution")
  }

  private[core] def runPipeline(implicit conf: Config,
                                     jobs: Seq[Job],
                                     state: PipelineState,
                                     appContext: AppContext,
                                     taskRunner: TaskRunner,
                                     spark: SparkSession): Try[Unit] = {
    handleFailure(Try {
      implicit val jobRunner: ConcurrentJobRunner = new ConcurrentJobRunnerImpl(
        appContext.appConfig.runtimeConfig,
        appContext.bookkeeper,
        taskRunner)

      val orchestrator = new OrchestratorImpl()

      orchestrator.runJobs(jobs)

      state.setSuccess()
    }, state, "running of the pipeline")
  }

  private[core] def shutdown(taskRunner: TaskRunner, state: PipelineState): Try[Unit] = {
    handleFailure(Try {
      taskRunner.shutdown()
    }, state, "shutting down task runner execution context")
  }
}
