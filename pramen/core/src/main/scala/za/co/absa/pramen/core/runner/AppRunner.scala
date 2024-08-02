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
import za.co.absa.pramen.core.PramenImpl
import za.co.absa.pramen.core.app.config.{HookConfig, RuntimeConfig}
import za.co.absa.pramen.core.app.{AppContext, AppContextImpl}
import za.co.absa.pramen.core.config.Keys.LOG_EXECUTOR_NODES
import za.co.absa.pramen.core.exceptions.ValidationException
import za.co.absa.pramen.core.metastore.peristence.{TransientJobManager, TransientTableManager}
import za.co.absa.pramen.core.pipeline.{Job, OperationDef, OperationSplitter, PipelineDef}
import za.co.absa.pramen.core.runner.jobrunner.{ConcurrentJobRunner, ConcurrentJobRunnerImpl}
import za.co.absa.pramen.core.runner.orchestrator.OrchestratorImpl
import za.co.absa.pramen.core.runner.task.{TaskRunner, TaskRunnerMultithreaded}
import za.co.absa.pramen.core.state.{PipelineState, PipelineStateImpl}
import za.co.absa.pramen.core.utils.Emoji._
import za.co.absa.pramen.core.utils.{BuildPropertyUtils, ResourceUtils}

import scala.util.{Failure, Success, Try}

object AppRunner {
  val ERROR_CODE_MAJOR_FAILURE = 2
  val ERROR_CODE_FAILURE = 1
  private var bannerShown = false

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Runs the pipeline defined in the given config.
    */
  def runPipeline(conf: Config): Int = {
    implicit val state: PipelineState = createPipelineState(conf) match {
      case Success(st) => st
      case Failure(ex)  =>
        log.error(s"An error has occurred before notification configuration is available. No notifications will be sent.", ex)
        return ERROR_CODE_MAJOR_FAILURE
    }

    val exitCodeTry = for {
      spark      <- getSparkSession(conf, state)
      _          <- Try { state.setSparkAppId(spark.sparkContext.applicationId) }
      _          <- logBanner(spark)
      _          <- logExecutorNodes(conf, state, spark)
      appContext <- createAppContext(conf, state, spark)
      taskRunner <- createTaskRunner(conf, state, appContext, spark.sparkContext.applicationId)
      pipeline   <- getPipelineDef(conf, state, appContext)
      jobsOrig   <- splitJobs(conf, pipeline, state, appContext, spark)
      jobs       <- filterJobs(state, jobsOrig, appContext.appConfig.runtimeConfig)
      _          <- runStartupHook(state, appContext.appConfig.hookConfig)
      _          <- validateShutdownHook(state, appContext.appConfig.hookConfig)
      _          <- initPipelineNotificationTargets(state)
      _          <- validatePipeline(jobs, state, appContext, spark)
      _          <- runPipeline(conf, jobs, state, appContext, taskRunner, spark)
      _          <- shutdownTaskRunner(taskRunner, state)
    } yield {
      val exitCode = state.getExitCode
      val emoji = if (exitCode == 0) SUCCESS else FAILURE
      log.info(s"$emoji The pipeline has finished. Exit code = $exitCode")
      exitCode
    }

    resetState(state)

    exitCodeTry match {
      case Success(exitCode)      =>
        exitCode
      case Failure(ex: Throwable) =>
        log.error(s"$FAILURE The pipeline has failed with an exception.", ex)
        ERROR_CODE_FAILURE
    }
  }

  private[core] def handleFailure[T](t: Try[T], state: PipelineState, stage: String): Try[T] = {
    t.recoverWith { case ex: Throwable =>
      state.setFailure(stage, ex)
      Failure(new RuntimeException(s"An error occurred during $stage.", ex))
    }
  }

  private[core] def createPipelineState(implicit conf: Config): Try[PipelineState] = {
    Try {
      new PipelineStateImpl()(conf, PramenImpl.instance.notificationBuilder)
    }
  }

  private[core] def createAppContext(implicit conf: Config,
                                     state: PipelineState,
                                     spark: SparkSession): Try[AppContext] = {
    handleFailure(Try {
      PramenImpl.instance.asInstanceOf[PramenImpl].setPipelineState(state)
      AppContextImpl(conf)
    }, state, "initialization of the pipeline")
  }

  private[core] def createTaskRunner(implicit conf: Config,
                                     state: PipelineState,
                                     appContext: AppContext,
                                     applicationId: String): Try[TaskRunner] = {
    handleFailure(Try {
      new TaskRunnerMultithreaded(conf, appContext.bookkeeper, appContext.journal, appContext.tokenLockFactory, state, appContext.appConfig.runtimeConfig, applicationId)
    }, state, "initialization of the task runner")
  }

  private[core] def initPipelineNotificationTargets(implicit state: PipelineState): Try[Unit] = {
    handleFailure(Try {
      state.asInstanceOf[PipelineStateImpl].initNotificationTargets()
    }, state, "Initialization of piepline notification targets")
  }

  private[core] def getSparkSession(implicit conf: Config,
                                    state: PipelineState): Try[SparkSession] = {
    handleFailure(Try {
      PipelineSparkSessionBuilder.buildSparkSession(conf)
    }, state, "Spark Session creation")
  }

  private[core] def logExecutorNodes(implicit conf: Config,
                                     state: PipelineState,
                                     spark: SparkSession): Try[Unit] = {
    handleFailure(Try {
      val logExecutorNodes = conf.getBoolean(LOG_EXECUTOR_NODES)

      if (logExecutorNodes) {
        val hosts = getExecutorNodes

        hosts.foreach(host => log.info(s"Executor node: $host"))
      }
    }, state, "Spark List of executor nodes")
  }

  private[core] def getExecutorNodes(implicit spark: SparkSession): Seq[String] = {
    val DEFAULT_DUMMY_JOB_ENTRIES = 1000000
    val sc = spark.sparkContext

    val data = sc.parallelize(1 to DEFAULT_DUMMY_JOB_ENTRIES).repartition(sc.defaultParallelism)
    data.mapPartitions { _ => Iterable(java.net.InetAddress.getLocalHost.getHostName).iterator }.collect().distinct.sorted
  }

  private[core] def logBanner(implicit spark: SparkSession): Try[Unit] = {
    if (!bannerShown) {
      Try {
        bannerShown = true
        val version = BuildPropertyUtils.instance.getFullVersion
        val banner = ResourceUtils.getResourceString("/pramen_banner.txt")
          .replace("""project_version""", version)
        log.info(s"\n$banner")
        log.info(s"Runtime Spark version: ${spark.version}")

        spark.sparkContext.uiWebUrl.foreach(url => log.info(s"Spark URL: $url"))
      }
    } else {
      Success(()) // Short version of the Darth Vader ship? (-()-)
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
      val isHistoricalRun = appContext.appConfig.runtimeConfig.runDateTo.nonEmpty
      val splitter = new OperationSplitter(conf, appContext.metastore, appContext.bookkeeper)

      if (isHistoricalRun)
        log.info("This is a historical run. Making all dependencies 'passive' for all jobs...")

      pipelineDef.operations.flatMap { opOrig =>
        val op = if (isHistoricalRun) {
          preProcessOperationForHistoricalRun(opOrig)
        } else {
          opOrig
        }

        splitter.createJobs(op)
      }
    }, state, "splitting of the pipeline into jobs")
  }

  private[core] def preProcessOperationForHistoricalRun(ops: OperationDef): OperationDef = {
    val newDep = ops.dependencies.map(dep => dep.copy(isPassive = true))
    ops.copy(dependencies = newDep)
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

        jobs.filter(job => selectedTablesSet.contains(job.outputTable.name) || job.outputTable.format.isLazy)
      }
    }, state, "selecting jobs for execution")
  }

  private[core] def validatePipeline(implicit jobs: Seq[Job],
                                     state: PipelineState,
                                     appContext: AppContext,
                                     spark: SparkSession): Try[Unit] = {
    handleFailure(Try {
      val orchestrator = new OrchestratorImpl()

      orchestrator.validateJobs(jobs)

      if (jobs.isEmpty && !appContext.appConfig.runtimeConfig.allowEmptyPipeline) {
        throw new ValidationException(s"No jobs defined in the pipeline. Please, define one or more operations.")
      }

      val runDate = appContext.appConfig.runtimeConfig.runDate

      if (runDate.isBefore(appContext.appConfig.infoDateDefaults.startDate)) {
        throw new ValidationException(s"The requested run date '$runDate' is older than the information start date '${appContext.appConfig.infoDateDefaults.startDate}'.")
      }
    }, state, "validating the pipeline")
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
        taskRunner,
        spark.sparkContext.applicationId)

      TransientJobManager.setTaskRunner(taskRunner)

      val orchestrator = new OrchestratorImpl()

      orchestrator.runJobs(jobs)

      state.setSuccess()
    }, state, "running the pipeline")
  }

  private[core] def runStartupHook(state: PipelineState,
                                   hookConfig: HookConfig): Try[Unit] = {

    val tryHandler: Try[Unit] =
      hookConfig.startupHook match {
        case Some(hookTry) => hookTry match {
          case Success(runnable) => Try {
            log.info(s"Running the startup hook...")
            runnable.run()
          }
          case Failure(ex) =>
            log.warn(s"An error has occurred while instantiating the startup hook. The exception will be re-thrown.")
            Failure(ex)
        }
        case None =>
          Success[Unit](())
      }

    handleFailure(tryHandler, state, "running the startup hook")
  }

  private[core] def validateShutdownHook(state: PipelineState,
                                         hookConfig: HookConfig): Try[Unit] = {
    val tryHandler: Try[Unit] =
      hookConfig.shutdownHook match {
        case Some(hookTry) =>
          log.info(s"Validating the shutdown hook...")
          hookTry match {
            case Success(_)  =>
              state.setShutdownHookCanRun()
              Success[Unit](())
            case Failure(ex) =>
              log.warn(s"An error has occurred while instantiating the shutdown hook. The exception will be re-thrown.")
              Failure(ex)
          }
        case None => Success[Unit](())
      }

    handleFailure(tryHandler, state, "validating the shutdown hook")
  }

  private[core] def shutdownTaskRunner(taskRunner: TaskRunner, state: PipelineState): Try[Unit] = {
    handleFailure(Try {
      taskRunner.close()
    }, state, "shutting down task runner execution context")
  }

  private[core] def resetState(state: PipelineState): Unit = {
    // Neither of these should throw any exceptions.
    // The handling of exceptions is added as a precaution.
    runIgnoringExceptions {
      log.info("Cleaning metastore transient table state...")
      TransientTableManager.reset()
    }
    runIgnoringExceptions {
      log.info("Cleaning metastore transient job state...")
      TransientJobManager.reset()
    }
    runIgnoringExceptions {
      log.info("Cleaning pramen impl...")
      PramenImpl.reset()
    }
    runIgnoringExceptions {
      log.info("Cleaning pramen state...")
      state.close()
    }
  }

  private [core] def runIgnoringExceptions(action: => Unit): Unit = {
    Try {
      action
    }.recover({
      case ex: Throwable => log.error("Error cleaning up the app state.", ex)
    })
  }
}
