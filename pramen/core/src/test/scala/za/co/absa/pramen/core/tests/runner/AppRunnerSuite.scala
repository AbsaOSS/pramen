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

package za.co.absa.pramen.core.tests.runner

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core._
import za.co.absa.pramen.core.app.AppContext
import za.co.absa.pramen.core.app.config.HookConfig
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.metastore.model.MetastoreDependencyFactory
import za.co.absa.pramen.core.mocks.RunnableSpy
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.mocks.state.PipelineStateSpy
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.state.PipelineState
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate
import scala.util.{Failure, Success}

class AppRunnerSuite extends AnyWordSpec with SparkTestBase {
  "runPipeline()" should {
    "run the mock pipeline" in {
      val conf: Config = getTestConfig()

      val exitCode = AppRunner.runPipeline(conf)

      assert(exitCode == 0)
    }
  }

  "createPipelineState()" should {
    "be able to initialize a pipeline state" in {
      val conf: Config = getTestConfig()

      val state = AppRunner.createPipelineState(conf)

      assert(state != null)
    }
  }

  "preProcessOperationForHistoricalRun" should {
    "make all dependencies passive" in {
      val conf = ConfigFactory.parseString(
        """
          |tables = [ "table1", "table2", "table3",]
          |date.from = "2020-01-01"
          |""".stripMargin
      )
      val dep1 = MetastoreDependencyFactory.fromConfig(conf, "")
      val dep2 = dep1.copy(isOptional = true)
      val dep3 = dep1.copy(isPassive = true)
      val op = OperationDefFactory.getDummyOperationDef(dependencies = Seq(dep1, dep2, dep3))

      val newOp = AppRunner.preProcessOperationForHistoricalRun(op)

      assert(newOp.dependencies.forall(_.isPassive))
    }
  }

  "filterJobs()" should {
    val conf: Config = getTestConfig()

    val state = AppRunner.createPipelineState(conf).get

    val jobs = Range(1, 10)
      .map(i => new JobSpy(jobName = s"Job $i", outputTableIn = s"table$i"))

    "do not change the input list of jobs if no run tables are specified" in {
      val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(runTables = Seq.empty[String])

      val filteredJobs = AppRunner.filterJobs(state, jobs, runtimeConfig).get

      assert(filteredJobs.size == 9)
    }

    "filter out jobs that are not specified" in {
      val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(runTables = Seq("table2", "table4"))

      val filteredJobs = AppRunner.filterJobs(state, jobs, runtimeConfig).get

      assert(filteredJobs.size == 2)
      assert(filteredJobs.map(_.outputTable.name).contains("table2"))
      assert(filteredJobs.map(_.outputTable.name).contains("table4"))
    }
  }

  "runStartupHook()" should {
    val conf: Config = getTestConfig()
    val state = AppRunner.createPipelineState(conf).get

    "do nothing if the startup hook is not defined" in {
      val attempt = AppRunner.runStartupHook(state, HookConfig(None, None))
      assert(attempt.isSuccess)
    }

    "run the hook if is set" in {
      val runnable = new RunnableSpy()

      val attempt = AppRunner.runStartupHook(state, HookConfig(Some(Success(runnable)), None))

      assert(attempt.isSuccess)
      assert(runnable.runCount == 1)
    }

    "return failure if initialization of the hook has failed" in {
      val ex = new RuntimeException("test")

      val attempt = AppRunner.runStartupHook(state, HookConfig(Some(Failure(ex)), None))

      assert(attempt.isFailure)
    }

    "return failure if the runnable failed" in {
      val ex = new RuntimeException("test")
      val runnable = new RunnableSpy(Some(ex))

      val attempt = AppRunner.runStartupHook(state, HookConfig(Some(Success(runnable)), None))

      assert(attempt.isFailure)
      assert(runnable.runCount == 1)
    }
  }

  "validateShutdownHook()" should {
    val conf: Config = getTestConfig()
    val state = AppRunner.createPipelineState(conf).get

    "do nothing if the shutdown hook is not defined" in {
      val attempt = AppRunner.validateShutdownHook(state, HookConfig(None, None))
      assert(attempt.isSuccess)
    }

    "return success the hook if is set" in {
      val runnable = new RunnableSpy()

      val attempt = AppRunner.validateShutdownHook(state, HookConfig(None, Some(Success(runnable))))

      assert(attempt.isSuccess)
      assert(runnable.runCount == 0)
    }

    "return failure if initialization of the hook has failed" in {
      val ex = new RuntimeException("test")

      val attempt = AppRunner.validateShutdownHook(state, HookConfig(None, Some(Failure(ex))))

      assert(attempt.isFailure)
    }
  }

  "validatePipeline" should {
    "return success when the pipeline is okay" in {
      implicit val appContext: AppContext = mock(classOf[AppContext])
      implicit val state: PipelineState = getMockPipelineState
      implicit val jobs: Seq[Job] = Seq(new JobSpy)

      when(appContext.appConfig).thenReturn(AppConfigFactory.getDummyAppConfig())

      val attempt = AppRunner.validatePipeline

      assert(attempt.isSuccess)
    }

    "throw when the pipeline is empty" in {
      implicit val appContext: AppContext = mock(classOf[AppContext])
      implicit val state: PipelineState = getMockPipelineState
      implicit val jobs: Seq[Job] = Seq.empty

      when(appContext.appConfig).thenReturn(AppConfigFactory.getDummyAppConfig())

      val attempt = AppRunner.validatePipeline

      assert(attempt.isFailure)
      assert(attempt.failed.get.getCause.getMessage == "No jobs defined in the pipeline. Please, define one or more operations.")
    }

    "throw when the run date is before the minimum allowed write date date" in {
      implicit val appContext: AppContext = mock(classOf[AppContext])
      implicit val state: PipelineState = getMockPipelineState
      implicit val jobs: Seq[Job] = Seq(new JobSpy)


      val startDate = LocalDate.parse("2023-11-12")
      val infoDateConfig = InfoDateConfigFactory.getDummyInfoDateConfig(startDate = startDate)
      val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(runDate = LocalDate.parse("2023-11-11"))

      when(appContext.appConfig).thenReturn(AppConfigFactory.getDummyAppConfig(infoDateDefaults = infoDateConfig, runtimeConfig = runtimeConfig))

      val attempt = AppRunner.validatePipeline

      assert(attempt.isFailure)
      assert(attempt.failed.get.getCause.getMessage == "The requested run date '2023-11-11' is older than the information start date '2023-11-12'.")
    }
  }

  "createAppContext()" should {
    "be able to initialize proper application context" in {
      implicit val conf: Config = getTestConfig()
      implicit val state: PipelineState = getMockPipelineState

      val appContext = AppRunner.createAppContext.get

      assert(appContext.bookkeeper != null)
    }

    "return a failure on error" in {
      implicit val conf: Config = ConfigFactory.empty()
      implicit val state: PipelineState = getMockPipelineState

      val appContextTry = AppRunner.createAppContext

      appContextTry match {
        case Success(_)  =>
          fail("Should have failed")
        case Failure(ex) =>
          assert(ex.getMessage.contains("An error occurred during initialization of the pipeline"))
          assert(ex.getCause.getMessage.contains("No configuration setting found for key 'pramen'"))
      }
    }
  }

  "getExecutorNodes()" should {
    "return a list of executor nodes" in {
      val nodes = AppRunner.getExecutorNodes(spark)

      assert(nodes.size == 1)
      assert(nodes.head.nonEmpty)
    }
  }

  "handleFailure()" should {
    val state = getMockPipelineState

    "pass around success" in {
      val success = AppRunner.handleFailure(Success(1), state, "dummy stage")
      assert(success.isSuccess)
      assert(success.get == 1)
    }

    "add stage info to a failure" in {
      val failure = AppRunner.handleFailure(Failure(new Exception("Test failure")), state, "dummy stage")
      assert(failure.isFailure)
      assert(failure.failed.get.getMessage.contains("An error occurred during dummy stage"))
      assert(failure.failed.get.getCause.getMessage.contains("Test failure"))
    }
  }

  "runIgnoringExceptions()" should {
    "run an action that does not throw an exception" in {
      var reached = false
      AppRunner.runIgnoringExceptions( {
        reached = true
      })
      assert(reached)
    }

    "do not throw non-fatal exceptions" in {
      var reached = false
      AppRunner.runIgnoringExceptions( {
        reached = true
        throw new RuntimeException("Test exception")
      })
      assert(reached)
    }

    "throw a fatal exception" in {
      var reached = false
      val ex = intercept[AbstractMethodError] {
        AppRunner.runIgnoringExceptions( {
          reached = true
          throw new AbstractMethodError("Test exception")
        })
      }
      assert(reached)
      assert(ex.getMessage.contains("Test exception"))
    }
  }

  private def getTestConfig(extraConf: Config = ConfigFactory.empty()): Config = {
    val configStr = ResourceUtils.getResourceString("/test/config/pipeline_v2_empty.conf")

    val configBase = ConfigFactory.parseString(configStr)

    extraConf
      .withFallback(configBase)
      .withFallback(ConfigFactory.load())
      .withValue("pramen.stop.spark.session", ConfigValueFactory.fromAnyRef(false))
      .resolve()
  }

  private def getMockPipelineState: PipelineState = {
    new PipelineStateSpy
  }
}
