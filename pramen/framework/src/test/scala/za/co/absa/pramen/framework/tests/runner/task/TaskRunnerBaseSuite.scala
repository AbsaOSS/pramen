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

package za.co.absa.pramen.framework.tests.runner.task

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.WordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.framework
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.bookkeeper.Bookkeeper
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.metastore.MetaTableStats
import za.co.absa.pramen.framework.metastore.model.MetastoreDependency
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.mocks.job.JobSpy
import za.co.absa.pramen.framework.mocks.state.PipelineStateSpy
import za.co.absa.pramen.framework.pipeline._
import za.co.absa.pramen.framework.runner.task.RunStatus.{Failed, NotRan, Succeeded}
import za.co.absa.pramen.framework.runner.task.{RunStatus, TaskRunnerBase, TaskRunnerParallel}
import za.co.absa.pramen.framework.utils.SparkUtils
import za.co.absa.pramen.framework.{OperationDefFactory, RuntimeConfigFactory}

import java.time.{Instant, LocalDate}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TaskRunnerBaseSuite extends WordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "runJobTasks" should {
    "run multiple successful jobs" in {
      val (runner, _, state, tasks) = getUseCase(runFunction = () => exampleDf)

      val taskPreDefs = (infoDate :: infoDate.plusDays(1) :: Nil).map(d => framework.pipeline.TaskPreDef(d, TaskRunReason.New))

      val fut = runner.runJobTasks(tasks.head.job, taskPreDefs)

      Await.result(fut, Duration.Inf)

      val result = state.completedStatuses

      val job = tasks.head.job.asInstanceOf[JobSpy]

      assert(job.validateCount == 2)
      assert(job.runCount == 2)
      assert(job.postProcessingCount == 2)
      assert(job.saveCount == 2)
      assert(result.length == 2)
      assert(result.head.runStatus.isInstanceOf[Succeeded])
      assert(result(1).runStatus.isInstanceOf[Succeeded])
    }

    "run multiple failure jobs" in {
      val (runner, _, state, tasks) = getUseCase(runFunction = () => throw new IllegalStateException("Test exception"))

      val taskPreDefs = (infoDate :: infoDate.plusDays(1) :: Nil).map(d => framework.pipeline.TaskPreDef(d, TaskRunReason.New))

      val fut = runner.runJobTasks(tasks.head.job, taskPreDefs)

      Await.result(fut, Duration.Inf)

      val result = state.completedStatuses

      val job = tasks.head.job.asInstanceOf[JobSpy]

      assert(job.validateCount == 2)
      assert(job.runCount == 2)
      assert(job.postProcessingCount == 0)
      assert(job.saveCount == 0)
      assert(result.length == 2)
      assert(result.head.runStatus.isInstanceOf[Failed])
      assert(result(1).runStatus.isInstanceOf[Failed])
    }
  }

  "preRunCheck" when {
    val started = Instant.now()

    "job is ready" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.Ready, Some(100), Nil))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isRight)
    }

    "job is ready with warnings" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.Ready, Some(100), Seq(DependencyWarning("table1"))))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isRight)
      assert(result.right.get.dependencyWarnings.length == 1)
      assert(result.right.get.dependencyWarnings.head.table == "table1")
    }

    "job needs update" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(100), Nil))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isRight)
    }

    "job needs update with warnings" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(100), Seq(DependencyWarning("table1"))))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isRight)
      assert(result.isRight)
      assert(result.right.get.dependencyWarnings.length == 1)
      assert(result.right.get.dependencyWarnings.head.table == "table1")
    }

    "no data for the job" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.NoData, None, Nil))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus == RunStatus.NoData)
    }

    "no data for the job with warnings" in {
      val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.NoData, None, Seq(DependencyWarning("table1"))))

      val result = runner.preRunCheck(task.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus == RunStatus.NoData)
      assert(result.left.get.dependencyWarnings.length == 1)
      assert(result.left.get.dependencyWarnings.head.table == "table1")
    }

    "job already ran" when {
      "normal run" in {
        val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(100), Nil))

        val result = runner.preRunCheck(task.head, started)

        assert(result.isLeft)
        assert(result.left.get.runStatus == NotRan)
      }

      "rerun" in {
        val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(100), Nil),
          isRerun = true)

        val result = runner.preRunCheck(task.head, started)

        assert(result.isRight)
      }

      "historical" in {
        val (runner, _, state, task) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(100), Nil))

        val result = runner.preRunCheck(task.head, started)

        assert(result.isLeft)
        assert(result.left.get.runStatus == NotRan)
      }
    }

    "job has failed dependencies" in {
      val depFailure = DependencyFailure(MetastoreDependency("table1" :: Nil, "@infoDate", None, triggerUpdates = true, isOptional = false), "table1" :: Nil, "2022-02-18 - 2022-02-19" :: Nil)
      val (runner, _, state, tasks) = getUseCase(preRunCheckFunction = () => JobPreRunResult(JobPreRunStatus.FailedDependencies(depFailure :: Nil), None, Nil))

      val result = runner.preRunCheck(tasks.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus == RunStatus.FailedDependencies(depFailure :: Nil))
    }
  }

  "validate" when {
    val started = Instant.now()

    "job is ready" in {
      val (runner, _, state, task) = getUseCase(validationFunction = () => Reason.Ready)

      val result = runner.validate(task.head, started)

      assert(result.isRight)
    }

    "job not ready" in {
      val (runner, _, state, task) = getUseCase(validationFunction = () => Reason.NotReady("dummy reason"))

      val result = runner.validate(task.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus.isInstanceOf[RunStatus.ValidationFailed])
      assert(result.left.get.runStatus.asInstanceOf[RunStatus.ValidationFailed].ex.isInstanceOf[ReasonException])
    }

    "job is skipped ready" in {
      val (runner, _, state, task) = getUseCase(validationFunction = () => Reason.Skip("dummy reason"))

      val result = runner.validate(task.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus.isInstanceOf[RunStatus.Skipped])
    }

    "validate threw an exception" in {
      val ex = new IllegalStateException("TestException")
      val (runner, _, state, tasks) = getUseCase(validationFunction = () => throw ex)

      val result = runner.validate(tasks.head, started)

      assert(result.isLeft)
      assert(result.left.get.runStatus == RunStatus.ValidationFailed(ex))
    }
  }

  "run" should {
    "handle a successful task" in {
      val expectedData =
        """[ {
          |  "a" : "B",
          |  "b" : 2,
          |  "c" : "2"
          |}, {
          |  "a" : "C",
          |  "b" : 3,
          |  "c" : "3"
          |} ]""".stripMargin

      val (runner, bk, state, tasks) = getUseCase(runFunction = () => exampleDf)
      val job = tasks.head.job.asInstanceOf[JobSpy]

      val started = Instant.now()

      val result = runner.run(tasks.head, started, JobPreRunResult(JobPreRunStatus.Ready, None, Nil))

      assert(result.runStatus.isInstanceOf[Succeeded])

      val success = result.runStatus.asInstanceOf[Succeeded]

      assert(success.recordCountOld.isEmpty)
      assert(success.recordCount == 2)
      assert(success.sizeBytes.contains(100))

      val actualData = SparkUtils.convertDataFrameToPrettyJSON(job.saveDf)

      compareText(actualData, expectedData)
    }

    "handle a failed task" in {
      val (runner, bk, state, tasks) = getUseCase(runFunction = () => throw new IllegalStateException("TestException"))

      val started = Instant.now()

      val result = runner.run(tasks.head, started, JobPreRunResult(JobPreRunStatus.Ready, Some(150), Nil))

      assert(result.runStatus.isInstanceOf[Failed])

      val failure = result.runStatus.asInstanceOf[Failed]

      assert(failure.ex.getMessage == "TestException")
      assert(bk.getDataChunks("table_out", infoDate, infoDate).isEmpty)
    }

    "handle a dry run" in {
      val (runner, bk, state, tasks) = getUseCase(runFunction = () => exampleDf, isDryRun = true)

      val started = Instant.now()

      val result = runner.run(tasks.head, started, JobPreRunResult(JobPreRunStatus.Ready, Some(150), Nil))

      assert(result.runStatus.isInstanceOf[Succeeded])

      val success = result.runStatus.asInstanceOf[Succeeded]

      assert(success.recordCount == 2)
      assert(success.sizeBytes.isEmpty)
      assert(bk.getDataChunks("table_out", infoDate, infoDate).isEmpty)
    }
  }

  "handleSchemaChange" should {
    "register a new schema" in {
      val (runner, bk, state, _) = getUseCase(runFunction = () => exampleDf)

      runner.handleSchemaChange(exampleDf, "table", infoDate)

      val schemaOpt1 = bk.getLatestSchema("table", infoDate.minusDays(1))
      val schemaOpt2 = bk.getLatestSchema("table", infoDate)

      assert(schemaOpt1.isEmpty)
      assert(schemaOpt2.nonEmpty)
    }

    "do nothing if schemas are the same" in {
      val (runner, bk, state, _) = getUseCase(runFunction = () => exampleDf)

      bk.saveSchema("table", infoDate.minusDays(10), exampleDf.schema)

      runner.handleSchemaChange(exampleDf, "table", infoDate)

      val schemaOpt1 = bk.getLatestSchema("table", infoDate)
      val schemaOpt2 = bk.getLatestSchema("table", infoDate.minusDays(11))

      assert(schemaOpt1.nonEmpty)
      assert(schemaOpt2.isEmpty)

      assert(schemaOpt1.get._2 == infoDate.minusDays(10))
    }

    "register schema update" in {
      val (runner, bk, state, _) = getUseCase(runFunction = () => exampleDf)

      bk.saveSchema("table", infoDate.minusDays(10), exampleDf.schema)

      val df2 = exampleDf.withColumn("c", lit(3))

      runner.handleSchemaChange(df2, "table", infoDate)

      val schemaOpt1 = bk.getLatestSchema("table", infoDate.minusDays(1))
      val schemaOpt2 = bk.getLatestSchema("table", infoDate)
      val schemaOpt3 = bk.getLatestSchema("table", infoDate.plusDays(1))

      assert(schemaOpt1.nonEmpty)
      assert(schemaOpt2.nonEmpty)
      assert(schemaOpt3.nonEmpty)

      assert(schemaOpt1.get._2 == infoDate.minusDays(10))
      assert(schemaOpt2.get._2 == infoDate)
      assert(schemaOpt3.get._2 == infoDate)
    }
  }

  def getUseCase(infoDates: Seq[LocalDate] = infoDate :: Nil,
                 preRunCheckFunction: () => JobPreRunResult = () => JobPreRunResult(JobPreRunStatus.Ready, None, Nil),
                 validationFunction: () => Reason = () => Reason.Ready,
                 runFunction: () => DataFrame = () => null,
                 isDryRun: Boolean = false,
                 isRerun: Boolean = false,
                 bookkeeperIn: Bookkeeper = null
                ): (TaskRunnerBase, Bookkeeper, PipelineStateSpy, Seq[Task]) = {
    val conf = ConfigFactory.empty()

    val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(isRerun = isRerun, isDryRun = isDryRun)

    val bookkeeper = if (bookkeeperIn == null) new SyncBookkeeperMock else bookkeeperIn

    val state = new PipelineStateSpy

    val operationDef = OperationDefFactory.getDummyOperationDef(
      schemaTransformations = List(TransformExpression("c", "cast(b as string)")),
      filters = List("b > 1")
    )

    val stats = MetaTableStats(2, Some(100))

    val job = new JobSpy(preRunCheckFunction = preRunCheckFunction, validationFunction = validationFunction, runFunction = runFunction, operationDef = operationDef, saveStats = stats)

    val tasks = infoDates.map(d => framework.pipeline.Task(job, d, TaskRunReason.New))

    val runner = new TaskRunnerParallel(conf, bookkeeper, state, runtimeConfig)

    (runner, bookkeeper, state, tasks)
  }

}
