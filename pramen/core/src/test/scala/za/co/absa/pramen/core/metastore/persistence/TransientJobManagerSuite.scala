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

package za.co.absa.pramen.core.metastore.persistence

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{RunStatus, TaskRunReason}
import za.co.absa.pramen.api.{CachePolicy, DataFormat}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.metastore.peristence.TransientJobManager.{MAXIMUM_UNIONS, WARN_UNIONS}
import za.co.absa.pramen.core.metastore.peristence.{TransientJobManager, TransientTableManager}
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.task.TaskRunner

import java.time.LocalDate
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TransientJobManagerSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private var tempDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    tempDir = createTempDir("transient_persist")
  }

  override def afterAll(): Unit = {
    deleteDir(tempDir)

    super.afterAll()
  }

  "setTaskRunner" should {
    "set the specified task runner" in {
      val taskRunner = mock(classOf[TaskRunner])

      assert(!TransientJobManager.hasTaskRunner)

      TransientJobManager.setTaskRunner(taskRunner)

      assert(TransientJobManager.hasTaskRunner)

      TransientJobManager.reset()
    }
  }

  "runLazyTasks" should {
    "return an empty dataframe when the job is not scheduled for the specified information date" in {
      val df = TransientJobManager.runLazyTasks("table1", Seq.empty)

      assert(df.isEmpty)
      assert(df.schema.fields.isEmpty)
    }

    "warn if the number of unions exceeds recommended amount" in {
      val infoDates = Range(0, WARN_UNIONS + 1)
        .map { i =>
          TransientTableManager.addRawDataFrame("table_dummy1", infoDate.plusDays(i), exampleDf)
          infoDate.plusDays(i)
        }

      val job = mock(classOf[Job])

      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))

      TransientJobManager.runLazyTasks("table_dummy1", infoDates)

      TransientJobManager.reset()
      TransientTableManager.reset()
    }

    "throw an exception if the number of unions exceeds maximum supported" in {
      val infoDates = Range(0, MAXIMUM_UNIONS + 1)
        .map { i =>
          infoDate.plusDays(i)
        }

      val job = mock(classOf[Job])

      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))

      val ex = intercept[IllegalArgumentException] {
        TransientJobManager.runLazyTasks("table_dummy1", infoDates)
      }

      TransientJobManager.reset()
      TransientTableManager.reset()

      assert(ex.getMessage == "Lazy job with output table name 'table_dummy1' not found or haven't registered yet.")
    }
  }

  "runLazyTask" should {
    "be able to wait for a task that is already running" in {
      val fut = Future {
        Thread.sleep(1000)
        exampleDf
      }

      TransientJobManager.testAndSetRunningJobFuture("table_dummy2", infoDate, fut)
      val actualFuture = TransientJobManager.getLazyTaskFuture("table_dummy2", infoDate)

      assert(actualFuture == fut)

      val df = Await.result(actualFuture, Duration.Inf)

      assert(df.schema.length == 2)
      assert(df.schema.fields.head.name == "a")

      TransientJobManager.reset()
    }
  }

  "getJob" should {
    "return the job if found in the list of registered jobs" in {
      val job = mock(classOf[Job])

      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))

      TransientJobManager.addLazyJob(job)

      assert(TransientJobManager.getJob("table_dummy1") == job)

      TransientJobManager.reset()
    }

    "throw an exception if the job is not registered" in {
      val ex = intercept[IllegalArgumentException] {
        TransientJobManager.getJob("table_dummy1")
      }

      assert(ex.getMessage == "Lazy job with output table name 'table_dummy1' not found or haven't registered yet.")

      TransientJobManager.reset()
    }
  }

  "runJob" should {
    "runs a job via teh task runner and return the dataframe" in {
      val taskRunner = mock(classOf[TaskRunner])
      val job = mock(classOf[Job])
      val successStatus = RunStatus.Succeeded(None, 1, None, TaskRunReason.OnRequest, Nil, Nil, Nil, Nil)

      whenMock(taskRunner.runLazyTask(job, infoDate)).thenReturn(successStatus)
      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table1", format = DataFormat.Transient(CachePolicy.NoCache)))
      TransientTableManager.addRawDataFrame("table1", infoDate, exampleDf)

      TransientJobManager.setTaskRunner(taskRunner)

      val df = TransientJobManager.runJob(job, infoDate)

      TransientJobManager.reset()
      TransientTableManager.reset()

      assert(df.schema.fields.length == 2)
      assert(df.schema.fields.head.name == "a")
    }

    "throw if the task runner is not set" in {
      TransientJobManager.reset()
      val job = mock(classOf[Job])
      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))

      val ex = intercept[IllegalStateException] {
        TransientJobManager.runJob(job, infoDate)
      }

      assert(ex.getMessage == "Task runner is not set.")
    }

    "return an empty dataframe on success skip" in {
      val taskRunner = mock(classOf[TaskRunner])
      val job = mock(classOf[Job])
      val status = RunStatus.Skipped("dummy")

      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))
      whenMock(taskRunner.runLazyTask(job, infoDate)).thenReturn(status)

      TransientJobManager.setTaskRunner(taskRunner)

      val df = TransientJobManager.runJob(job, infoDate)

      assert(df.isEmpty)

      TransientJobManager.reset()
    }

    "throw if the job runner returns 'ValidationFailed'" in {
      val taskRunner = mock(classOf[TaskRunner])
      val job = mock(classOf[Job])
      val status = RunStatus.ValidationFailed(new RuntimeException("dummy"))

      whenMock(taskRunner.runLazyTask(job, infoDate)).thenReturn(status)
      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table1", format = DataFormat.Transient(CachePolicy.NoCache)))

      TransientJobManager.setTaskRunner(taskRunner)

      val ex = intercept[IllegalStateException] {
        TransientJobManager.runJob(job, infoDate)
      }

      assert(ex.getMessage == "Lazy job outputting to 'table1' for '2022-02-18' validation failed.")
      assert(ex.getCause.getMessage == "dummy")

      TransientJobManager.reset()
    }

    "throw if the job runner returns 'Failed'" in {
      val taskRunner = mock(classOf[TaskRunner])
      val job = mock(classOf[Job])
      val status = RunStatus.Failed(new RuntimeException("dummy"))

      whenMock(taskRunner.runLazyTask(job, infoDate)).thenReturn(status)
      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table1", format = DataFormat.Transient(CachePolicy.NoCache)))

      TransientJobManager.setTaskRunner(taskRunner)

      val ex = intercept[RuntimeException] {
        TransientJobManager.runJob(job, infoDate)
      }

      assert(ex.getMessage == "Lazy job outputting to 'table1' for '2022-02-18' failed.")
      assert(ex.getCause.getMessage == "dummy")

      TransientJobManager.reset()
    }

    "throw if the job runner returns some other failure status" in {
      val taskRunner = mock(classOf[TaskRunner])
      val job = mock(classOf[Job])
      val status = RunStatus.NoData(true)

      whenMock(job.outputTable).thenReturn(MetaTableFactory.getDummyMetaTable("table_dummy1", format = DataFormat.Transient(CachePolicy.NoCache)))
      whenMock(taskRunner.runLazyTask(job, infoDate)).thenReturn(status)

      TransientJobManager.setTaskRunner(taskRunner)

      val ex = intercept[IllegalStateException] {
        TransientJobManager.runJob(job, infoDate)
      }

      assert(ex.getMessage == "Lazy job outputting to 'table_dummy1' for '2022-02-18' failed to run (No data at the source).")

      TransientJobManager.reset()
    }
  }

  "safeUnion" should {
    "work when both dataframes are not empty" in {
      val df1 = exampleDf
      val df2 = exampleDf

      val df = TransientJobManager.safeUnion(df1, df2)

      assert(df.count() == 6)
      assert(df.schema.fields.head.name == "a")
      assert(df.schema.fields(1).name == "b")
    }

    "work when the first dataframe is empty" in {
      val df1 = spark.emptyDataFrame
      val df2 = exampleDf

      val df = TransientJobManager.safeUnion(df1, df2)

      assert(df.count() == 3)
      assert(df.schema.fields.head.name == "a")
      assert(df.schema.fields(1).name == "b")
    }

    "work when the second dataframe is empty" in {
      val df1 = exampleDf
      val df2 = spark.emptyDataFrame

      val df = TransientJobManager.safeUnion(df1, df2)

      assert(df.count() == 3)
      assert(df.schema.fields.head.name == "a")
      assert(df.schema.fields(1).name == "b")
    }

    "work when both dataframes are empty" in {
      val df1 = spark.emptyDataFrame
      val df2 = spark.emptyDataFrame

      val df = TransientJobManager.safeUnion(df1, df2)

      assert(df.isEmpty)
      assert(df.schema.fields.isEmpty)
    }
  }
}
