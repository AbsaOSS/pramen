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

package za.co.absa.pramen.core.source

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.journal.JournalMock
import za.co.absa.pramen.core.mocks.lock.TokenLockFactoryMock
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.mocks.state.PipelineStateSpy
import za.co.absa.pramen.core.mocks.{MetaTableFactory, SourceTableFactory}
import za.co.absa.pramen.core.pipeline.{IngestionJob, Job, OperationType}
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunnerImpl
import za.co.absa.pramen.core.runner.task.TaskRunnerMultithreaded
import za.co.absa.pramen.core.utils.LocalFsUtils
import za.co.absa.pramen.core.{OperationDefFactory, RuntimeConfigFactory}

import java.io.File
import java.time.LocalDate

class SourceValidationSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {
  private val runDate = LocalDate.of(2022, 2, 18)

  val tempDir: String = createTempDir("source_validation")
  val sourceTemp = new Path(tempDir, "temp")
  val filesPath = new Path(tempDir, "files")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    LocalFsUtils.deleteTemp(new File(tempDir))
  }

  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
       |   sources = [
       |    {
       |      name = "source_mock1"
       |      factory.class = "za.co.absa.pramen.core.mocks.source.SourceMock"
       |    },
       |    {
       |      name = "source_mock2"
       |      factory.class = "za.co.absa.pramen.core.mocks.source.SourceMock"
       |
       |      validation.warning = "Dummy Warning"
       |    },
       |    {
       |      name = "source_mock3"
       |      factory.class = "za.co.absa.pramen.core.mocks.source.SourceMock"
       |
       |      throw.validation.exception = true
       |    },
       |  ]
       | }
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "Source" should {
    "be usable from an ingestion job" in {
      val (runner, _, state, job) = getIngestionUseCase()

      runner.runJob(job)

      val results = state.completedStatuses

      val taskResult = results.head
      val status = taskResult.runStatus

      assert(status.isInstanceOf[RunStatus.Succeeded])
      assert(status.asInstanceOf[RunStatus.Succeeded].warnings.isEmpty)
    }

    "pass validation warnings to the end result" in {
      val (runner, _, state, job) = getIngestionUseCase(sourceName = "source_mock2")

      runner.runJob(job)

      val results = state.completedStatuses

      val taskResult = results.head
      val status = taskResult.runStatus

      assert(status.isInstanceOf[RunStatus.Succeeded])
      assert(status.asInstanceOf[RunStatus.Succeeded].warnings.contains("Dummy Warning"))
    }

    "pass validation exception to the end result" in {
      val (runner, _, state, job) = getIngestionUseCase(sourceName = "source_mock3")

      runner.runJob(job)

      val results = state.completedStatuses

      val taskResult = results.head
      val status = taskResult.runStatus

      assert(status.isInstanceOf[RunStatus.ValidationFailed])
      assert(status.asInstanceOf[RunStatus.ValidationFailed].ex.getMessage == "Validation test exception")
    }
  }

  def getIngestionUseCase(runDateIn: LocalDate = runDate, sourceName: String = "source_mock1"): (ConcurrentJobRunnerImpl, Bookkeeper, PipelineStateSpy, Job) = {
    val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(runDate = runDateIn)

    val metastore = new MetastoreSpy()
    val bookkeeper = new SyncBookkeeperMock
    val journal = new JournalMock
    val tokenLockFactory = new TokenLockFactoryMock

    val state = new PipelineStateSpy

    bookkeeper.setRecordCount("table_out", runDate.minusDays(1), runDate.minusDays(1), runDate.minusDays(1), 1, 1, 0, 0, isTableTransient = false)

    val sourceTable = SourceTableFactory.getDummySourceTable()

    val table1Path = new Path(tempDir, "table1")
    val table1Format = DataFormat.Parquet(table1Path.toString, None)

    val metaTable = MetaTableFactory.getDummyMetaTable(name = "table1", format = table1Format)

    val operationDef = OperationDefFactory.getDummyOperationDef(
      name = "Dummy ingestion",
      operationType = OperationType.Ingestion(sourceName, Seq(sourceTable))
    )

    val source = SourceManager.getSourceByName(sourceName, conf, None)

    val job = new IngestionJob(operationDef, metastore, bookkeeper, Nil, sourceName, source, sourceTable, metaTable, "", None, false)

    val taskRunner = new TaskRunnerMultithreaded(conf, bookkeeper, journal, tokenLockFactory, state, runtimeConfig, "app_123")

    val jobRunner = new ConcurrentJobRunnerImpl(runtimeConfig, bookkeeper, taskRunner, "app_123")

    (jobRunner, bookkeeper, state, job)
  }

}
