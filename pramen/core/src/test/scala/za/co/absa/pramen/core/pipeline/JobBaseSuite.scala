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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{MetastoreDependency, TaskRunReason}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.JobBaseDummy
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.pipeline.JobPreRunStatus.FailedDependencies

import java.time.{Instant, LocalDate}

class JobBaseSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture {

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val runReason: TaskRunReason = TaskRunReason.New

  "allowRunningTasksInParallel()" should {
    "be true for jobs that don't have dependencies" in {
      val job = getUseCase()

      assert(job.allowRunningTasksInParallel)
    }

    "be true for jobs that don't have self-dependencies" in {
      val dep = MetastoreDependency(Seq("table1", "table2"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false)
      val job = getUseCase(dependencies = Seq(dep))

      assert(job.allowRunningTasksInParallel)
    }

    "be false for jobs that don't allow parallel execution" in {
      val dep = MetastoreDependency(Seq("table1", "table2"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false)
      val job = getUseCase(allowParallel = false, dependencies = Seq(dep))

      assert(!job.allowRunningTasksInParallel)
    }

    "be false for jobs that have self-dependencies" in {
      val dep = MetastoreDependency(Seq("table1", "test_output_table"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false)
      val job = getUseCase(dependencies = Seq(dep))

      assert(!job.allowRunningTasksInParallel)
    }
  }

  "getInfoDateRange()" should {
    "return correct range when both from and to are not provided" in {
      val job = getUseCase()

      val (from, to) = job.getInfoDateRange(infoDate, None, None)

      assert(from == infoDate)
      assert(to == infoDate)
    }

    "return correct range when from is provided but to is not" in {
      val job = getUseCase()

      val (from, to) = job.getInfoDateRange(infoDate, Some("@infoDate - 5"), None)

      assert(from == infoDate.minusDays(5))
      assert(to == infoDate)
    }

    "return correct range when to is provided but from is not" in {
      val job = getUseCase()

      val (from, to) = job.getInfoDateRange(infoDate, None, Some("@infoDate + 5"))

      assert(from == infoDate)
      assert(to == infoDate.plusDays(5))
    }

    "return correct range when both from and to are provided" in {
      val job = getUseCase()

      val (from, to) = job.getInfoDateRange(infoDate, Some("@infoDate - 5"), Some("@infoDate + 5"))

      assert(from == infoDate.minusDays(5))
      assert(to == infoDate.plusDays(5))
    }

    "throw an exception when the resulting date range is invalid" in {
      val job = getUseCase()

      val ex = intercept[IllegalArgumentException] {
        job.getInfoDateRange(infoDate, Some("@infoDate + 1"), Some("@infoDate - 1"))
      }

      assert(ex.getMessage.contains("Incorrect date range specified for test_output_table: from=2022-01-19 > to=2022-01-17"))
    }

    "throw an exception when an expression is invalid" in {
      val job = getUseCase()

      val ex = intercept[SyntaxErrorException] {
        job.getInfoDateRange(infoDate, Some("@unknownDate + 1"), None)
      }

      assert(ex.getMessage.contains("Unset variable 'unknownDate' used"))
    }
  }

  "preRunCheck()" should {
    "return failure on failed dependencies" in {
      val conf = ConfigFactory.empty()
      val dep = MetastoreDependency(Seq("table1"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false)
      val job = getUseCase(dependencies = Seq(dep), isTableAvailable = false)

      val actual = job.preRunCheck(infoDate, runReason, conf)

      assert(actual.dependencyWarnings.isEmpty)
      assert(actual.status.isInstanceOf[FailedDependencies])
      assert(actual.status.asInstanceOf[FailedDependencies].failures.head.emptyTables.isEmpty)
      assert(actual.status.asInstanceOf[FailedDependencies].failures.head.failedTables.head == "table1")
      assert(actual.status.asInstanceOf[FailedDependencies].isFailure)
    }

    "return failure on failed passive dependencies" in {
      val conf = ConfigFactory.empty()
      val dep = MetastoreDependency(Seq("table1"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = true)
      val job = getUseCase(dependencies = Seq(dep), isTableAvailable = false)

      val actual = job.preRunCheck(infoDate, runReason, conf)

      assert(actual.dependencyWarnings.isEmpty)
      assert(actual.status.isInstanceOf[FailedDependencies])
      assert(actual.status.asInstanceOf[FailedDependencies].failures.head.emptyTables.isEmpty)
      assert(actual.status.asInstanceOf[FailedDependencies].failures.head.failedTables.head == "table1")
      assert(!actual.status.asInstanceOf[FailedDependencies].isFailure)
    }

    "return failure on empty tables" in {
      val conf = ConfigFactory.empty()
      val dep = MetastoreDependency(Seq("table2"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false)
      val job = getUseCase(dependencies = Seq(dep), isTableAvailable = false, isTableEmpty = true)

      val actual = job.preRunCheck(infoDate, runReason, conf)

      assert(actual.dependencyWarnings.isEmpty)
      assert(actual.status.isInstanceOf[FailedDependencies])
      assert(actual.status.asInstanceOf[FailedDependencies].failures.head.emptyTables.head == "table2")
    }

    "return warnings on failed optional dependencies" in {
      val conf = ConfigFactory.empty()
      val dep = MetastoreDependency(Seq("table1"), "@infoDate", None, triggerUpdates = false, isOptional = true, isPassive = false)
      val job = getUseCase(dependencies = Seq(dep), isTableAvailable = false)

      val actual = job.preRunCheck(infoDate.plusDays(1), runReason, conf)

      assert(actual.dependencyWarnings.nonEmpty)
      assert(actual.dependencyWarnings.head.table == "table1")
    }
  }

  "getTookTooLongWarnings" should {
    "return empty seq if maximum time is not defined" in {
      val job = getUseCase()

      val warnings = job.getTookTooLongWarnings(Instant.ofEpochSecond(1000), Instant.ofEpochSecond(1010), None)

      assert(warnings.isEmpty)
    }

    "return empty seq if actual time is less than operation maximum time" in {
      val job = getUseCase(warnMaxExecutionTimeSeconds = Some(100))

      val warnings = job.getTookTooLongWarnings(Instant.ofEpochSecond(1000), Instant.ofEpochSecond(1010), None)

      assert(warnings.isEmpty)
    }

    "return empty seq if actual time is less than table maximum time" in {
      val job = getUseCase()

      val warnings = job.getTookTooLongWarnings(Instant.ofEpochSecond(1000), Instant.ofEpochSecond(1010), Some(100))

      assert(warnings.isEmpty)
    }

    "return a warning actual time is bigger than minimum of operation and table times" in {
      val job = getUseCase(warnMaxExecutionTimeSeconds = Some(100))

      val warnings = job.getTookTooLongWarnings(Instant.ofEpochSecond(1000), Instant.ofEpochSecond(1300), Some(200))

      assert(warnings.nonEmpty)
      assert(warnings.head == "The job took longer (00:05:00) than expected (00:01:40).")
    }
  }

  "createOrRefreshHiveTable" should {
    "do nothing if Hive table is not defined" in {
      val job = getUseCase()

      val warnings = job.createOrRefreshHiveTable(null, infoDate, recreate = false)

      assert(warnings.isEmpty)

    }

    "return an empty seq of warnings when the operation succeeded" in {
      val job = getUseCase(hiveTable = Some("test_hive_table"))

      val warnings = job.createOrRefreshHiveTable(null, infoDate, recreate = false)

      assert(warnings.isEmpty)
    }

    "return warnings if ignore failures enabled" in {
      val job = getUseCase(hiveTable = Some("test_hive_table"), hiveFailure = true, ignoreHiveFailures = true)

      val warnings = job.createOrRefreshHiveTable(null, infoDate, recreate = false)

      assert(warnings.nonEmpty)
      assert(warnings.head == "Failed to create or update Hive table 'test_hive_table': Test exception")
    }

    "re-throw the exception if ignore failures disabled" in {
      val job = getUseCase(hiveTable = Some("test_hive_table"), hiveFailure = true)

      val ex = intercept[RuntimeException] {
        job.createOrRefreshHiveTable(null, infoDate, recreate = false)
      }

      assert(ex.getMessage == "Test exception")
    }
  }

  def getUseCase(tableDf: DataFrame = null,
                 dependencies: Seq[MetastoreDependency] = Nil,
                 isTableAvailable: Boolean = true,
                 isTableEmpty: Boolean = false,
                 allowParallel: Boolean = true,
                 warnMaxExecutionTimeSeconds: Option[Int] = None,
                 hiveTable: Option[String] = None,
                 hiveFailure: Boolean = false,
                 ignoreHiveFailures: Boolean = false): JobBase = {
    val operation = OperationDefFactory.getDummyOperationDef(dependencies = dependencies,
      allowParallel = allowParallel,
      warnMaxExecutionTimeSeconds = warnMaxExecutionTimeSeconds,
      extraOptions = Map[String, String]("value" -> "7"))

    val bk = new SyncBookkeeperMock

    val metastore = new MetastoreSpy(tableDf = tableDf, isTableAvailable = isTableAvailable, isTableEmpty = isTableEmpty, failHive = hiveFailure)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "test_output_table",
      hiveTable = hiveTable,
      hiveConfig = HiveConfig.getNullConfig.copy(ignoreFailures = ignoreHiveFailures)
    )

    new JobBaseDummy(operation, Nil, metastore, bk, outputTable)
  }
}
