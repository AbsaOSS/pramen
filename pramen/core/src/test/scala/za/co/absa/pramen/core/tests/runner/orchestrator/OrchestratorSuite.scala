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

package za.co.absa.pramen.core.tests.runner.orchestrator

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.MetastoreDependency
import za.co.absa.pramen.api.status.RunStatus.{Failed, MissingDependencies, NoData, Succeeded}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.AppContextFixture
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.mocks.runner.ConcurrentJobRunnerSpy
import za.co.absa.pramen.core.mocks.state.PipelineStateSpy
import za.co.absa.pramen.core.pipeline.OperationDef
import za.co.absa.pramen.core.runner.orchestrator.OrchestratorImpl

class OrchestratorSuite extends AnyWordSpec with SparkTestBase with AppContextFixture {
  "runJobs" should {
    val job1 = new JobSpy("Job1", "table1")
    val job2 = new JobSpy("Job2", "table2")
    val job3 = new JobSpy("Job3", "table3",
      operationDef = getOperation(Seq(
        MetastoreDependency(Seq("table1", "table2"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false))))
    val job4 = new JobSpy("Job4", "table4",
      operationDef = getOperation(Seq(
        MetastoreDependency(Seq("table1"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false))))

    "do nothing for empty jobs" in {
      withAppContext(spark) { appContext =>
        val conf = ConfigFactory.empty()

        val orchestrator = new OrchestratorImpl()
        val runner = new ConcurrentJobRunnerSpy()

        orchestrator.runJobs(Seq.empty)(conf, null, appContext, runner, spark)
      }
    }

    "invoke the runner for all jobs" in {
      withAppContext(spark) { appContext =>
        val conf = ConfigFactory.empty()

        val orchestrator = new OrchestratorImpl()
        val runner = new ConcurrentJobRunnerSpy()
        val state = new PipelineStateSpy()

        orchestrator.runJobs(Seq(job1, job2, job3))(conf, state, appContext, runner, spark)

        assert(state.completedStatuses.length == 3)

        val task1Status = state.completedStatuses.head
        val task2Status = state.completedStatuses(1)
        val task3Status = state.completedStatuses(2)

        assert(task1Status.runStatus.isInstanceOf[Succeeded])
        assert(task2Status.runStatus.isInstanceOf[Succeeded])
        assert(task3Status.runStatus.isInstanceOf[Succeeded])
      }
    }

    "invoke app state for successes and failures" in {
      withAppContext(spark) { appContext =>
        val conf = ConfigFactory.empty()

        val orchestrator = new OrchestratorImpl()
        val runner = new ConcurrentJobRunnerSpy(includeFails = true)
        val state = new PipelineStateSpy()

        orchestrator.runJobs(Seq(job1, job2, job3, job4))(conf, state, appContext, runner, spark)

        assert(state.completedStatuses.length == 4)

        val task1Status = state.completedStatuses.head
        val task2Status = state.completedStatuses(1)
        val task3Status = state.completedStatuses(2)
        val task4Status = state.completedStatuses(3)

        assert(task1Status.runStatus.isInstanceOf[Succeeded])
        assert(task2Status.runStatus.isInstanceOf[Failed])
        assert(task3Status.runStatus == NoData(false))
        assert(task4Status.runStatus.isInstanceOf[MissingDependencies])

        assert(task1Status.runInfo.exists(info => info.infoDate == runner.infoDate))
        assert(task1Status.runInfo.exists(info => info.started == runner.started))
        assert(task1Status.runInfo.exists(info => info.finished == runner.finished))
        assert(task2Status.runStatus.asInstanceOf[Failed].ex.getMessage.contains("Dummy exception"))
        assert(task4Status.runStatus.asInstanceOf[MissingDependencies].tables == Seq("table2"))
      }
    }

    "succeed even if tables specified in job definitions are not in metastore" in {
      val job4 = new JobSpy("Job4", "table4",
        operationDef = getOperation(Seq(
          MetastoreDependency(Seq("table1", "dummy_table1", "dummy_table2"), "@infoDate", None, triggerUpdates = false, isOptional = false, isPassive = false))))

      withAppContext(spark) { appContext =>
        val conf = ConfigFactory.empty()

        val orchestrator = new OrchestratorImpl()
        val runner = new ConcurrentJobRunnerSpy()
        val state = new PipelineStateSpy()

        orchestrator.runJobs(Seq(job1, job4))(conf, state, appContext, runner, spark)

        assert(state.completedStatuses.length == 2)

        val task1Status = state.completedStatuses.head
        val task2Status = state.completedStatuses(1)

        assert(task1Status.runStatus.isInstanceOf[Succeeded])
        assert(task2Status.runStatus.isInstanceOf[Succeeded])
      }
    }
  }

  private def getOperation(dependencies: Seq[MetastoreDependency]): OperationDef = {
    OperationDefFactory.getDummyOperationDef(dependencies = dependencies)
  }
}
