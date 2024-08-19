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
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy

import java.time.{Instant, LocalDate}

class TransformationJobSuite extends AnyWordSpec with SparkTestBase {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val conf = ConfigFactory.empty()
  private val runReason: TaskRunReason = TaskRunReason.New

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "preRunCheckJob" should {
    "return Ready" in {
      val (job, _) = getUseCase(validateFunction = () => Reason.Ready)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual.status == JobPreRunStatus.Ready)
    }
  }

  "validate" should {
    "return Ready when the underlying validation returns ready" in {
      val (job, _) = getUseCase(validateFunction = () => Reason.Ready)

      val actual = job.validate(infoDate, conf)

      assert(actual == Reason.Ready)
    }

    "return NotReady when the underlying validation returns not ready" in {
      val (job, _) = getUseCase(validateFunction = () => Reason.NotReady("DummyNotReady"))

      val actual = job.validate(infoDate, conf)

      assert(actual == Reason.NotReady("DummyNotReady"))
    }

    "return Skip when the underlying validation returns skip" in {
      val (job, _) = getUseCase(validateFunction = () => Reason.Skip("DummySkip"))

      val actual = job.validate(infoDate, conf)

      assert(actual == Reason.Skip("DummySkip"))
    }

    "passes the exception thrown by the transformer validator" in {
      val (job, _) = getUseCase(validateFunction = () => throw new IllegalStateException("DummyMsg"))

      val ex = intercept[IllegalStateException] {
        job.validate(infoDate, conf)
      }

      assert(ex.getMessage == "DummyMsg")
    }
  }

  "run" should {
    "invoke run() method of the transformer" in {
      val (job, _) = getUseCase(runFunction = () => exampleDf)

      val actual = job.run(infoDate, conf).data

      assert(actual.count() == 3)
    }
  }

  "postProcess" should {
    "return the same input as given" in {
      val (job, _) = getUseCase()

      val actual = job.postProcessing(null, infoDate, conf)

      assert(actual == null)
    }

    "return the same dataframe as given" in {
      val (job, _) = getUseCase()

      val actual = job.postProcessing(exampleDf, infoDate, conf)

      assert(actual.count() == 3)
    }
  }

  "save" should {
    "invoke save() method of teh metastore" in {
      val (job, mt) = getUseCase()

      job.save(exampleDf, infoDate, conf, Instant.now(), None)

      assert(mt.saveTableInvocations.length == 1)
      assert(mt.saveTableInvocations.head._1 == "table1")
      assert(mt.saveTableInvocations.head._2 == infoDate)
      assert(mt.saveTableInvocations.head._3.count() == 3)
    }
  }

  def getUseCase(validateFunction: () => Reason = () => Reason.Ready,
                 runFunction: () => DataFrame = () => null): (TransformationJob, MetastoreSpy) = {
    val transformer = new TransformerSpy(validateFunction, runFunction)

    val operation = OperationDefFactory.getDummyOperationDef(extraOptions = Map[String, String]("value" -> "7"))

    val metastore = new MetastoreSpy()

    val bk = new SyncBookkeeperMock

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table1")

    (new TransformationJob(operation, metastore, bk, Nil, outputTable, transformer), metastore)
  }

}
