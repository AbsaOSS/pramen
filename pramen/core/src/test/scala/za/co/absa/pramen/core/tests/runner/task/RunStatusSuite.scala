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

package za.co.absa.pramen.core.tests.runner.task

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{DependencyFailure, MetastoreDependency, RunStatus, TaskRunReason}

class RunStatusSuite extends AnyWordSpec {
  "toString" should {
    "Succeeded" when {
      "New" in {
        val status = RunStatus.Succeeded(None, 0, None, TaskRunReason.New, Nil, Nil, Nil, Nil)

        assert(status.toString == "New")
      }
      "Update" in {
        val status = RunStatus.Succeeded(None, 0, None, TaskRunReason.Update, Nil, Nil, Nil, Nil)

        assert(status.toString == "Update")
      }
    }

    "Validation Failed" in {
      val status = RunStatus.ValidationFailed(null)

      assert(status.toString == "Validation failed")
    }

    "Failed" in {
      val status = RunStatus.Failed(null)

      assert(status.toString == "Failed")
    }

    "MissingDependencies" in {
      val status = RunStatus.MissingDependencies(isFailure = false, null)

      assert(status.toString == "Dependent job failed")
    }

    "FailedDependencies" in {
      val status = RunStatus.FailedDependencies(isFailure = false, null)

      assert(status.toString == "Dependency check failed")
    }

    "NoData" in {
      val status = RunStatus.NoData(false)

      assert(status.toString == "No data")
    }

    "InsufficientData" in {
      val status = RunStatus.InsufficientData(0, 0, None)

      assert(status.toString == "Insufficient data")
    }

    "NotRan" in {
      val status = RunStatus.NotRan

      assert(status.toString == "Not ran")
    }

    "Skipped" in {
      val status = RunStatus.Skipped(null)

      assert(status.toString == "Skipped")
    }
  }

  "isFailure" should {
    "Succeeded" in {
       val status = RunStatus.Succeeded(None, 0, None, TaskRunReason.New, Nil, Nil, Nil, Nil)

       assert(!status.isFailure)
    }

    "Validation Failed" in {
      val status = RunStatus.ValidationFailed(null)

      assert(status.isFailure)
    }

    "Failed" in {
      val status = RunStatus.Failed(null)

      assert(status.isFailure)
    }

    "MissingDependencies" when {
      "failure" in {
        val status = RunStatus.MissingDependencies(isFailure = true, null)

        assert(status.isFailure)
      }

      "not a failure" in {
        val status = RunStatus.MissingDependencies(isFailure = false, null)

        assert(!status.isFailure)
      }
    }

    "FailedDependencies" when {
      "failure" in {
        val status = RunStatus.FailedDependencies(isFailure = true, null)

        assert(status.isFailure)
      }

      "not a failure" in {
        val status = RunStatus.FailedDependencies(isFailure = false, null)

        assert(!status.isFailure)
      }
    }

    "NoData" when {
      "failure" in {
        val status = RunStatus.NoData(isFailure = true)

        assert(status.isFailure)
      }

      "not a failure" in {
        val status = RunStatus.NoData(isFailure = false)

        assert(!status.isFailure)
      }
    }

    "InsufficientData" in {
      val status = RunStatus.InsufficientData(0, 0, None)

      assert(status.isFailure)
    }

    "NotRan" in {
      val status = RunStatus.NotRan

      assert(!status.isFailure)
    }

    "Skipped" in {
      val status = RunStatus.Skipped(null)

      assert(!status.isFailure)
    }
  }

  "getReason" should {
    "Succeeded" in {
      val status = RunStatus.Succeeded(None, 0, None, TaskRunReason.New, Nil, Nil, Nil, Nil)

      assert(status.getReason().isEmpty)
    }

    "Validation Failed" in {
      val status = RunStatus.ValidationFailed(new RuntimeException("test1", new RuntimeException("test2")))

      assert(status.getReason().contains("test1 (test2)"))
    }

    "Failed" in {
      val status = RunStatus.Failed(new RuntimeException("test1", new RuntimeException("test2")))

      assert(status.getReason().contains("test1 (test2)"))
    }

    "MissingDependencies" in {
      val status = RunStatus.MissingDependencies(isFailure = false, Seq("table2", "table1"))

      assert(status.getReason().contains("Dependent job failures: table2, table1"))
    }

    "FailedDependencies" in {
      val status = RunStatus.FailedDependencies(isFailure = false, Seq(
        DependencyFailure(
          MetastoreDependency(Seq("table2", "table1", "table3"), "", None, triggerUpdates = false, isOptional = false, isPassive = false),
          Seq("table3"),
          Seq("table1"),
          Seq("2022-01-01")
        )))

      assert(status.getReason().contains("Dependency check failures: table3 (Empty table or wrong table name), table1 (2022-01-01)"))
    }

    "NoData" in {
      val status = RunStatus.NoData(false)

      assert(status.getReason().contains("No data at the source"))
    }

    "InsufficientData" in {
      val status = RunStatus.InsufficientData(100, 200, None)

      assert(status.getReason().contains("Got 100, expected at least 200 records"))
    }

    "NotRan" in {
      val status = RunStatus.NotRan

      assert(status.getReason().isEmpty)
    }

    "Skipped" when {
      "empty reason" in {
        val status = RunStatus.Skipped("")

        assert(status.getReason().isEmpty)
      }
      "non-empty reason" in {
        val status = RunStatus.Skipped("My reason")

        assert(status.getReason().contains("My reason"))
      }
    }
  }
}
