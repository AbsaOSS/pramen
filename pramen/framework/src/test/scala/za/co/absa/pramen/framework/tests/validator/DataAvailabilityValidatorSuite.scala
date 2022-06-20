/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.framework.tests.validator

import org.scalatest.WordSpec
import za.co.absa.pramen.framework.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.framework.samples.DataValidationTestCaseBuilder
import za.co.absa.pramen.framework.validator.RunDecision

class DataAvailabilityValidatorSuite extends WordSpec {
  "validateTask()" should {
    "succeed" when {
      "empty validation" in {
        val builder = new DataValidationTestCaseBuilder
        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }

      "one mandatory table" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addCheck()

        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }

      "several up to date mandatory tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addMandatoryUpToDateTable("b")
        builder.addMandatoryUpToDateTable("c")
        builder.addCheck("minusMonths(@infoDate, 1)", "")

        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }

      "one optional table" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addOptionalUpToDateTable("a")
        builder.addCheck()

        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }

      "several optional tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addOptionalUpToDateTable("d")
        builder.addOptionalOutdatedTable("e")
        builder.addCheck("minusMonths(@infoDate, 1)", "")

        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }

      "several mandatory and optional tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addMandatoryUpToDateTable("b")
        builder.addMandatoryUpToDateTable("c")
        builder.addOptionalOutdatedTable("d")
        builder.addOptionalUpToDateTable("e")
        builder.addCheck("minusMonths(@infoDate, 1)", "")

        val validator = builder.getTestCase

        validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
      }
    }
    "fail construction" when {
      "date.from is not specified" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addCheck(dateFrom = "")

        val ex = intercept[IllegalArgumentException] {
          builder.getTestCase
        }

        assert(ex.getMessage.contains("Mandatory configuration options are missing: pramen.input.data.availability.check.1.date.from"))
      }
    }

    "fail validation" when {
      "An unsupported function is used" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addCheck("minusMonth(@infoDate)")
        val validator = builder.getTestCase

        val ex = intercept[SyntaxErrorException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Unsupported function 'minusMonth'"))
      }

      "one mandatory table" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryOutdatedTable("a")
        builder.addCheck()

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Outdated tables: a"))
      }

      "several mandatory tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("a")
        builder.addMandatoryOutdatedTable("b")
        builder.addMandatoryUpToDateTable("c")
        builder.addCheck("minusMonths(@infoDate, 1)", "")

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Outdated tables: b"))
      }

      "one optional table" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addOptionalOutdatedTable("a")
        builder.addCheck()

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Outdated tables: a"))
      }

      "several optional tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addOptionalOutdatedTable("a")
        builder.addOptionalOutdatedTable("b")
        builder.addOptionalOutdatedTable("c")
        builder.addCheck()

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Outdated tables: a, b, c"))
      }

      "several mandatory and optional tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryOutdatedTable("a")
        builder.addMandatoryUpToDateTable("b")
        builder.addMandatoryUpToDateTable("c")
        builder.addOptionalOutdatedTable("d")
        builder.addOptionalOutdatedTable("e")
        builder.addOptionalOutdatedTable("f")
        builder.addCheck()

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage.contains("Outdated tables: a, d, e, f"))
      }

      "several mandatory and optional tables with at least one optional ok" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryOutdatedTable("a")
        builder.addMandatoryUpToDateTable("b")
        builder.addMandatoryUpToDateTable("c")
        builder.addOptionalUpToDateTable("d")
        builder.addOptionalOutdatedTable("e")
        builder.addOptionalOutdatedTable("f")
        builder.addCheck()

        val validator = builder.getTestCase

        val ex = intercept[IllegalStateException] {
          validator.validateTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd)
        }

        assert(ex.getMessage == "Outdated tables: a")
      }
    }
  }

  "decideRunTask()" should {
    "return RunDecision.RunNew" when {
      "there are no input tables for a job and the job hasn't been ran yet" in {
        val builder = new DataValidationTestCaseBuilder
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.RunNew)
        assert(summary2.decision == RunDecision.RunNew)
      }

      "there are input tables for a job and the job hasn't been ran yet" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("A")
        builder.addMandatoryUpToDateTable("B")
        builder.addOptionalUpToDateTable("C")
        builder.addCheck()
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.RunNew)
        assert(summary2.decision == RunDecision.RunNew)
      }

    }

    "return RunDecision.SkipNodata" when {
      "there is no data in input tables" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryOutdatedTable("A")
        builder.addMandatoryUpToDateTable("B")
        builder.addOptionalOutdatedTable("C")
        builder.addCheck()
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.SkipNoData)
        assert(summary1.updatedTables.isEmpty)
        assert(summary1.noDataTables == Seq("A", "C"))
        assert(summary2.decision == RunDecision.SkipNoData)
        assert(summary2.updatedTables.isEmpty)
        assert(summary2.noDataTables == Seq("A", "C"))
      }
    }

    "return RunDecision.SkipUpToDate" when {
      "there are no input tables for a job, but the job has been ran already" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addOutputTableUpToDate("Z")
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.SkipUpToDate)
        assert(summary2.decision == RunDecision.RunUpdates)
      }

      "all input tables are up to date" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("A")
        builder.addMandatoryUpToDateTable("B")
        builder.addOptionalUpToDateTable("C")
        builder.addCheck()
        builder.addOutputTableUpToDate("Z")
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.SkipUpToDate)
        assert(summary2.decision == RunDecision.RunUpdates)
      }
    }

    "return RunDecision.RunUpdates" when {
      "one of the tables is not up to date" in {
        val builder = new DataValidationTestCaseBuilder
        builder.addMandatoryUpToDateTable("A")
        builder.addMandatoryUpToDateTable("B")
        builder.addOptionalUpToDateTable("C")
        builder.addCheck()
        builder.addOutputTableOutDated("Z")
        val validator = builder.getTestCase

        val summary1 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", false)
        val summary2 = validator.decideRunTask(builder.getInfoDateBegin, builder.getInfoDateEnd, builder.getInfoDateEnd, "Z", true)

        assert(summary1.decision == RunDecision.RunUpdates)
        assert(summary1.noDataTables.isEmpty)
        assert(summary1.updatedTables.map(_._1) == Seq("A", "B", "C"))
        assert(summary2.decision == RunDecision.RunUpdates)
        assert(summary2.noDataTables.isEmpty)
        assert(summary2.updatedTables.map(_._1) == Seq("A", "B", "C"))
      }
    }

  }
}
