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

package za.co.absa.pramen.core.bulkload

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.bulkload.BulkLoadDateUtils.getBulkLoadDates
import za.co.absa.pramen.bulkload.model.BulkLoadSize

import java.time.LocalDate

class BulkLoadDateUtilsSuite extends AnyWordSpec {
  "getBulkLoadDates" should {
    "monthly" when {
      "return proper date range for strict month dates" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-31"), BulkLoadSize.Monthly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-31"))
      }


      "return proper date range for multiple months" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-01"), LocalDate.parse("2026-09-30"), BulkLoadSize.Monthly)

        assert(dates.length == 2)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-31"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2026-09-30"))
      }

      "return proper date range for a days within a month" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-10"), BulkLoadSize.Monthly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-10"))
      }

      "return proper date range for a single day" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-05"), BulkLoadSize.Monthly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-05"))
      }

      "return proper date range for multiple non-strict months" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-10-10"), BulkLoadSize.Monthly)

        assert(dates.length == 3)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-31"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2026-09-30"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2026-09-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2026-09-30"))
        assert(dates(2).outputInfoDate == LocalDate.parse("2026-10-01"))
        assert(dates(2).dataDateFrom == LocalDate.parse("2026-10-01"))
        assert(dates(2).dataFateTo == LocalDate.parse("2026-10-10"))
      }

      "throw an exception if date range is incorrect" in {
        assertThrows[IllegalArgumentException] {
          getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-04"), BulkLoadSize.Monthly)
        }
      }
    }
    
    "quarterly" when {
      "return proper date range for strict quarter dates" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-07-01"), LocalDate.parse("2026-09-30"), BulkLoadSize.Quarterly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-07-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-07-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-09-30"))
      }

      "return proper date range for multiple quarters" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-07-01"), LocalDate.parse("2026-12-31"), BulkLoadSize.Quarterly)

        assert(dates.length == 2)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-07-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-07-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-09-30"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2026-10-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2026-10-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2026-12-31"))
      }

      "return proper date range for a days within a quarter" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-10"), BulkLoadSize.Quarterly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-10"))
      }

      "return proper date range for a single day" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-05"), BulkLoadSize.Quarterly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-05"))
      }

      "return proper date range for multiple non-strict quarters" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2027-02-10"), BulkLoadSize.Quarterly)

        assert(dates.length == 3)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-09-30"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2026-10-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2026-10-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2026-12-31"))
        assert(dates(2).outputInfoDate == LocalDate.parse("2027-01-01"))
        assert(dates(2).dataDateFrom == LocalDate.parse("2027-01-01"))
        assert(dates(2).dataFateTo == LocalDate.parse("2027-02-10"))
      }

      "throw an exception if date range is incorrect" in {
        assertThrows[IllegalArgumentException] {
          getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-04"), BulkLoadSize.Quarterly)
        }
      }
    }

    "yearly" when {
      "return proper date range for strict year dates" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-01-01"), LocalDate.parse("2026-12-31"), BulkLoadSize.Yearly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-01-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-01-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-12-31"))
      }

      "return proper date range for multiple years" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-01-01"), LocalDate.parse("2027-12-31"), BulkLoadSize.Yearly)

        assert(dates.length == 2)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-01-01"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-01-01"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-12-31"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2027-01-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2027-01-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2027-12-31"))
      }

      "return proper date range for a days within a year" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-10"), BulkLoadSize.Yearly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-10"))
      }

      "return proper date range for a single day" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-05"), BulkLoadSize.Yearly)

        assert(dates.length == 1)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-08-05"))
      }

      "return proper date range for multiple non-strict years" in {
        val dates = getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2028-02-10"), BulkLoadSize.Yearly)

        assert(dates.length == 3)
        assert(dates.head.outputInfoDate == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataDateFrom == LocalDate.parse("2026-08-05"))
        assert(dates.head.dataFateTo == LocalDate.parse("2026-12-31"))
        assert(dates(1).outputInfoDate == LocalDate.parse("2027-01-01"))
        assert(dates(1).dataDateFrom == LocalDate.parse("2027-01-01"))
        assert(dates(1).dataFateTo == LocalDate.parse("2027-12-31"))
        assert(dates(2).outputInfoDate == LocalDate.parse("2028-01-01"))
        assert(dates(2).dataDateFrom == LocalDate.parse("2028-01-01"))
        assert(dates(2).dataFateTo == LocalDate.parse("2028-02-10"))
      }

      "throw an exception if date range is incorrect" in {
        assertThrows[IllegalArgumentException] {
          getBulkLoadDates(LocalDate.parse("2026-08-05"), LocalDate.parse("2026-08-04"), BulkLoadSize.Yearly)
        }
      }
    }
  }

}
