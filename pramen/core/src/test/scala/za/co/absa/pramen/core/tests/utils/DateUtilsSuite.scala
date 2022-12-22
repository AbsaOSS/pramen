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

package za.co.absa.pramen.core.tests.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.DateUtils._

import java.time.{LocalDate, ZoneId}

class DateUtilsSuite extends AnyWordSpec {

  "convertStrToDate()" should {
    "Use the first pattern if it is parsable" in {
      assert(convertStrToDate("2020-10-30", "yyyy-MM-dd", "dummy") == LocalDate.of(2020, 10, 30))
    }

    "Use the second pattern if it is parsable" in {
      assert(convertStrToDate("20201030", "yyyy-MM-dd", "yyyyMMdd") == LocalDate.of(2020, 10, 30))
    }

    "Fail if both patterns are not suitable" in {
      val ex = intercept[IllegalArgumentException] {
        convertStrToDate("20201030", "yyyy-MM-dd", "yyyy/MMd/dd")
      }

      assert(ex.getMessage.contains(s"Cannot parse '20201030' in one of 'yyyy-MM-dd', 'yyyy/MMd/dd'"))
    }

    "Fail if no patterns are specified" in {
      val ex = intercept[IllegalArgumentException] {
        convertStrToDate("20201030")
      }

      assert(ex.getMessage.contains(s"No patterns provided to parse date '20201030'"))
    }
  }

  "fromDateToTimestampMs" should {
    "return the timestamp in milliseconds" in {
      val date = LocalDate.of(2020, 10, 30)
      assert(fromDateToTimestampMs(date, ZoneId.of("UTC")) == 1604016000000L)
    }
  }

}
