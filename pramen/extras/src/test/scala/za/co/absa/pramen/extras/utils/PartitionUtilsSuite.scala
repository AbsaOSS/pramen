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

package za.co.absa.pramen.extras.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.utils.PartitionUtils._

import java.time.LocalDate

class PartitionUtilsSuite extends AnyWordSpec {
  private val infoDate = LocalDate.of(2021, 12, 29)

  "unpackCustomPartitionPattern" should {
    "leave original string if no substitutions" in {
      val expected = "ABC abc 123"

      val actual = unpackCustomPartitionPattern("ABC abc 123", "info_date", infoDate, 1)

      assert(actual == expected)
    }

    "support the default pattern" in {
      val expected = "info_date=2021-12-29"

      val actual = unpackCustomPartitionPattern("{column}={year}-{month}-{day}", "info_date", infoDate, 1)

      assert(actual == expected)
    }

    "support Enceladus pattern" in {
      val expected = "2021/12/29/v1"

      val actual = unpackCustomPartitionPattern("{year}/{month}/{day}/v1", "info_date", infoDate, 1)

      assert(actual == expected)
    }

    "support leading zeros in the Enceladus pattern" in {
      val expected = "2022/02/06/v1"

      val actual = unpackCustomPartitionPattern("{year}/{month}/{day}/v1", "info_date", LocalDate.of(2022, 2, 6), 1)

      assert(actual == expected)
    }

    "support info version" in {
      val expected = "2022/02/06/v2"

      val actual = unpackCustomPartitionPattern("{year}/{month}/{day}/v{version}", "info_date", LocalDate.of(2022, 2, 6), 2)

      assert(actual == expected)
    }
  }

}
