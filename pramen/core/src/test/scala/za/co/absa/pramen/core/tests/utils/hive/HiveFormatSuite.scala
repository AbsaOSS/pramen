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

package za.co.absa.pramen.core.tests.utils.hive

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.hive.HiveFormat

class HiveFormatSuite extends AnyWordSpec {
  "fromString()" should {
    "work with Parquet" in {
      val actual = HiveFormat.fromString("Parquet")

      assert(actual == HiveFormat.Parquet)
    }

    "work with Delta" in {
      val actual = HiveFormat.fromString("delta")

      assert(actual == HiveFormat.Delta)
    }

    "throw an exception with an unsupported format" in {
      assertThrows[IllegalArgumentException] {
        HiveFormat.fromString("unsupported")
      }
    }
  }
}
