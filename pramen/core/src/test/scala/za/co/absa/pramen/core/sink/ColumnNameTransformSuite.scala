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

package za.co.absa.pramen.core.sink

import org.scalatest.wordspec.AnyWordSpec

class ColumnNameTransformSuite extends AnyWordSpec {
  "fromString" should {
    "parse an empty string" in {
      assert(ColumnNameTransform.fromString("") == ColumnNameTransform.NoChange)
    }
    "parse NoChange" in {
      assert(ColumnNameTransform.fromString("no_change") == ColumnNameTransform.NoChange)
    }
    "parse MakeUpper" in {
      assert(ColumnNameTransform.fromString("make_upper") == ColumnNameTransform.MakeUpper)
    }
    "parse MakeLower" in {
      assert(ColumnNameTransform.fromString("make_lower") == ColumnNameTransform.MakeLower)
    }
    "throw an exception on malformed value" in {
      val ex = intercept[IllegalArgumentException] {
        ColumnNameTransform.fromString("dummy")
      }
      assert(ex.getMessage.contains("Unknown value of column name transform"))
    }
  }
}
