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

package za.co.absa.pramen.core.tests.sql

import org.scalatest.wordspec.AnyWordSpec

class SqlColumnTypeSuite extends AnyWordSpec {
  import za.co.absa.pramen.api.sql.SqlColumnType._

  "fromString()" should {
    "return corresponding type" in {
      assert(fromString("date").contains(DATE))
      assert(fromString("datetime").contains(DATETIME))
      assert(fromString("string").contains(STRING))
      assert(fromString("number").contains(NUMBER))
    }

    "support mixed case values" in {
      assert(fromString("Date").contains(DATE))
      assert(fromString("DateTimE").contains(DATETIME))
      assert(fromString("STRING").contains(STRING))
      assert(fromString("nUmbeR").contains(NUMBER))
    }

    "return None for unknown type" in {
      assert(fromString("Hello").isEmpty)
    }
  }

  "fromStringStrict" should {
    "return corresponding type" in {
      assert(fromStringStrict("date") == DATE)
      assert(fromStringStrict("datetime") == DATETIME)
      assert(fromStringStrict("string") == STRING)
      assert(fromStringStrict("number") == NUMBER)
    }

    "support mixed case values" in {
      assert(fromStringStrict("Date") == DATE)
      assert(fromStringStrict("DateTimE") == DATETIME)
      assert(fromStringStrict("STRING") == STRING)
      assert(fromStringStrict("nUmbeR") == NUMBER)
    }

    "throw IllegalArgumentException for unknown type with parent" in {
      val ex = intercept[IllegalArgumentException] {
        fromStringStrict("Hello", "parent")
      }

      assert(ex.getMessage.contains("Unknown information type 'Hello' configured at parent. Allowed valued: date, datetime, string, number."))
    }

    "throw IllegalArgumentException for unknown type without parent" in {
      val ex = intercept[IllegalArgumentException] {
        fromStringStrict("Hello")
      }

      assert(ex.getMessage.contains("Unknown information type 'Hello'. Allowed valued: date, datetime, string, number."))
    }

  }

}
