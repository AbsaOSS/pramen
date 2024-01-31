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

}
