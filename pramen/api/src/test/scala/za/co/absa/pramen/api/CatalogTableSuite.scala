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

package za.co.absa.pramen.api

import org.scalatest.wordspec.AnyWordSpec

class CatalogTableSuite extends AnyWordSpec {
  "fromFullTableName" should {
    "work with a table name without catalog and database" in {
      val actual = CatalogTable.fromFullTableName("table")

      assert(actual == CatalogTable(None, None, "table"))
    }

    "work with a table name with a database" in {
      val actual = CatalogTable.fromFullTableName("`database`.table")

      assert(actual == CatalogTable(None, Some("database"), "table"))
    }

    "work with a table name with a catalog and a database" in {
      val actual = CatalogTable.fromFullTableName("`catalog`.`database`.`table`")

      assert(actual == CatalogTable(Some("catalog"), Some("database"), "table"))
    }

    "throw an exception with too many components" in {
      assertThrows[IllegalArgumentException] {
        CatalogTable.fromFullTableName("catalog.database.table.test")
      }
    }
  }

  "fromComponents" should {
    "work with a table name without catalog and database" in {
      val actual = CatalogTable.fromComponents(None, None, "table")

      assert(actual == CatalogTable(None, None, "table"))
    }

    "work with a table name with a database" in {
      val actual = CatalogTable.fromComponents(None, Some("`database`"), "`table`")

      assert(actual == CatalogTable(None, Some("database"), "table"))
    }

    "work with a table name with a catalog and a database" in {
      val actual = CatalogTable.fromComponents(Some("`catalog`"), Some("database"), "table")

      assert(actual == CatalogTable(Some("catalog"), Some("database"), "table"))
    }

    "work with a table name with a catalog" in {
      val actual = CatalogTable.fromComponents(Some("catalog"), None, "table")

      assert(actual == CatalogTable(Some("catalog"), None, "table"))
    }
  }
}
