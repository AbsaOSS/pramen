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

package za.co.absa.pramen.core.model

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Query

class QueryBuilderSuite extends AnyWordSpec {
  case class TestCase(prefix: String, configStr: String, expected: Query)

  private val testCases = Seq(
    TestCase("", """sql = "SELECT * FROM table"""", Query.Sql("SELECT * FROM table")),
    TestCase("", """table = "table1"""", Query.Table("table1")),
    TestCase("", """path = "/some/path"""", Query.Path("/some/path")),
    TestCase("", """data.file.1 = "/some/data/file"""", Query.Custom(Map("data.file.1" -> "/some/data/file"))),
    TestCase("input", """input.sql = "SELECT * FROM table"""", Query.Sql("SELECT * FROM table")),
    TestCase("input", """input.table = table1""", Query.Table("table1")),
    TestCase("input", """input.path = /some/path""", Query.Path("/some/path")),
    TestCase("input", "input.data.file.1 = /some/data/file1\ninput.data.file.2 = /some/data/file2",
      Query.Custom(Map("data.file.1" -> "/some/data/file1", "data.file.2" -> "/some/data/file2")))
  )

  "QueryBuilder" should {
    testCases.foreach(testCase => {
      s"build a query from a config for '${testCase.configStr}'" in {
        val conf = ConfigFactory.parseString(testCase.configStr)

        val query = QueryBuilder.fromConfig(conf, testCase.prefix, "")

        assert(query == testCase.expected)
      }
    })

    "throw an exception when no query configuration is specified" in {
      val conf = ConfigFactory.parseString("")

      val exception = intercept[IllegalArgumentException] {
        QueryBuilder.fromConfig(conf, "", "")
      }

      assert(exception.getMessage == "No options are specified for the query. Usually, it is one of: 'sql', 'path', 'table', 'db.table'.")
    }

    "throw an exception when the prefix is empty" in {
      val conf = ConfigFactory.parseString("data = /tmp")

      val exception = intercept[IllegalArgumentException] {
        QueryBuilder.fromConfig(conf, "input", "")
      }

      assert(exception.getMessage == "No options are specified for the 'input' query. Usually, it is one of: 'input.sql', 'input.path', 'input.table', 'input.db.table'.")
    }

    "throw an exception when the prefix is empty and parent is specified" in {
      val conf = ConfigFactory.parseString("data = /tmp")

      val exception = intercept[IllegalArgumentException] {
        QueryBuilder.fromConfig(conf, "input", "my.parent")
      }

      assert(exception.getMessage == "No options are specified for the 'input' query. Usually, it is one of: 'input.sql', 'input.path', 'input.table', 'input.db.table' at my.parent.")
    }
  }
}
