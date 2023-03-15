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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.hive.QueryExecutorJdbc

import java.sql.SQLSyntaxErrorException

class QueryExecutorJdbcSuite extends AnyWordSpec with BeforeAndAfterAll with RelationalDbFixture  {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "QueryExecutorJdbc" should {
    "be constructed from JdbcConfig" in {
      val jdbcConfig = JdbcConfig(
        driver = driver,
        primaryUrl = Option(url),
        user = user,
        password = password
      )

      val qe = QueryExecutorJdbc.fromJdbcConfig(jdbcConfig)

      qe.execute("UPDATE company SET id = 200 WHERE id = 100")
      qe.close()
    }

    "execute JDBC queries" in {
      val connection = getConnection

      val qe = new QueryExecutorJdbc(connection)

      qe.execute("SELECT * FROM company")
      qe.close()
    }

    "execute CREATE TABLE queries" in {
      val connection = getConnection

      val qe = new QueryExecutorJdbc(connection)

      qe.execute("CREATE TABLE my_table (id INT)")

      val exist = qe.doesTableExist(database, "my_table")

      assert(exist)

      qe.close()
    }

    "throw an exception on errors" in {
      val qe = new QueryExecutorJdbc(getConnection)

      val ex = intercept[SQLSyntaxErrorException] {
        qe.execute("SELECT * FROM does_not_exist")
      }

      assert(ex.getMessage.contains("object not found"))
    }

    "return true if the table is found" in {
      val qe = new QueryExecutorJdbc(getConnection)

      val exist = qe.doesTableExist(database, "company")

      assert(exist)

      qe.close()
    }

    "return false if the table is not found" in {
      val qe = new QueryExecutorJdbc(getConnection)

      val exist = qe.doesTableExist(database, "does_not_exist")

      assert(!exist)

      qe.close()
    }
  }
}
