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

import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.hive.QueryExecutorJdbc

import java.sql.SQLSyntaxErrorException

class QueryExecutorJdbcSuite extends AnyWordSpec with BeforeAndAfterAll with RelationalDbFixture {
  private val jdbcConfig = JdbcConfig(
    driver = driver,
    primaryUrl = Option(url),
    user = Option(user),
    password = Option(password)
  )

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
      val qe = QueryExecutorJdbc.fromJdbcConfig(jdbcConfig, optimizedExistQuery = false)

      qe.execute("UPDATE company SET id = 200 WHERE id = 100")
      qe.close()
    }

    "execute JDBC queries" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = true)

      qe.execute("SELECT * FROM company")
      qe.close()
    }

    "execute CREATE TABLE queries" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = false)

      qe.execute("CREATE TABLE my_table (id INT)")

      val exist = qe.doesTableExist(None, "my_table")

      assert(exist)

      qe.close()
    }

    "throw an exception on errors" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = false)

      val ex = intercept[SQLSyntaxErrorException] {
        qe.execute("SELECT * FROM does_not_exist")
      }

      assert(ex.getMessage.contains("object not found"))
    }

    "return true if the table is found" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = false)

      val exist = qe.doesTableExist(None, "company")

      assert(exist)

      qe.close()
    }

    "return false if the table is not found" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = false)

      val exist = qe.doesTableExist(Option(database), "does_not_exist")

      assert(!exist)

      qe.close()
    }

    "return false if the table is not found in an optimized query" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = true)

      val exist = qe.doesTableExist(Option(database), "does_not_exist")

      assert(!exist)

      qe.close()
    }

    "return false if the table is not found in an optimized query without a database" in {
      val qe = new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), optimizedExistQuery = true)

      val exist = qe.doesTableExist(None, "does_not_exist")

      assert(!exist)

      qe.close()
    }

    "handle retries" in {
      val baseSelector = JdbcUrlSelector(jdbcConfig)
      val (conn, _) = baseSelector.getWorkingConnection(1)
      val sel = mock(classOf[JdbcUrlSelector])

      whenMock(sel.jdbcConfig).thenReturn(jdbcConfig)
      whenMock(sel.getWorkingConnection(anyInt())).thenReturn((conn, "dummyurl"))

      val qe = new QueryExecutorJdbc(sel, true)
      qe.execute("SELECT * FROM company")

      var execution = 0
      var actionExecuted = false
      qe.executeActionOnConnection { conn =>
        execution += 1
        if (execution == 1) {
          throw new RuntimeException("fail the first time")
        }
        actionExecuted = true
        assert(conn != null)
        true
      }

      qe.close()

      assert(execution == 2)
      assert(actionExecuted)
    }

    "fail if retry fails" in {
      val baseSelector = JdbcUrlSelector(jdbcConfig)
      val (conn, _) = baseSelector.getWorkingConnection(1)
      val sel = mock(classOf[JdbcUrlSelector])

      whenMock(sel.jdbcConfig).thenReturn(jdbcConfig)
      whenMock(sel.getWorkingConnection(anyInt()))
        .thenReturn((conn, "dummyurl"))
        .thenThrow(new RuntimeException("fail the second time"))

      val qe = new QueryExecutorJdbc(sel, true)

      var execution = 0
      var actionExecuted = false

      val ex = intercept[RuntimeException] {
        qe.executeActionOnConnection { conn =>
          execution += 1
          if (execution == 1) {
            throw new RuntimeException("fail the first time")
          }
          actionExecuted = true
          assert(conn != null)
          true
        }
      }

      qe.close()

      assert(ex.getMessage.contains("fail the second time"))
      assert(execution == 1)
      assert(!actionExecuted)
    }

    "fail the first time when a connection selector can't select a connection" in {
      val baseSelector = JdbcUrlSelector(jdbcConfig)
      val (conn, _) = baseSelector.getWorkingConnection(1)
      val sel = mock(classOf[JdbcUrlSelector])

      whenMock(sel.jdbcConfig).thenReturn(jdbcConfig)
      whenMock(sel.getWorkingConnection(anyInt()))
        .thenThrow(new RuntimeException("fail the first time"))
        .thenThrow(new RuntimeException("fail the second time"))
        .thenReturn((conn, "dummyurl"))

      val qe = new QueryExecutorJdbc(sel, true)

      var execution = 0
      var actionExecuted = false

      val ex = intercept[RuntimeException] {
        qe.executeActionOnConnection { conn =>
          execution += 1
          actionExecuted = true
          assert(conn != null)
          true
        }
      }

      qe.close()

      assert(ex.getMessage.contains("fail the first time"))
      assert(execution == 0)
      assert(!actionExecuted)
    }
  }
}
