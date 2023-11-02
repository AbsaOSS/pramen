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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.JdbcSparkUtils
import za.co.absa.pramen.core.utils.impl.JdbcFieldMetadata

class JdbcSparkUtilsSuite extends AnyWordSpec with SparkTestBase with RelationalDbFixture with BeforeAndAfterAll {
  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))

  override def beforeAll(): Unit = {
    super.beforeAll()

    RdbExampleTable.Company.initTable(getConnection)
  }

  "addMetadataFromJdbc" should {

  }

  "withJdbcMetadata" should {

  }

  "withResultSet" should {

  }

  "convertTimestampToDates" should {

  }

  "getCorrectedDecimalsSchema" should {

  }

  "getFieldMetadata" should {
    "convert sized varchar to the internal model" in {
      val expectedVarcharN = JdbcFieldMetadata(
        name = "NAME",
        label = "NAME",
        sqlType = java.sql.Types.VARCHAR,
        sqlTypeName = "VARCHAR",
        displaySize = 50,
        precision = 50,
        scale = 0,
        nullable = false
      )

      withQuery("SELECT name FROM COMPANY WHERE 0=1") { rs =>
        val actualVarcharN = JdbcSparkUtils.getFieldMetadata(rs.getMetaData, 1)

        assert(actualVarcharN == expectedVarcharN)
      }
    }

    "work with unsized data types" in {
      val expectedVarchar = JdbcFieldMetadata(
        name = "DESCRIPTION",
        label = "DESCRIPTION",
        sqlType = java.sql.Types.VARCHAR,
        sqlTypeName = "VARCHAR",
        displaySize = 32768,
        precision = 32768,
        scale = 0,
        nullable = false
      )

      withQuery("SELECT description FROM COMPANY WHERE 0=1") { rs =>
        val actualVarchar = JdbcSparkUtils.getFieldMetadata(rs.getMetaData, 1)

        assert(actualVarchar == expectedVarchar)
      }
    }
  }

  "getJdbcOptions" should {
    "combine all provided options to a single map" in {
      val jdbcConfig = JdbcConfig(
        primaryUrl = Option(url),
        user = Option("user"),
        password = Option("password1"),
        database = Option("mydb"),
        driver = "org.hsqldb.jdbc.JDBCDriver",
        extraOptions = Map("key1" -> "value1", "key2" -> "value2")
      )

      val extraOptions = Map("key1" -> "value11", "key3" -> "value3")

      val options = JdbcSparkUtils.getJdbcOptions(url, jdbcConfig, "my_table", extraOptions)

      assert(options.size == 9)
      assert(options("url") == url)
      assert(options("user") == "user")
      assert(options("password") == "password1")
      assert(options("dbtable") == "my_table")
      assert(options("database") == "mydb")
      assert(options("driver") == "org.hsqldb.jdbc.JDBCDriver")
      assert(options("key1") == "value11")
      assert(options("key2") == "value2")
      assert(options("key3") == "value3")
    }

    "work with minimum configuration" in {
      val jdbcConfig = JdbcConfig(
        driver = "org.hsqldb.jdbc.JDBCDriver",
        primaryUrl = None
      )

      val extraOptions = Map.empty[String, String]

      val options = JdbcSparkUtils.getJdbcOptions(url, jdbcConfig, "my_table", extraOptions)

      assert(options.size == 3)
      assert(options("url") == url)
      assert(options("dbtable") == "my_table")
      assert(options("driver") == "org.hsqldb.jdbc.JDBCDriver")
    }
  }
}
