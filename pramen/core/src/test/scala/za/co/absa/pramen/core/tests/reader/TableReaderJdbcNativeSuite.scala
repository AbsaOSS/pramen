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

package za.co.absa.pramen.core.tests.reader

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TextComparisonFixture}
import za.co.absa.pramen.core.reader.TableReaderJdbcNative
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.LocalDate

class TableReaderJdbcNativeSuite extends AnyWordSpec with RelationalDbFixture with SparkTestBase with TextComparisonFixture {
  private val tableName = RdbExampleTable.Company.tableName

  private val conf = ConfigFactory.parseString(
    s"""reader {
       |  jdbc {
       |    driver = "$driver"
       |    connection.string = "$url"
       |    user = "$user"
       |    password = "$password"
       |    autocommit = true
       |  }
       |
       |  has.information.date.column = true
       |
       |  information.date.column = "FOUNDED"
       |  information.date.type = "date"
       |  information.date.format = "YYYY-MM-dd"
       |  option.fetchsize = 1000
       |
       |}
       |reader_legacy {
       |  jdbc {
       |    driver = "$driver"
       |    connection.string = "$url"
       |    user = "$user"
       |    password = "$password"
       |  }
       |
       |  has.information.date.column = true
       |  jdbc.sanitize.datetime = false
       |
       |  information.date.column = "FOUNDED"
       |  information.date.type = "date"
       |  information.date.app.format = "yyyy-MM-DD"
       |
       |}
       |reader_minimal {
       |  jdbc {
       |    driver = "$driver"
       |    connection.string = "$url"
       |    user = "$user"
       |    password = "$password"
       |  }
       |
       |  has.information.date.column = false
       |}
       |reader_limit {
       |  jdbc {
       |    driver = "$driver"
       |    connection.string = "$url"
       |    user = "$user"
       |    password = "$password"
       |  }
       |
       |  has.information.date.column = false
       |  limit.records = 100
       |}""".stripMargin)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  private def getReader: TableReaderJdbcNative =
    TableReaderJdbcNative(conf.getConfig("reader"), "reader")

  "TableReaderJdbcNative factory" should {
    "construct a reader object" in {
      val reader = getReader
      assert(reader != null)
      assert(reader.getJdbcReaderConfig.infoDateFormat == "YYYY-MM-dd")
      assert(reader.getJdbcReaderConfig.jdbcConfig.autoCommit)
      assert(reader.getJdbcReaderConfig.jdbcConfig.fetchSize.get == 1000)
    }

    "work with legacy config" in {
      val reader = TableReaderJdbcNative(conf.getConfig("reader_legacy"), "reader_legacy")
      assert(reader.getJdbcReaderConfig.infoDateFormat == "yyyy-MM-DD")
      assert(!reader.getJdbcReaderConfig.jdbcConfig.sanitizeDateTime)
    }

    "work with minimal config" in {
      val reader = TableReaderJdbcNative(conf.getConfig("reader_minimal"), "reader_minimal")
      assert(reader.getJdbcReaderConfig.infoDateFormat == "yyyy-MM-dd")
      assert(reader.getJdbcReaderConfig.jdbcConfig.sanitizeDateTime)
      assert(!reader.getJdbcReaderConfig.jdbcConfig.autoCommit)
      assert(reader.getJdbcReaderConfig.jdbcConfig.fetchSize.isEmpty)
    }

    "throw an exception if config is missing" in {
      intercept[IllegalArgumentException] {
        TableReaderJdbcNative(conf)
      }
    }
  }

  "getJdbcConfig()" should {
    "return the proper config" in {
      val reader = getReader

      val jdbcConfig = reader.getJdbcReaderConfig.jdbcConfig

      assert(jdbcConfig.driver == driver)
      assert(jdbcConfig.primaryUrl.get == url)
      assert(jdbcConfig.user.contains(user))
      assert(jdbcConfig.user.contains(password))
      assert(jdbcConfig.database.isEmpty)
    }
  }

  "getRecordCount()" should {
    "return the actual count for a single day" in {
      val reader = getReader

      val count = reader.getRecordCount(Query.Sql(s"SELECT * FROM $tableName WHERE founded = '@infoDateBegin'"),
        LocalDate.parse("2005-03-29"),
        LocalDate.parse("2005-03-29")
      )

      assert(count == 1)
    }

    "return the actual count for a single end day" in {
      val reader = getReader

      val count = reader.getRecordCount(Query.Sql(s"SELECT * FROM $tableName WHERE founded = '@date'"),
        LocalDate.parse("2005-03-29"),
        LocalDate.parse("2005-03-29"))

      assert(count == 1)
    }

    "return the actual count for a date range" in {
      val reader = getReader

      val count = reader.getRecordCount(
        Query.Sql(s"SELECT * FROM $tableName WHERE founded >= '@infoDateBegin' AND founded <= '@infoDateEnd'"),
        LocalDate.parse("2000-01-01"),
        LocalDate.parse("2017-12-31"))

      assert(count == 4)
    }
  }

  "getData()" should {
    "return the actual data for a single day" in {
      val expectedSchema =
      """{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "ID",
        |    "type" : "integer",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "NAME",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : {
        |      "maxLength" : 50
        |    }
        |  }, {
        |    "name" : "EMAIL",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : {
        |      "maxLength" : 50
        |    }
        |  }, {
        |    "name" : "FOUNDED",
        |    "type" : "date",
        |    "nullable" : true,
        |    "metadata" : { }
        |  } ]
        |}""".stripMargin

      val expectedData =
      """[ {
        |  "ID" : 2,
        |  "NAME" : "Company2",
        |  "EMAIL" : "company2@example.com",
        |  "FOUNDED" : "2005-03-29"
        |} ]""".stripMargin

      val reader = getReader

      val df = reader.getData(
        Query.Sql(s"SELECT id, name, email, founded FROM $tableName WHERE founded = '@infoDateBegin'"),
        LocalDate.parse("2005-03-29"),
        LocalDate.parse("2005-03-29"),
        Nil)

      val actualSchema = df.schema.prettyJson
      val actualData = SparkUtils.convertDataFrameToPrettyJSON(df)

      compareText(actualSchema, expectedSchema)
      compareText(actualData, expectedData)
    }

    "return the actual data for a date range" in {
      val expectedData =
      """[ {
        |  "ID" : 1,
        |  "NAME" : "Company1",
        |  "EMAIL" : "company1@example.com",
        |  "FOUNDED" : "2000-10-11"
        |}, {
        |  "ID" : 2,
        |  "NAME" : "Company2",
        |  "EMAIL" : "company2@example.com",
        |  "FOUNDED" : "2005-03-29"
        |}, {
        |  "ID" : 3,
        |  "NAME" : "Company3",
        |  "EMAIL" : "company3@example.com",
        |  "FOUNDED" : "2016-12-30"
        |}, {
        |  "ID" : 4,
        |  "NAME" : "Company4",
        |  "EMAIL" : "company4@example.com",
        |  "FOUNDED" : "2016-12-31"
        |} ]"""

      val reader = getReader

      val df = reader.getData(
        Query.Sql(s"SELECT id, name, email, founded FROM $tableName WHERE founded >= '@dateFrom' AND founded <= '@dateTo'"),
        LocalDate.parse("2000-01-01"),
        LocalDate.parse("2017-12-31"),
        Nil)

      val actualData = SparkUtils.convertDataFrameToPrettyJSON(df)

      assert(stripLineEndings(actualData) == stripLineEndings(expectedData))
    }
  }

  "getSqlExpression" should {
    "throw an exception if the query is not an SQL" in {
      val reader = getReader

      assertThrows[IllegalArgumentException] {
        reader.getSqlExpression(Query.Table("table1"))
      }
    }
  }

  "getSqlDataQuery" should {
    val infoDateBegin = LocalDate.parse("2022-02-18")
    val infoDateEnd = LocalDate.parse("2022-02-19")

    "return a query with info date if it is enabled" in {
      val reader = getReader

      val actual = reader.getSqlDataQuery("table1", infoDateBegin, infoDateEnd, Nil)

      assert(actual.startsWith("SELECT * FROM table1 WHERE FOUNDED >="))
    }

    "return a query without info date if it is disabled" in {
      val reader = TableReaderJdbcNative(conf.getConfig("reader_minimal"), "reader_minimal")

      val actual = reader.getSqlDataQuery("table1", infoDateBegin, infoDateEnd, Nil)

      assert(actual == "SELECT * FROM table1")
    }

    "return a query without with limits" in {
      val reader = TableReaderJdbcNative(conf.getConfig("reader_limit"), "reader_limit")

      val actual = reader.getSqlDataQuery("table1", infoDateBegin, infoDateEnd, Nil)

      assert(actual == "SELECT * FROM table1 LIMIT 100")
    }
  }
}
