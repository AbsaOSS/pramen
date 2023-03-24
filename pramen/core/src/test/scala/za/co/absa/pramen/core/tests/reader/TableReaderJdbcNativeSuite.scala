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

import java.time.LocalDate
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.TableReaderJdbcNative
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.SparkUtils

class TableReaderJdbcNativeSuite extends AnyWordSpec with RelationalDbFixture with SparkTestBase {
  private val tableName = RdbExampleTable.Company.tableName

  private val conf = ConfigFactory.parseString(
    s"""reader {
       |  jdbc {
       |    driver = "$driver"
       |    connection.string = "$url"
       |    user = "$user"
       |    password = "$password"
       |  }
       |
       |  has.information.date.column = true
       |
       |  information.date.column = "FOUNDED"
       |  information.date.type = "date"
       |  information.date.app.format = "yyyy-MM-DD"
       |  information.date.sql.format = "YYYY-mm-DD"
       |
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
      assert(getReader != null)
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

      val jdbcConfig = reader.getJdbcConfig

      assert(jdbcConfig.driver == driver)
      assert(jdbcConfig.primaryUrl.get == url)
      assert(jdbcConfig.user == user)
      assert(jdbcConfig.password == password)
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

      assert(count == 3)
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
        |    "metadata" : { }
        |  }, {
        |    "name" : "EMAIL",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "FOUNDED",
        |    "type" : "date",
        |    "nullable" : true,
        |    "metadata" : { }
        |  } ]
        |}"""

      val expectedData =
      """[ {
        |  "ID" : 2,
        |  "NAME" : "Company2",
        |  "EMAIL" : "company2@example.com",
        |  "FOUNDED" : "2005-03-29"
        |} ]"""

      val reader = getReader

      val df = reader.getData(
        Query.Sql(s"SELECT id, name, email, founded FROM $tableName WHERE founded = '@infoDateBegin'"),
        LocalDate.parse("2005-03-29"),
        LocalDate.parse("2005-03-29"),
        Nil)

      val actualSchema = df.schema.prettyJson
      val actualData = SparkUtils.convertDataFrameToPrettyJSON(df)

      assert(stripLineEndings(actualSchema) == stripLineEndings(expectedSchema))
      assert(stripLineEndings(actualData) == stripLineEndings(expectedData))

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
}
