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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.mocks.SqlGeneratorDummy
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.reader.{JdbcUrlSelector, TableReaderJdbc, TableReaderJdbcNative}
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql.SqlGeneratorHsqlDb
import za.co.absa.pramen.core.utils.SparkUtils.{COMMENT_METADATA_KEY, MAX_LENGTH_METADATA_KEY}

import java.time.LocalDate

class TableReaderJdbcSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with RelationalDbFixture {
  private val infoDate = LocalDate.parse("2022-02-18")

  override def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "TableReaderJdbc" should {
    val conf = ConfigFactory.parseString(
      s"""reader {
         |  jdbc {
         |    driver = "$driver"
         |    connection.string = "$url"
         |    user = "$user"
         |    password = "$password"
         |  }
         |
         |  has.information.date.column = false
         |
         |  information.date.column = "INFO_DATE"
         |
         |  offset.column {
         |    name = "ts"
         |    type = "datetime"
         |  }
         |
         |  identifier.quoting.policy = "never"
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
         |
         |  information.date.column = "INFO_DATE"
         |  information.date.type = "date"
         |  information.date.app.format = "YYYY-MM-dd"
         |  sql.generator.class = "za.co.absa.pramen.core.mocks.SqlGeneratorDummy"
         |}
         |reader_minimal {
         |  jdbc {
         |    driver = "$driver"
         |    connection.string = "$url"
         |    user = "$user"
         |    password = "$password"
         |  }
         |
         |  has.information.date.column = true
         |
         |  information.date.column = "INFO_DATE"
         |  information.date.type = "number"
         |}""".stripMargin)

    "be able to be constructed properly from config" in {
      val reader = TableReaderJdbc(conf.getConfig("reader"), conf.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.jdbcConfig.database.isEmpty)
      assert(jdbc.jdbcConfig.driver == driver)
      assert(jdbc.jdbcConfig.primaryUrl.get == url)
      assert(jdbc.jdbcConfig.user.contains(user))
      assert(jdbc.jdbcConfig.password.contains(password))
      assert(jdbc.offsetInfoOpt.nonEmpty)
      assert(jdbc.offsetInfoOpt.get.offsetColumn == "ts")
      assert(jdbc.offsetInfoOpt.get.offsetType.dataTypeString == "datetime")
      assert(jdbc.identifierQuotingPolicy == QuotingPolicy.Never)
      assert(jdbc.sqlGeneratorClass.isEmpty)
      assert(!jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
    }

    "be able to be constructed properly from legacy config" in {
      val reader = TableReaderJdbc(conf.getConfig("reader_legacy"), conf.getConfig("reader_legacy"), "reader_legacy")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.jdbcConfig.database.isEmpty)
      assert(jdbc.jdbcConfig.driver == driver)
      assert(jdbc.jdbcConfig.primaryUrl.get == url)
      assert(jdbc.jdbcConfig.user.contains(user))
      assert(jdbc.jdbcConfig.password.contains(password))
      assert(jdbc.infoDateColumn == "INFO_DATE")
      assert(jdbc.infoDateType == SqlColumnType.DATE)
      assert(jdbc.infoDateFormat == "YYYY-MM-dd")
      assert(jdbc.identifierQuotingPolicy == QuotingPolicy.Auto)
      assert(jdbc.sqlGeneratorClass.contains("za.co.absa.pramen.core.mocks.SqlGeneratorDummy"))
      assert(jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
    }

    "be able to be constructed properly from minimal config" in {
      val reader = TableReaderJdbc(conf.getConfig("reader_minimal"), conf.getConfig("reader_minimal"), "reader_minimal")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.jdbcConfig.database.isEmpty)
      assert(jdbc.jdbcConfig.driver == driver)
      assert(jdbc.jdbcConfig.primaryUrl.get == url)
      assert(jdbc.jdbcConfig.user.contains(user))
      assert(jdbc.jdbcConfig.password.contains(password))
      assert(jdbc.infoDateColumn == "INFO_DATE")
      assert(jdbc.infoDateType == SqlColumnType.NUMBER)
      assert(jdbc.infoDateFormat == "yyyy-MM-dd")
      assert(jdbc.offsetInfoOpt.isEmpty)
      assert(jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
      assert(jdbc.identifierQuotingPolicy == QuotingPolicy.Auto)
      assert(jdbc.sqlGeneratorClass.isEmpty)
    }

    "ensure sql query generator is properly selected 1" in {
      val reader = TableReaderJdbc(conf.getConfig("reader"), conf.getConfig("reader"), "reader")

      assert(reader.sqlGen.isInstanceOf[SqlGeneratorHsqlDb])
    }

    "ensure sql query generator is properly selected 2" in {
      val reader = TableReaderJdbc(conf.getConfig("reader_legacy"), conf.getConfig("reader_legacy"), "reader_legacy")

      assert(reader.sqlGen.isInstanceOf[SqlGeneratorDummy])
    }

    "ensure jdbc config properties are passed correctly" in {
      val testConfig = conf
        .withValue("reader.save.timestamps.as.dates", ConfigValueFactory.fromAnyRef(true))
        .withValue("reader.correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.saveTimestampsAsDates)
      assert(jdbc.correctDecimalsInSchema)
    }

    "ensure jdbc minimal snapshot configuration works" in {
      val testConfig = ConfigFactory.parseString(
        s"""reader {
           |  jdbc {
           |    driver = "$driver"
           |    connection.string = "$url"
           |    user = "$user"
           |    password = "$password"
           |  }
           |
           |  has.information.date.column = false
           |}""".stripMargin)
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(!jdbc.hasInfoDate)
    }

    "ensure jdbc minimal snapshot configuration works even if wrong format for the info date is specified" in {
      val testConfig = ConfigFactory.parseString(
        s"""reader {
           |  jdbc {
           |    driver = "$driver"
           |    connection.string = "$url"
           |    user = "$user"
           |    password = "$password"
           |  }
           |
           |  has.information.date.column = false
           |  information.date.column = "INFO_DATE"
           |  information.date.type = "wrong"
           |}""".stripMargin)
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(!jdbc.hasInfoDate)
    }

    "ensure isNative is true for the DBR driver" in {
      val testConfig = ConfigFactory.parseString(
        s"""reader {
           |  jdbc {
           |    driver = "com.databricks.client.jdbc.Driver"
           |    connection.string = "$url"
           |    user = "$user"
           |    password = "$password"
           |  }
           |
           |  use.jdbc.native = false
           |  has.information.date.column = false
           |  information.date.column = "INFO_DATE"
           |  information.date.type = "wrong"
           |}""".stripMargin)
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.useJdbcNative)
    }

    "ensure jdbc minimal event configuration works" in {
      val testConfig = ConfigFactory.parseString(
        s"""reader {
           |  jdbc {
           |    driver = "$driver"
           |    connection.string = "$url"
           |    user = "$user"
           |    password = "$password"
           |  }
           |
           |  has.information.date.column = true
           |  information.date.column = "sync_date"
           |  information.date.type = "date"
           |}""".stripMargin)
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.hasInfoDate)
      assert(jdbc.infoDateColumn == "sync_date")
      assert(jdbc.infoDateType == SqlColumnType.DATE)
    }

    "getWithRetry" should {
      "return the successful dataframe on the second try" in {
        val readerConfig = conf.getConfig("reader")
        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, readerConfig, "reader")

        val urlSelector = mock(classOf[JdbcUrlSelector])

        whenMock(urlSelector.getUrl)
          .thenThrow(new RuntimeException("dummy"))
          .thenReturn(url)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        reader.getWithRetry("company", isDataQuery = true, 2, None) { df =>
          assert(!df.isEmpty)
        }

        assert(jdbcTableReaderConfig.identifierQuotingPolicy == QuotingPolicy.Never)
      }

      "pass the exception when out of retries" in {
        val readerConfig = conf.getConfig("reader")
        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, readerConfig, "reader")

        val urlSelector = mock(classOf[JdbcUrlSelector])

        whenMock(urlSelector.getUrl)
          .thenThrow(new RuntimeException("dummy"))
          .thenThrow(new RuntimeException("dummy"))
          .thenReturn(url)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        val ex = intercept[RuntimeException] {
          reader.getWithRetry("company", isDataQuery = true, 2, None) { _ => }
        }

        assert(ex.getMessage.contains("dummy"))
      }
    }

    "getDataFrame" should {
      "support varchar metadata when enabled" in {
        val readerConfig = conf.getConfig("reader")
          .withValue("reader.save.timestamps.as.dates", ConfigValueFactory.fromAnyRef(true))
          .withValue("reader.correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))
          .withValue("enable.schema.metadata", ConfigValueFactory.fromAnyRef(true))

        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, readerConfig, "reader")
        val urlSelector = JdbcUrlSelector(jdbcTableReaderConfig.jdbcConfig)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        val df = reader.getDataFrame("SELECT * FROM company", isDataQuery = true, Option("company"))

        // NAME VARCHAR(50)
        assert(df.schema.fields(1).name == "NAME")
        assert(df.schema.fields(1).metadata.getLong(MAX_LENGTH_METADATA_KEY) == 50L)
        assert(df.schema.fields(1).metadata.getString(COMMENT_METADATA_KEY) == "This is company name")
        // DESCRIPTION VARCHAR
        assert(df.schema.fields(2).name == "DESCRIPTION")
        assert(!df.schema.fields(2).metadata.contains(MAX_LENGTH_METADATA_KEY))
      }
    }

    "getCount()" should {
      "return count for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val count = reader.getRecordCount(Query.Table("company"), null, null)

        assert(count == 4)
      }

      "return count for a sql snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val count = reader.getRecordCount(Query.Sql("SELECT * FROM company"), null, null)

        assert(count == 4)
      }

      "return count for a table event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val count = reader.getRecordCount(Query.Table("company"), infoDate,  infoDate)

        assert(count == 3)
      }

      "return count for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val count = reader.getRecordCount(Query.Sql("SELECT id FROM company"), infoDate, infoDate)

        assert(count == 4)
      }

      "return count for an event-like SQL query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val count = reader.getRecordCount(Query.Sql("SELECT id, info_date FROM company WHERE info_date BETWEEN '@dateFrom' AND '@dateTo'"), infoDate, infoDate)

        assert(count == 3)
      }
    }

    "getCountSqlQuery" should {
      "return a count query for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val sql = reader.getCountSqlQuery("SELECT * FROM COMPANY")

        assert(sql == "SELECT COUNT(*) FROM (SELECT * FROM COMPANY)")
      }

      "return a count query for a table event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val sqlTemplate = "SELECT * FROM COMPANY WHERE info_date BETWEEN '@dateFrom' AND '@dateTo'"
        val sqlIn = TableReaderJdbcNative.applyInfoDateExpressionToString(sqlTemplate, infoDate, infoDate)
        val sql = reader.getCountSqlQuery(sqlIn)

        assert(sql == "SELECT COUNT(*) FROM (SELECT * FROM COMPANY WHERE info_date BETWEEN '2022-02-18' AND '2022-02-18')")
      }

      "return a count query for a complex event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("jdbc.driver", ConfigValueFactory.fromAnyRef("net.sourceforge.jtds.jdbc.Driver"))
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val sqlTemplate = "SELECT * FROM my_db.my_table WHERE info_date = CAST(REPLACE(CAST(CAST('@infoDate' AS DATE) AS VARCHAR(10)), '-', '') AS INTEGER)"
        val sqlIn = TableReaderJdbcNative.applyInfoDateExpressionToString(sqlTemplate, infoDate, infoDate)
        val sql = reader.getCountSqlQuery(sqlIn)

        assert(sql == "SELECT COUNT_BIG(*) FROM (SELECT * FROM my_db.my_table WHERE info_date = CAST(REPLACE(CAST(CAST('2022-02-18' AS DATE) AS VARCHAR(10)), '-', '') AS INTEGER)) AS query")
      }
    }

    "getData()" should {
      "return data for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Table("company"), null, null, Seq.empty[String])

        assert(df.count() == 4)
        assert(df.schema.fields.length == 9)
      }

      "return selected column for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Table("company"), null, null, Seq("id", "name"))

        assert(df.count() == 4)
        assert(df.schema.fields.length == 2)
      }

      "return data for a table event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))
          .withValue("correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val df = reader.getData(Query.Table("company"), infoDate, infoDate, Seq.empty[String])

        assert(df.count() == 3)
      }

      "return data for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Sql("SELECT id FROM company"), infoDate, infoDate, Seq.empty[String])

        assert(df.count == 4)
        assert(df.schema.fields.length == 1)
      }

      "return selected columns for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Sql("SELECT * FROM company"), infoDate, infoDate, Seq("id", "name"))

        assert(df.count == 4)
        assert(df.schema.fields.length == 2)
      }

      "return data for an event-like SQL query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, testConfig, "reader")

        val df = reader.getData(Query.Sql("SELECT id, info_date FROM company WHERE info_date BETWEEN '@dateFrom' AND '@dateTo'"), infoDate, infoDate, Seq.empty[String])

        assert(df.count == 3)
      }
    }

    "getSqlConfig" should {
      "throw an exception on an incorrect info date column type" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("not_exist"))

        assertThrows[IllegalArgumentException] {
          TableReaderJdbc(testConfig, testConfig, "reader")
        }
      }
    }
  }
}
