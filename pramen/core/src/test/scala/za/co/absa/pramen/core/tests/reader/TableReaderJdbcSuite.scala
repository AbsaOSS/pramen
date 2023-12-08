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
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.reader.{JdbcUrlSelector, TableReaderJdbc}
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql.SqlGeneratorHsqlDb
import za.co.absa.pramen.core.utils.SparkUtils.MAX_LENGTH_METADATA_KEY

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
         |  information.date.type = "number"
         |  information.date.app.format = "yyyy-MM-DD"
         |  information.date.sql.format = "YYYY-mm-DD"
         |
         |}""".stripMargin)

    "be able to be constructed properly from config" in {
      val reader = TableReaderJdbc(conf.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.jdbcConfig.database.isEmpty)
      assert(jdbc.jdbcConfig.driver == driver)
      assert(jdbc.jdbcConfig.primaryUrl.get == url)
      assert(jdbc.jdbcConfig.user.contains(user))
      assert(jdbc.jdbcConfig.password.contains(password))
      assert(jdbc.infoDateColumn == "INFO_DATE")
      assert(jdbc.infoDateType == "number")
      assert(jdbc.infoDateFormatApp == "yyyy-MM-DD")
      assert(!jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
    }

    "ensure sql query generator is properly selected" in {
      val reader = TableReaderJdbc(conf.getConfig("reader"), "reader")

      assert(reader.sqlGen.isInstanceOf[SqlGeneratorHsqlDb])
    }

    "ensure jdbc config properties are passed correctly" in {
      val testConfig = conf
        .withValue("reader.save.timestamps.as.dates", ConfigValueFactory.fromAnyRef(true))
        .withValue("reader.correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

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
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(!jdbc.hasInfoDate)
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
      val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.hasInfoDate)
      assert(jdbc.infoDateColumn == "sync_date")
      assert(jdbc.infoDateType == "date")
    }

    "getWithRetry" should {
      "return the successful dataframe on the second try" in {
        val readerConfig = conf.getConfig("reader")
        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, "reader")

        val urlSelector = mock(classOf[JdbcUrlSelector])

        whenMock(urlSelector.getUrl)
          .thenThrow(new RuntimeException("dummy"))
          .thenReturn(url)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        reader.getWithRetry("company", isDataQuery = true, 2) { df =>
          assert(!df.isEmpty)
        }
      }

      "pass the exception when out of retries" in {
        val readerConfig = conf.getConfig("reader")
        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, "reader")

        val urlSelector = mock(classOf[JdbcUrlSelector])

        whenMock(urlSelector.getUrl)
          .thenThrow(new RuntimeException("dummy"))
          .thenThrow(new RuntimeException("dummy"))
          .thenReturn(url)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        val ex = intercept[RuntimeException] {
          reader.getWithRetry("company", isDataQuery = true, 2) { _ => }
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

        val jdbcTableReaderConfig = TableReaderJdbcConfig.load(readerConfig, "reader")
        val urlSelector = JdbcUrlSelector(jdbcTableReaderConfig.jdbcConfig)

        val reader = new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, readerConfig)

        val df = reader.getDataFrame("SELECT * FROM company", isDataQuery = true)

        // NAME VARCHAR(50)
        assert(df.schema.fields(1).name == "NAME")
        assert(df.schema.fields(1).metadata.getLong(MAX_LENGTH_METADATA_KEY) == 50L)
        // DESCRIPTION VARCHAR
        assert(df.schema.fields(2).name == "DESCRIPTION")
        assert(!df.schema.fields(2).metadata.contains(MAX_LENGTH_METADATA_KEY))
      }
    }

    "getCount()" should {
      "return count for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val count = reader.getRecordCount(Query.Table("company"), null, null)

        assert(count == 4)
      }

      "return count for a table event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.app.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, "reader")

        val count = reader.getRecordCount(Query.Table("company"), infoDate,  infoDate)

        assert(count == 3)
      }

      "return count for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val count = reader.getRecordCount(Query.Sql("SELECT id FROM company"), infoDate, infoDate)

        assert(count == 4)
      }

      "return count for an event-like SQL query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.app.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, "reader")

        val count = reader.getRecordCount(Query.Sql("SELECT id, info_date FROM company WHERE info_date BETWEEN '@dateFrom' AND '@dateTo'"), infoDate, infoDate)

        assert(count == 3)
      }
    }

    "getData()" should {
      "return data for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Table("company"), null, null, Seq.empty[String])

        assert(df.count() == 4)
        assert(df.schema.fields.length == 7)
      }

      "return selected columnd for a table snapshot-like query" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Table("company"), null, null, Seq("id", "name"))

        assert(df.count() == 4)
        assert(df.schema.fields.length == 2)
      }

      "return data for a table event-like query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.app.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))
          .withValue("correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))

        val reader = TableReaderJdbc(testConfig, "reader")

        val df = reader.getData(Query.Table("company"), infoDate, infoDate, Seq.empty[String])

        assert(df.count() == 3)
      }

      "return data for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Sql("SELECT id FROM company"), infoDate, infoDate, Seq.empty[String])

        assert(df.count == 4)
        assert(df.schema.fields.length == 1)
      }

      "return selected columns for a snapshot-like SQL" in {
        val testConfig = conf
        val reader = TableReaderJdbc(testConfig.getConfig("reader"), "reader")

        val df = reader.getData(Query.Sql("SELECT * FROM company"), infoDate, infoDate, Seq("id", "name"))

        assert(df.count == 4)
        assert(df.schema.fields.length == 2)
      }

      "return data for an event-like SQL query" in {
        val testConfig = conf.getConfig("reader")
          .withValue("has.information.date.column", ConfigValueFactory.fromAnyRef(true))
          .withValue("information.date.column", ConfigValueFactory.fromAnyRef("info_date"))
          .withValue("information.date.type", ConfigValueFactory.fromAnyRef("string"))
          .withValue("information.date.app.format", ConfigValueFactory.fromAnyRef("yyyy-MM-dd"))

        val reader = TableReaderJdbc(testConfig, "reader")

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

        val reader = TableReaderJdbc(testConfig, "reader")

        assertThrows[IllegalArgumentException] {
          reader.getSqlConfig
        }
      }
    }
  }
}
