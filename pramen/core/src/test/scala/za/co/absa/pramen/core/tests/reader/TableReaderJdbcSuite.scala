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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.reader.{JdbcUrlSelector, TableReaderJdbc}
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql.SqlGeneratorHsqlDb
import org.mockito.Mockito.{mock, when => whenMock}
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.utils.SparkUtils.MAX_LENGTH_METADATA_KEY

class TableReaderJdbcSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with RelationalDbFixture {
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
      assert(jdbc.infoDateFormatSql == "YYYY-mm-DD")
      assert(!jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
    }

    "ensure sql query generator is properly selected" in {
      val reader = TableReaderJdbc(conf.getConfig("reader"), "reader")

      assert(reader.sqlGen.isInstanceOf[SqlGeneratorHsqlDb])
    }

    "ensure jdbc config properties are passed correctly" in {
      val textConfig = conf
        .withValue("reader.save.timestamps.as.dates", ConfigValueFactory.fromAnyRef(true))
        .withValue("reader.correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))
      val reader = TableReaderJdbc(textConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.saveTimestampsAsDates)
      assert(jdbc.correctDecimalsInSchema)
    }

    "getTableName" should {

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
  }
}
