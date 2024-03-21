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

package za.co.absa.pramen.core.source

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{Query, Source}
import za.co.absa.pramen.core.ExternalChannelFactoryReflect
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.pipeline.OperationSplitter.DISABLE_COUNT_QUERY
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig.USE_JDBC_NATIVE
import za.co.absa.pramen.core.reader.{TableReaderJdbc, TableReaderJdbcNative}
import za.co.absa.pramen.core.samples.RdbExampleTable

import java.time.LocalDate

class JdbcSourceSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with RelationalDbFixture {

  override def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
       |   sources = [
       |    {
       |      name = "jdbc1"
       |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
       |      jdbc {
       |        driver = "$driver"
       |        connection.string = "$url"
       |        user = "$user"
       |        password = "$password"
       |      }
       |
       |      save.timestamps.as.dates = true
       |      correct.decimals.in.schema = true
       |      correct.decimals.fix.precision = true
       |      enable.schema.metadata = true
       |
       |      has.information.date.column = true
       |      information.date.column = "INFO_DATE"
       |      information.date.type = "date"
       |      information.date.format = "yyyy-MM-dd"
       |      use.jdbc.native = true
       |    },
       |    {
       |      name = "jdbc2"
       |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
       |      jdbc {
       |        driver = "driver2"
       |        connection.string = "url2"
       |        user = "user2"
       |        password = "password2"
       |        option.database = "mydb"
       |      }
       |
       |      has.information.date.column = false
       |      limit.records = 100
       |      disable.count.query = true
       |      information.date.column = "INFO_DATE"
       |      information.date.type = "date"
       |      information.date.app.format = "yyyy-MM-DD"
       |      information.date.sql.format = "YYYY-mm-DD"
       |    }
       |  ]
       | }
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "the factory" should {
    "be able to create a proper Source job object" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)

      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]
      val (fetchedConfig, index) = ExternalChannelFactoryReflect.getConfigByName(conf, None, "pramen.sources", "jdbc1", "source")

      assert(src.jdbcReaderConfig.infoDateColumn == "INFO_DATE")
      assert(index == 0)
      assert(!fetchedConfig.hasPath(DISABLE_COUNT_QUERY))
    }

    "be able to get a source by its name" in {
      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "Jdbc2", "source").asInstanceOf[JdbcSource]
      val (fetchedConfig, index) = ExternalChannelFactoryReflect.getConfigByName(conf, None, "pramen.sources", "jdbc2", "source")

      assert(src.jdbcReaderConfig.limitRecords.contains(100))
      assert(src.jdbcReaderConfig.jdbcConfig.driver == "driver2")
      assert(src.jdbcReaderConfig.jdbcConfig.extraOptions("database") == "mydb")
      assert(index == 1)
      assert(fetchedConfig.hasPath(DISABLE_COUNT_QUERY))
    }

    "be able to get a source by its name from the manager" in {
      val src = SourceManager.getSourceByName("Jdbc2", conf, None).asInstanceOf[JdbcSource]

      assert(src.jdbcReaderConfig.limitRecords.contains(100))
      assert(src.jdbcReaderConfig.jdbcConfig.driver == "driver2")
      assert(src.jdbcReaderConfig.jdbcConfig.extraOptions("database") == "mydb")
    }

    "throw an exception if a source is not found" in {
      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "Dummy", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Unknown name of a data source: Dummy"))
    }

    "throw an exception when a source name is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
           |    },
           |    {
           |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
           |      has.information.date.column = false
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("A name is not configured for 2 source(s)"))
    }

    "throw an exception when a factory class is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "mysource1"
           |    },
           |    {
           |      name = "mysource2"
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Factory class is not configured for 2 source(s)"))
    }

    "throw an exception when there are duplicate source names" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "mysource1"
           |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
           |    },
           |    {
           |      name = "MYsource1"
           |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Duplicate source names: mysource1, MYsource1"))
    }

  }

  "hasInfoDateColumn" should {
    "return true if JDBC is configured with info date column" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]

      assert(src.hasInfoDateColumn(null))
    }

    "return false if JDBC is configured without info date column" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(1)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.1", "source").asInstanceOf[JdbcSource]

      assert(!src.hasInfoDateColumn(null))
    }
  }

  "getReader" should {
    "return proper JDBC table reader when a table is specified" in {
      val infoDate = LocalDate.parse("2022-02-18")
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]
      val query = Query.Table("company")

      val reader1 = src.getReader(query, isCountQuery = true)
      val reader2 = src.getReader(query, isCountQuery = false)

      val df = reader2.getData(query, infoDateBegin = infoDate, infoDateEnd = infoDate, Nil)
      val count = reader1.getRecordCount(query, infoDateBegin = infoDate, infoDateEnd = infoDate)

      assert(reader1.isInstanceOf[TableReaderJdbc])
      assert(reader2.isInstanceOf[TableReaderJdbcNative])
      assert(df.schema.fields(1).metadata.getLong("maxLength") == 50L)
      assert(count == 3)
    }

    "return JDBC Native table reader when a SQL query is specified" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]
      val query = Query.Sql("SELECT * FROM company")

      val reader = src.getReader(query, isCountQuery = false)
      val df = reader.getData(query, infoDateBegin = LocalDate.now(), infoDateEnd = LocalDate.now(), Nil)

      assert(reader.isInstanceOf[TableReaderJdbcNative])
      assert(df.schema.fields(1).metadata.getLong("maxLength") == 50L)
    }

    "return JDBC table reader when a SELECT query is specified and jdbc native is not enabled" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0).withoutPath(USE_JDBC_NATIVE)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]
      val query = Query.Sql("SELECT * FROM company")

      val reader = src.getReader(query, isCountQuery = false)
      val df = reader.getData(query, infoDateBegin = LocalDate.now(), infoDateEnd = LocalDate.now(), Nil)

      assert(reader.isInstanceOf[TableReaderJdbc])
      assert(df.schema.fields(1).metadata.getLong("maxLength") == 50L)
    }

    "return JDBC Native table reader when a non-SELECT query is specified and jdbc native is not enabled" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]
      val query = Query.Sql("EXECUTE SELECT * FROM company")

      val reader = src.getReader(query, isCountQuery = true)

      assert(reader.isInstanceOf[TableReaderJdbcNative])
    }

    "throw an exception on unknown query type" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)
      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source").asInstanceOf[JdbcSource]

      val ex =  intercept[IllegalArgumentException] {
        src.getReader(Query.Path("/dummy"), isCountQuery = false)
      }

      assert(ex.getMessage.contains("Unexpected 'path' spec for the JDBC reader. Only 'table' or 'sql' are supported. Config path: pramen.sources.0"))
    }
  }

}
