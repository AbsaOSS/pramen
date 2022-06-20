/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.source

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.api.v2.Source
import za.co.absa.pramen.framework.ExternalChannelFactory
import za.co.absa.pramen.framework.base.SparkTestBase

class JdbcSourceSuite extends WordSpec with SparkTestBase {
  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
       |   sources = [
       |    {
       |      name = "jdbc1"
       |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
       |      jdbc {
       |        driver = "driver1"
       |        connection.string = "url1"
       |        user = "user1"
       |        password = "password1"
       |      }
       |
       |      has.information.date.column = true
       |      information.date.column = "INFO_DATE"
       |      information.date.type = "date"
       |      information.date.app.format = "yyyy-MM-DD"
       |      information.date.sql.format = "YYYY-mm-DD"
       |    },
       |    {
       |      name = "jdbc2"
       |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
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

      val src = ExternalChannelFactory.fromConfig[Source](src1Config, "pramen.sources.0", "source").asInstanceOf[JdbcSource]

      assert(src.hasInfoDate)
      assert(src.jdbcReaderConfig.infoDateColumn == "INFO_DATE")
    }

    "be able to get a source by its name" in {
      val src = ExternalChannelFactory.fromConfigByName[Source](conf, None, "pramen.sources", "Jdbc2", "source").asInstanceOf[JdbcSource]

      assert(!src.hasInfoDate)
      assert(src.jdbcReaderConfig.limitRecords.contains(100))
      assert(src.jdbcReaderConfig.jdbcConfig.driver == "driver2")
      assert(src.jdbcReaderConfig.jdbcConfig.extraOptions("database") == "mydb")
    }

    "be able to get a source by its name from the manager" in {
      val src = SourceManager.getSourceByName("Jdbc2", conf, None).asInstanceOf[JdbcSource]

      assert(!src.hasInfoDate)
      assert(src.jdbcReaderConfig.limitRecords.contains(100))
      assert(src.jdbcReaderConfig.jdbcConfig.driver == "driver2")
      assert(src.jdbcReaderConfig.jdbcConfig.extraOptions("database") == "mydb")
    }

    "throw an exception if a source is not found" in {
      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactory.fromConfigByName[Source](conf, None, "pramen.sources", "Dummy", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Unknown name of a data source: Dummy"))
    }

    "throw an exception when a source name is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
           |    },
           |    {
           |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
           |      has.information.date.column = false
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactory.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
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
        ExternalChannelFactory.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
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
           |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
           |    },
           |    {
           |      name = "MYsource1"
           |      factory.class = "za.co.absa.pramen.framework.source.JdbcSource"
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactory.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Duplicate source names: mysource1, MYsource1"))
    }

  }

}
