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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class BookkeeperConfigSuite extends AnyWordSpec {
  "BookkeeperConfig" should {
    "deserialize the config properly for disabled bookkeeping" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = false
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(!bookkeeperConfig.bookkeepingEnabled)
    }

    "deserialize the config properly for JDBC" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |
           |  bookkeeping.jdbc {
           |    driver = "org.postgresql.Driver"
           |
           |    url = "jdbc:postgresql://dummurl"
           |    user = "user"
           |    password = "12345"
           |  }
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(bookkeeperConfig.bookkeepingEnabled)
      assert(bookkeeperConfig.bookkeepingJdbcConfig.nonEmpty)
      assert(bookkeeperConfig.bookkeepingJdbcConfig.get.driver == "org.postgresql.Driver")
      assert(bookkeeperConfig.bookkeepingJdbcConfig.get.primaryUrl.contains("jdbc:postgresql://dummurl"))
      assert(bookkeeperConfig.bookkeepingJdbcConfig.get.user.get == "user")
      assert(bookkeeperConfig.bookkeepingJdbcConfig.get.password.get == "12345")
    }

    "deserialize the config properly for Hadoop" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |  bookkeeping.location = "hdfs://dummy_path"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(bookkeeperConfig.bookkeepingEnabled)
      assert(bookkeeperConfig.bookkeepingLocation.contains("hdfs://dummy_path"))
    }

    "deserialize the config properly for Hadoop Delta Path" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |  bookkeeping.location = "hdfs://dummy_path"
           |  bookkeeping.hadoop.format = "delta"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(bookkeeperConfig.bookkeepingEnabled)
      assert(bookkeeperConfig.bookkeepingLocation.contains("hdfs://dummy_path"))
      assert(bookkeeperConfig.bookkeepingHadoopFormat == HadoopFormat.Delta)
    }

    "deserialize the config properly for Hadoop Delta Table" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |  bookkeeping.hadoop.format = "delta"
           |  bookkeeping.delta.table.prefix = "tbl_"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(bookkeeperConfig.bookkeepingEnabled)
      assert(bookkeeperConfig.bookkeepingLocation.isEmpty)
      assert(bookkeeperConfig.bookkeepingHadoopFormat == HadoopFormat.Delta)
      assert(bookkeeperConfig.deltaTablePrefix.contains("tbl_"))
    }

    "deserialize the config properly for MongoDB" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |  bookkeeping.mongodb.connection.string = "mongodb://aaabbb"
           |  bookkeeping.mongodb.database = "mydb"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val bookkeeperConfig = BookkeeperConfig.fromConfig(config)

      assert(bookkeeperConfig.bookkeepingEnabled)
      assert(bookkeeperConfig.bookkeepingConnectionString.contains("mongodb://aaabbb"))
      assert(bookkeeperConfig.bookkeepingDbName.contains("mydb"))
    }

    "throw an exception when MongoDB is used by db is not defined" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |  bookkeeping.mongodb.connection.string = "mongodb://aaabbb"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[RuntimeException] {
        BookkeeperConfig.fromConfig(config)
      }

      assert(ex.getMessage.contains("Database name is not defined. Please, define"))
    }

    "throw an exception when no bookkeeping means are provided" in {
      val configStr =
        s"""pramen {
           |  bookkeeping.enabled = true
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[RuntimeException] {
        BookkeeperConfig.fromConfig(config)
      }

      assert(ex.getMessage.contains("One of the following should be defined"))
    }
  }

}