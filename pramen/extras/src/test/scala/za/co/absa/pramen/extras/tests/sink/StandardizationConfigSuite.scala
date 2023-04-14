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

package za.co.absa.pramen.extras.tests.sink

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.sink.StandardizationConfig

import java.time.ZoneId

class StandardizationConfigSuite extends AnyWordSpec with SparkTestBase {
  "fromConfig" should {
    "construct a config from the minimal settings" in {
      val conf = ConfigFactory.parseString(
        s"""""".stripMargin)

      val publishConfig = StandardizationConfig.fromConfig(conf)

      assert(publishConfig.timezoneId == ZoneId.systemDefault())
      assert(publishConfig.rawPartitionPattern == "{year}/{month}/{day}/v{version}")
      assert(publishConfig.publishPartitionPattern == "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}")
      assert(publishConfig.recordsPerPartition.isEmpty)
      assert(publishConfig.generateInfoFile)
      assert(publishConfig.hiveDatabase.isEmpty)
    }

    "construct a config from the provided settings" in {
      val conf = ConfigFactory.parseString(
        s"""raw.partition.pattern = "ABC"
           |publish.partition.pattern = "DEF"
           |
           |records.per.partition = 100
           |
           |hive.database = "mydb"
           |
           |info.file {
           |  generate = false
           |  source.application = "App1"
           |}
           |""".stripMargin)

      val publishConfig = StandardizationConfig.fromConfig(conf)

      assert(publishConfig.timezoneId == ZoneId.systemDefault())
      assert(publishConfig.rawPartitionPattern == "ABC")
      assert(publishConfig.publishPartitionPattern == "DEF")
      assert(publishConfig.recordsPerPartition.contains(100))
      assert(!publishConfig.generateInfoFile)
      assert(publishConfig.hiveDatabase.contains("mydb"))
    }

    "construct a config with Hive JDBC connection settings" in {
      val conf = ConfigFactory.parseString(
        s"""raw.partition.pattern = "Dummy1"
           |publish.partition.pattern = "Dummy2"
           |
           |hive.jdbc {
           |   driver = "org.postgresql.Driver"
           |   url = "jdbc:postgresql://dummyhost:5432/dummy_db"
           |   user = "dummy_user"
           |   password = "dummy_password"
           |}
           |
           |hive.database = "dummy_db"
           |
           |timezone = "UTC"
           |""".stripMargin)

      val publishConfig = StandardizationConfig.fromConfig(conf)

      assert(publishConfig.timezoneId == ZoneId.of("UTC"))
      assert(publishConfig.hiveDatabase.contains("dummy_db"))
      assert(publishConfig.hiveJdbcConfig.nonEmpty)
      assert(publishConfig.hiveJdbcConfig.exists(_.driver == "org.postgresql.Driver"))
      assert(publishConfig.hiveJdbcConfig.exists(_.primaryUrl.contains("jdbc:postgresql://dummyhost:5432/dummy_db")))
      assert(publishConfig.hiveJdbcConfig.exists(_.user.get == "dummy_user"))
      assert(publishConfig.hiveJdbcConfig.exists(_.password.get == "dummy_password"))
    }
  }
}
