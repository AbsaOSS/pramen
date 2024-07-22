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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates

class HiveConfigSuite extends AnyWordSpec {
  "fromConfigWithDefaults()" should {
    "return the default config if not overridden" in {
      val conf = ConfigFactory.empty()

      val defaultConfig = HiveDefaultConfig(
        HiveApi.SparkCatalog,
        Some("mydb1"),
        Map("parquet" -> HiveQueryTemplates("create1", "repair1", "add_partition1", "drop1")),
        None,
        ignoreFailures = true,
        alwaysEscapeColumnNames = false,
        optimizeExistQuery = true)

      val hiveConfig = HiveConfig.fromConfigWithDefaults(conf, defaultConfig, DataFormat.Parquet("dummy", None))

      assert(hiveConfig.hiveApi == HiveApi.SparkCatalog)
      assert(hiveConfig.database.contains("mydb1"))
      assert(hiveConfig.jdbcConfig.isEmpty)
      assert(hiveConfig.ignoreFailures)
      assert(!hiveConfig.alwaysEscapeColumnNames)
      assert(hiveConfig.optimizeExistQuery)
      assert(hiveConfig.templates.createTableTemplate.contains("create1"))
      assert(hiveConfig.templates.repairTableTemplate.contains("repair1"))
      assert(hiveConfig.templates.addPartitionTemplate.contains("add_partition1"))
      assert(hiveConfig.templates.dropTableTemplate.contains("drop1"))
    }

    "return the overridden config" in {
      val conf = ConfigFactory.parseString(
        """api = spark_catalog
          |database = mydb2
          |
          |ignore.failures = true
          |escape.column.names = true
          |
          |jdbc {
          |  driver = driver2
          |  url = url2
          |  user = user2
          |  password = pass2
          |}
          |
          |conf {
          |   create.table.template = "create2"
          |   repair.table.template = "repair2"
          |   add.partition.template = "add_partition2"
          |   drop.table.template = "drop2"
          |   optimize.exist.query = false
          |}
          |""".stripMargin)

      val defaultConfig = HiveDefaultConfig(
        HiveApi.Sql,
        Some("mydb1"),
        Map("parquet" -> HiveQueryTemplates("create1", "repair1", "add_partition1", "drop1")),
        None,
        ignoreFailures = false,
        alwaysEscapeColumnNames = false,
        optimizeExistQuery = true)

      val hiveConfig = HiveConfig.fromConfigWithDefaults(conf, defaultConfig, DataFormat.Parquet("dummy", None))

      assert(hiveConfig.hiveApi == HiveApi.SparkCatalog)
      assert(hiveConfig.database.contains("mydb2"))
      assert(hiveConfig.jdbcConfig.nonEmpty)
      assert(hiveConfig.jdbcConfig.map(_.driver).contains("driver2"))
      assert(hiveConfig.ignoreFailures)
      assert(hiveConfig.alwaysEscapeColumnNames)
      assert(!hiveConfig.optimizeExistQuery)
      assert(hiveConfig.templates.createTableTemplate.contains("create2"))
      assert(hiveConfig.templates.repairTableTemplate.contains("repair2"))
      assert(hiveConfig.templates.addPartitionTemplate.contains("add_partition2"))
      assert(hiveConfig.templates.dropTableTemplate.contains("drop2"))
    }
  }

  "fromDefaults()" should {
    "return the default config" in {
      val defaultConfig = HiveDefaultConfig(
        HiveApi.Sql,
        Some("mydb"),
        Map("parquet" -> HiveQueryTemplates("create", "repair", "add_partition1", "drop")),
        None,
        ignoreFailures = true,
        alwaysEscapeColumnNames = true,
        optimizeExistQuery = true)

      val hiveConfig = HiveConfig.fromDefaults(defaultConfig, DataFormat.Parquet("dummy", None))

      assert(hiveConfig.hiveApi == HiveApi.Sql)
      assert(hiveConfig.database.contains("mydb"))
      assert(hiveConfig.jdbcConfig.isEmpty)
      assert(hiveConfig.ignoreFailures)
      assert(hiveConfig.alwaysEscapeColumnNames)
      assert(hiveConfig.optimizeExistQuery)
      assert(hiveConfig.templates.createTableTemplate.contains("create"))
      assert(hiveConfig.templates.repairTableTemplate.contains("repair"))
      assert(hiveConfig.templates.addPartitionTemplate.contains("add_partition1"))
      assert(hiveConfig.templates.dropTableTemplate.contains("drop"))
    }
  }

  "getNullConfig()" should {
    "return the default Hive config" in {
      val hiveDefaultConfig = HiveConfig.getNullConfig

      assert(hiveDefaultConfig.database.isEmpty)
      assert(hiveDefaultConfig.jdbcConfig.isEmpty)
      assert(!hiveDefaultConfig.ignoreFailures)
    }
  }
}
