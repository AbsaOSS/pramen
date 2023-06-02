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

class HiveDefaultConfigSuite extends AnyWordSpec {
  "fromConfig()" should {
    "return default config for parquet and delta" in {
      val conf = ConfigFactory.empty()

      val hiveDefaultConfig = HiveDefaultConfig.fromConfig(conf)

      assert(hiveDefaultConfig.hiveApi == HiveApi.Sql)
      assert(hiveDefaultConfig.database.isEmpty)
      assert(hiveDefaultConfig.jdbcConfig.isEmpty)
      assert(!hiveDefaultConfig.ignoreFailures)
      assert(hiveDefaultConfig.templates.nonEmpty)
      assert(hiveDefaultConfig.templates.contains("parquet"))
      assert(hiveDefaultConfig.templates.contains("delta"))
    }

    "return overridden config" in {
      val conf = ConfigFactory.parseString(
        """pramen.hive {
          |  api = spark_catalog
          |  database = mydb
          |
          |  table = my_hive_table
          |  ignore.failures = true
          |
          |  jdbc {
          |    driver = driver
          |    url = url
          |    user = user
          |    password = pass
          |  }
          |
          |  conf {
          |     parquet.create.table.template = "create1"
          |     parquet.repair.table.template = "repair1"
          |     parquet.drop.table.template = "drop1"
          |     delta.create.table.template = "create2"
          |     delta.repair.table.template = "repair2"
          |     delta.drop.table.template = "drop2"
          |  }
          |}
          |""".stripMargin)

      val hiveDefaultConfig = HiveDefaultConfig.fromConfig(conf.getConfig("pramen.hive"))

      assert(hiveDefaultConfig.hiveApi == HiveApi.SparkCatalog)
      assert(hiveDefaultConfig.database.contains("mydb"))
      assert(hiveDefaultConfig.jdbcConfig.map(_.driver).contains("driver"))
      assert(hiveDefaultConfig.ignoreFailures)
      assert(hiveDefaultConfig.templates.nonEmpty)
      assert(hiveDefaultConfig.templates("parquet").createTableTemplate.contains("create1"))
      assert(hiveDefaultConfig.templates("parquet").repairTableTemplate.contains("repair1"))
      assert(hiveDefaultConfig.templates("parquet").dropTableTemplate.contains("drop1"))
      assert(hiveDefaultConfig.templates("delta").createTableTemplate.contains("create2"))
      assert(hiveDefaultConfig.templates("delta").repairTableTemplate.contains("repair2"))
      assert(hiveDefaultConfig.templates("delta").dropTableTemplate.contains("drop2"))
    }
  }

  "getNullConfig()" should {
    "return the default Hive config" in {
      val hiveDefaultConfig = HiveDefaultConfig.getNullConfig

      assert(hiveDefaultConfig.database.isEmpty)
      assert(hiveDefaultConfig.jdbcConfig.isEmpty)
      assert(!hiveDefaultConfig.ignoreFailures)
      assert(hiveDefaultConfig.templates.isEmpty)
    }
  }
}
