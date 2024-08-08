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
import org.apache.spark.sql.SaveMode
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat.Parquet
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates

import java.time.LocalDate

class MetaTableSuite extends AnyWordSpec {
  "fromConfig()" should {
    "be able to parse multiple tables" in {
      val conf = ConfigFactory.parseString(
        """pramen.information.date.column = "INFORMATION_DATE"
          |pramen.information.date.format = "yyyy-MM-dd"
          |pramen.information.date.start = "2020-01-31"
          |pramen.track.days = 2
          |pramen.hive.api = sql
          |pramen.hive.prefer.add.partition = false
          |
          |pramen.metastore.tables = [
          |  {
          |    name = table1
          |    format = parquet
          |    path = /a/b/c
          |    information.date.column = INFO_DATE
          |    information.date.format = dd-MM-yyyy
          |    hive.prefer.add.partition = true
          |  },
          |  {
          |    name = table2
          |    format = delta
          |    path = /c/d/e
          |  },
          |  {
          |    name = table3
          |    format = delta
          |    path = /a/b/c
          |  },
          |  {
          |    name = table4
          |    format = transient
          |  }
          |]
          |""".stripMargin)

      val infoDateConfig = InfoDateConfig.fromConfig(conf.withFallback(ConfigFactory.load()))

      val metaTables = MetaTable.fromConfig(conf, infoDateConfig, "pramen.metastore.tables")

      assert(metaTables.size == 4)
      assert(metaTables.head.name == "table1")
      assert(metaTables.head.format.name == "parquet")
      assert(metaTables.head.infoDateColumn == "INFO_DATE")
      assert(metaTables.head.infoDateFormat == "dd-MM-yyyy")
      assert(metaTables.head.trackDays == 2)
      assert(metaTables.head.infoDateStart == LocalDate.of(2020, 1, 31))
      assert(metaTables.head.hivePreferAddPartition)
      assert(metaTables(1).name == "table2")
      assert(metaTables(1).format.name == "delta")
      assert(metaTables(1).infoDateColumn == "INFORMATION_DATE")
      assert(metaTables(1).infoDateFormat == "yyyy-MM-dd")
      assert(metaTables(1).hiveConfig.hiveApi == HiveApi.Sql)
      assert(!metaTables(1).hivePreferAddPartition)
      assert(metaTables(2).name == "table3")
      assert(metaTables(2).format.name == "delta")
      assert(metaTables(2).infoDateColumn == "INFORMATION_DATE")
      assert(metaTables(2).infoDateFormat == "yyyy-MM-dd")
      assert(metaTables(3).name == "table4")
      assert(metaTables(3).format.name == "transient")
    }

    "create an empty metastore if the config key is missing" in {
      val conf = ConfigFactory.parseString(
        """pramen.information.date.column = "INFORMATION_DATE"
          |pramen.information.date.format = "yyyy-MM-dd"
          |pramen.information.date.start = "2020-01-31"
          |pramen.track.days = 2
          |pramen.hive.api = sql
          |pramen.hive.prefer.add.partition = true
          |""".stripMargin)

      val infoDateConfig = InfoDateConfig.fromConfig(conf.withFallback(ConfigFactory.load()))

      val metaTables = MetaTable.fromConfig(conf, infoDateConfig, "pramen.metastore.tables")

      assert(metaTables.isEmpty)
    }

    "throw an exception if there are multiple tables with the same name" in {
      val conf = ConfigFactory.parseString(
        """pramen.information.date.column = "INFORMATION_DATE"
          |pramen.information.date.format = "yyyy-MM-dd"
          |pramen.information.date.start = "2020-01-31"
          |pramen.track.days = 1
          |pramen.hive.api = sql
          |pramen.hive.prefer.add.partition = true
          |
          |pramen.metastore.tables = [
          |  {
          |    name = table1
          |    format = parquet
          |    path = /a/b/c
          |  },
          |  {
          |    name = table2
          |    format = delta
          |    path = /c/d/e
          |  },
          |  {
          |    name = TABLE1
          |    format = delta
          |    path = /a/b/c
          |  }
          |]
          |""".stripMargin)
      val infoDateConfig = InfoDateConfig.fromConfig(conf.withFallback(ConfigFactory.load()))

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfig(conf, infoDateConfig, "pramen.metastore.tables")
      }

      assert(ex.getMessage.contains("Duplicate table definitions in the metastore: TABLE1"))
    }
  }

  "fromConfigSingleEntity()" should {
    "load a metatable definition" in {
      val conf = ConfigFactory.parseString(
        """
          |name = my_table
          |format = delta
          |path = /a/b/c
          |records.per.partition = 100
          |spark.conf = {
          |  key1 = value1
          |}
          |save.mode = append
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig.getNullConfig

      val metaTable = MetaTable.fromConfigSingleEntity(conf, conf, "INFO_DATE", "dd-MM-yyyy", LocalDate.parse("2020-01-31"), 0, defaultHiveConfig, defaultPreferAddPartition = true)

      assert(metaTable.name == "my_table")
      assert(metaTable.format.name == "delta")
      assert(metaTable.hiveTable.isEmpty)
      assert(metaTable.hivePath.isEmpty)
      assert(metaTable.hiveConfig.hiveApi == HiveApi.Sql)
      assert(metaTable.hiveConfig.database.isEmpty)
      assert(metaTable.hiveConfig.jdbcConfig.isEmpty)
      assert(!metaTable.hiveConfig.ignoreFailures)
      assert(metaTable.hivePreferAddPartition)
      assert(metaTable.trackDays == 0)
      assert(metaTable.infoDateColumn == "INFO_DATE")
      assert(metaTable.infoDateFormat == "dd-MM-yyyy")
      assert(metaTable.infoDateStart.toString == "2020-01-31")
      assert(metaTable.sparkConfig("key1") == "value1")
      assert(metaTable.saveModeOpt.contains(SaveMode.Append))
    }

    "load a metatable definition with hive table defined" in {
      val conf = ConfigFactory.parseString(
        """
          |name = my_table
          |format = parquet
          |path = /a/b/c
          |hive.table = my_hive_table
          |hive.path = /d/e/f
          |information.date.column = INFORMATION_DATE
          |information.date.format = yyyy-MM-dd
          |hive.prefer.add.partition = false
          |read.option {
          |  some.option.a = test1
          |  some.option.b = 12
          |}
          |write.option {
          |  x = test2
          |  y = 101
          |}
          |spark.conf = {
          |  key1 = value1
          |}
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig(HiveApi.Sql,
        Some("mydb"),
        Map("parquet" -> HiveQueryTemplates("create", "repair", "add_partition", "drop")),
        Some(JdbcConfig("driver", Some("url"),
          user = Some("user"),
          password = Some("pass")
        )), ignoreFailures = true, alwaysEscapeColumnNames = false, optimizeExistQuery = true)

      val appConf = ConfigFactory.parseString("pramen.default.records.per.partition = 100")

      val metaTable = MetaTable.fromConfigSingleEntity(conf, appConf, "INFO_DATE", "dd-MM-yyyy", LocalDate.parse("2020-01-31"), 1, defaultHiveConfig, defaultPreferAddPartition = true)

      assert(metaTable.name == "my_table")
      assert(metaTable.hiveConfig.hiveApi == HiveApi.Sql)
      assert(metaTable.hiveConfig.database.contains("mydb"))
      assert(metaTable.hiveConfig.jdbcConfig.exists(_.driver == "driver"))
      assert(metaTable.hiveConfig.templates.createTableTemplate == "create")
      assert(metaTable.hiveConfig.templates.repairTableTemplate == "repair")
      assert(metaTable.hiveConfig.templates.addPartitionTemplate == "add_partition")
      assert(metaTable.hiveConfig.templates.dropTableTemplate == "drop")
      assert(metaTable.hiveConfig.ignoreFailures)
      assert(metaTable.format.name == "parquet")
      assert(metaTable.format.asInstanceOf[Parquet].recordsPerPartition.contains(100))
      assert(metaTable.hiveTable.contains("my_hive_table"))
      assert(metaTable.hivePath.contains("/d/e/f"))
      assert(!metaTable.hivePreferAddPartition)
      assert(metaTable.trackDays == 1)
      assert(metaTable.infoDateColumn == "INFORMATION_DATE")
      assert(metaTable.infoDateFormat == "yyyy-MM-dd")
      assert(metaTable.readOptions("some.option.a") == "test1")
      assert(metaTable.readOptions("some.option.b") == "12")
      assert(metaTable.writeOptions("x") == "test2")
      assert(metaTable.writeOptions("y") == "101")
      assert(metaTable.sparkConfig("key1") == "value1")
      assert(metaTable.saveModeOpt.isEmpty)
    }

    "load a metatable definition with hive overrides" in {
      val conf = ConfigFactory.parseString(
        """
          |name = my_table
          |format = parquet
          |path = /a/b/c
          |hive {
          |  api = spark_catalog
          |  database = mydb2
          |  table = my_hive_table
          |  ignore.failures = true
          |
          |  jdbc {
          |    driver = driver2
          |    url = url2
          |    user = user2
          |    password = pass2
          |  }
          |
          |  conf {
          |     create.table.template = "create2"
          |     repair.table.template = "repair2"
          |     add.partition.template = "add_partition2"
          |     drop.table.template = "drop2"
          |  }
          |}
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig(
        HiveApi.Sql,
        Some("mydb1"),
        Map("parquet" -> HiveQueryTemplates("create1", "repair1", "add_partition1", "drop1")),
        Some(JdbcConfig("driver1", Some("url1"),
          user = Some("user1"),
          password = Some("pass1")
        )), ignoreFailures = false, alwaysEscapeColumnNames = false, optimizeExistQuery = true)

      val appConf = ConfigFactory.parseString("pramen.default.records.per.partition = 100")

      val metaTable = MetaTable.fromConfigSingleEntity(conf, appConf, "INFO_DATE", "dd-MM-yyyy", LocalDate.parse("2020-01-31"), 1, defaultHiveConfig, defaultPreferAddPartition = true)

      assert(metaTable.name == "my_table")
      assert(metaTable.hiveConfig.hiveApi == HiveApi.SparkCatalog)
      assert(metaTable.hiveConfig.database.contains("mydb2"))
      assert(metaTable.hiveConfig.jdbcConfig.exists(_.driver == "driver2"))
      assert(metaTable.hiveConfig.templates.createTableTemplate == "create2")
      assert(metaTable.hiveConfig.templates.repairTableTemplate == "repair2")
      assert(metaTable.hiveConfig.templates.addPartitionTemplate == "add_partition2")
      assert(metaTable.hiveConfig.templates.dropTableTemplate == "drop2")
      assert(metaTable.hiveConfig.ignoreFailures)
      assert(metaTable.format.name == "parquet")
      assert(metaTable.hiveTable.contains("my_hive_table"))
    }

    "throw an exception when mandatory the option is missing" in {
      val conf = ConfigFactory.parseString(
        """
          |format = parquet
          |path = /a/b/c
          |hive.table = my_hive_table
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig.getNullConfig

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, conf, "", "", LocalDate.parse("2020-01-31"), 0, defaultHiveConfig, defaultPreferAddPartition = true)
      }

      assert(ex.getMessage.contains("Mandatory option missing: name"))
    }

    "throw an exception when the format definition is invalid" in {
      val conf = ConfigFactory.parseString(
        """
          |name = table1
          |format = iceberg
          |path = /a/b/c
          |hive.table = my_hive_table
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig.getNullConfig

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, conf, "", "", LocalDate.parse("2020-01-31"), 0, defaultHiveConfig, defaultPreferAddPartition = true)
      }

      assert(ex.getMessage.contains("Unable to read data format from config for the metastore table: table1"))
    }

    "throw an exception when the save mode is not supported" in {
      val conf = ConfigFactory.parseString(
        """
          |name = table1
          |format = parquet
          |path = /a/b/c
          |save.mode = "ignore"
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig.getNullConfig

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, conf, "", "", LocalDate.parse("2020-01-31"), 0, defaultHiveConfig, defaultPreferAddPartition = true)
      }

      assert(ex.getMessage.contains("Invalid or unsupported save mode: 'ignore' for table 'table1'."))
    }

    "throw an exception when the save mode is invalid" in {
      val conf = ConfigFactory.parseString(
        """
          |name = table1
          |format = parquet
          |path = /a/b/c
          |save.mode = "test"
          |""".stripMargin)

      val defaultHiveConfig = HiveDefaultConfig.getNullConfig

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, conf, "", "", LocalDate.parse("2020-01-31"), 0, defaultHiveConfig, defaultPreferAddPartition = true)
      }

      assert(ex.getMessage.contains("Invalid or unsupported save mode: 'test' for table 'table1'."))
    }
  }

  "getSaveMode()" should {
    "return SaveMode.Append" in {
      assert(MetaTable.getSaveMode("append", "table1") == SaveMode.Append)
    }

    "return SaveMode.Overwrite" in {
      assert(MetaTable.getSaveMode("overwrite", "table1") == SaveMode.Overwrite)
    }

    "throw an exception when the save mode is not supported" in {
      val ex = intercept[IllegalArgumentException] {
        MetaTable.getSaveMode("error_if_exists", "table1")
      }

      assert(ex.getMessage.contains("Invalid or unsupported save mode: 'error_if_exists' for table 'table1'"))
    }


    "throw an exception when the save mode is invalid" in {
      val ex = intercept[IllegalArgumentException] {
        MetaTable.getSaveMode("invalid", "table1")
      }

      assert(ex.getMessage.contains("Invalid or unsupported save mode: 'invalid' for table 'table1'"))
    }

  }

}
