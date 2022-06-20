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

package za.co.absa.pramen.framework.metastore.model

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

import java.time.LocalDate

class MetaTableSuite extends WordSpec {
  "fromConfig()" should {
    "be able to parse multiple tables" in {
      val conf = ConfigFactory.parseString(
        """pramen.information.date.column = "INFORMATION_DATE"
          |pramen.information.date.format = "yyyy-MM-dd"
          |pramen.information.date.start = "2020-01-31"
          |pramen.track.days = 2
          |
          |syncpramen.metastore.tables = [
          |  {
          |    name = table1
          |    format = parquet
          |    path = /a/b/c
          |    information.date.column = INFO_DATE
          |    information.date.format = dd-MM-yyyy
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
          |  }
          |]
          |""".stripMargin)

      val metaTables = MetaTable.fromConfig(conf, "syncpramen.metastore.tables")

      assert(metaTables.size == 3)
      assert(metaTables.head.name == "table1")
      assert(metaTables.head.format.name == "parquet")
      assert(metaTables.head.infoDateColumn == "INFO_DATE")
      assert(metaTables.head.infoDateFormat == "dd-MM-yyyy")
      assert(metaTables.head.trackDays == 2)
      assert(metaTables.head.infoDateStart == LocalDate.of(2020, 1, 31))
      assert(metaTables(1).name == "table2")
      assert(metaTables(1).format.name == "delta")
      assert(metaTables(1).infoDateColumn == "INFORMATION_DATE")
      assert(metaTables(1).infoDateFormat == "yyyy-MM-dd")
      assert(metaTables(2).name == "table3")
      assert(metaTables(2).format.name == "delta")
      assert(metaTables(2).infoDateColumn == "INFORMATION_DATE")
      assert(metaTables(2).infoDateFormat == "yyyy-MM-dd")
    }

    "throw an exception if there are multiple tables with the same name" in {
      val conf = ConfigFactory.parseString(
        """pramen.information.date.column = "INFORMATION_DATE"
          |pramen.information.date.format = "yyyy-MM-dd"
          |pramen.information.date.start = "2020-01-31"
          |pramen.track.days = 1
          |
          |syncpramen.metastore.tables = [
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

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfig(conf, "syncpramen.metastore.tables")
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
          |""".stripMargin)

      val metaTable = MetaTable.fromConfigSingleEntity(conf, "INFO_DATE", "dd-MM-yyyy", LocalDate.parse("2020-01-31"), 0)

      assert(metaTable.name == "my_table")
      assert(metaTable.format.name == "delta")
      assert(metaTable.hiveTable.isEmpty)
      assert(metaTable.trackDays == 0)
      assert(metaTable.infoDateColumn == "INFO_DATE")
      assert(metaTable.infoDateFormat == "dd-MM-yyyy")
      assert(metaTable.infoDateStart.toString == "2020-01-31")
    }

    "load a metatable definition with hive table defined" in {
      val conf = ConfigFactory.parseString(
        """
          |name = my_table
          |format = parquet
          |path = /a/b/c
          |hive.table = my_hive_table
          |information.date.column = INFORMATION_DATE
          |information.date.format = yyyy-MM-dd
          |read.option {
          |  some.option.a = test1
          |  some.option.b = 12
          |}
          |write.option {
          |  x = test2
          |  y = 101
          |}
          |""".stripMargin)

      val metaTable = MetaTable.fromConfigSingleEntity(conf, "INFO_DATE", "dd-MM-yyyy", LocalDate.parse("2020-01-31"), 1)

      assert(metaTable.name == "my_table")
      assert(metaTable.format.name == "parquet")
      assert(metaTable.hiveTable.contains("my_hive_table"))
      assert(metaTable.trackDays == 1)
      assert(metaTable.infoDateColumn == "INFORMATION_DATE")
      assert(metaTable.infoDateFormat == "yyyy-MM-dd")
      assert(metaTable.readOptions("some.option.a") == "test1")
      assert(metaTable.readOptions("some.option.b") == "12")
      assert(metaTable.writeOptions("x") == "test2")
      assert(metaTable.writeOptions("y") == "101")
    }

    "throw an exception when mandatory the option is missing" in {
      val conf = ConfigFactory.parseString(
        """
          |format = parquet
          |path = /a/b/c
          |hive.table = my_hive_table
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, "", "", LocalDate.parse("2020-01-31"), 0)
      }

      assert(ex.getMessage.contains("Mandatory option missing: name"))
    }

    "throw an exception when the format definition is invalid" in {
      val conf = ConfigFactory.parseString(
        """
          |name = table1
          |format = isberg
          |path = /a/b/c
          |hive.table = my_hive_table
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        MetaTable.fromConfigSingleEntity(conf, "", "", LocalDate.parse("2020-01-31"), 0)
      }

      assert(ex.getMessage.contains("Unable to read data format from config for the metastore table: table1"))
    }
  }

}
