/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.tests.builtin.utils

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.builtin.model.JdbcSource.{Query, Table}
import za.co.absa.pramen.builtin.utils.JobConfigUtils

class JobConfigUtilsSuite extends WordSpec {
  "getTableDefs()" should {
    "support tables configured as an array" in {
      val tableConfig =
        """jdbc.sync {
          |table.1 = [ table1, /output/path/table1, 10 ]
          |table.2 = [ table2, /output/path/table2, 20 ]
          |}
          |""".stripMargin

      val conf = ConfigFactory.parseString(tableConfig)

      val tableDefs = JobConfigUtils.getTableDefs(conf)

      assert(tableDefs.size == 2)
      assert(tableDefs.head.nameInSyncWatcher == "table1")
      assert(tableDefs.head.outputPath == "/output/path/table1")
      assert(tableDefs.head.recordsPerPartition == 10)
      assert(tableDefs.head.source.isInstanceOf[Table])
      assert(tableDefs(1).nameInSyncWatcher == "table2")
      assert(tableDefs(1).outputPath == "/output/path/table2")
      assert(tableDefs(1).recordsPerPartition == 20)
      assert(tableDefs(1).source.isInstanceOf[Table])
    }

    "support tables configured as numbered subconfigs" in {
      val tableConfig =
        """jdbc.sync.tables {
          |  table.1 {
          |    db.table = db_table1
          |    pramen.table = pramen_table1
          |    columns = [ column1, "cast(column2, date) as column2", column3 ]
          |    output.path = /output/path/table1
          |    records.per.partition = 10
          |  },
          |  table.2 {
          |    sql = "SELECT * FROM db_table2"
          |    pramen.table = pramen_table2
          |    output.path = /output/path/table2
          |    records.per.partition = 20
          |  }
          |}
          |""".stripMargin

      val conf = ConfigFactory.parseString(tableConfig)

      val tableDefs = JobConfigUtils.getTableDefs(conf)

      assert(tableDefs.size == 2)
      assert(tableDefs.head.nameInSyncWatcher == "pramen_table1")
      assert(tableDefs.head.outputPath == "/output/path/table1")
      assert(tableDefs.head.recordsPerPartition == 10)
      assert(tableDefs.head.source.isInstanceOf[Table])
      assert(tableDefs.head.source.asInstanceOf[Table].name == "db_table1")
      assert(tableDefs.head.columns == Seq("column1", "cast(column2, date) as column2", "column3"))
      assert(tableDefs(1).nameInSyncWatcher == "pramen_table2")
      assert(tableDefs(1).outputPath == "/output/path/table2")
      assert(tableDefs(1).recordsPerPartition == 20)
      assert(tableDefs(1).source.isInstanceOf[Query])
      assert(tableDefs(1).source.asInstanceOf[Query].sql == "SELECT * FROM db_table2")
    }

    "fail on empty config since at least one table should be defined" in {
      val conf = ConfigFactory.empty()

      val ex = intercept[IllegalArgumentException] {
        JobConfigUtils.getTableDefs(conf)
      }

      assert(ex.getMessage.contains("No tables defined for synchronization"))
    }

    "fail if a mandatory option is missing in array" in {
      val tableConfig =
        """jdbc.sync {
          |table.1 = [ table1, /output/path/table1 ]
          |table.2 = [ table2, /output/path/table2 ]
          |}
          |""".stripMargin

      val conf = ConfigFactory.parseString(tableConfig)

      val ex = intercept[IllegalArgumentException] {
        JobConfigUtils.getTableDefs(conf)
      }

      assert(ex.getMessage.contains("Table definition for table 1 is invalid"))
    }

    "fail if a mandatory option is missing in config" in {
      val tableConfig =
        """jdbc.sync.tables {
          |  table.1 {
          |    db.table = db_table1
          |    pramen.table = pramen_table1
          |    columns = [ column1, "cast(column2, date) as column2", column3 ]
          |    output.path = /output/path/table1
          |  }
          |}
          |""".stripMargin

      val conf = ConfigFactory.parseString(tableConfig)

      val ex = intercept[IllegalArgumentException] {
        JobConfigUtils.getTableDefs(conf)
      }

      assert(ex.getMessage.contains("Mandatory configuration options are missing"))
    }
  }
}
