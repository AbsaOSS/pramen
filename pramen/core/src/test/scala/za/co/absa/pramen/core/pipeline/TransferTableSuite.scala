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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{DataFormat, Query}
import za.co.absa.pramen.core.app.config.InfoDateConfig

import java.time.LocalDate

class TransferTableSuite extends AnyWordSpec {
  "fromConfig" should {
    "create a list of transfer tables" in {
      val conf = ConfigFactory.parseString(
        """
          |transfer.tables = [
          | {
          |    input.db.table = db1.table1
          |    output.path = /datalake/path
          | },
          | {
          |    input.sql = "SELECT * FROM users"
          |    job.metastore.table = "output_table1"
          |    output.topic.name = "mytopic1"
          | },
          | {
          |    input.path = "hdfs://something/something"
          |    job.metastore.table = "output_table2"
          |    output.table.name = "mytable2"
          | },
          | {
          |    input.db.table = db1.table2
          |    job.metastore.table = "output_table3"
          |    output {
          |      path = /datalake/path
          |      write.legacy = "true"
          |    }
          |    warn.maximum.execution.time.seconds = 50
          |    transformations = [
          |      { col="a", expr="2+2" },
          |      { col="b", expr="cast(a) as string" },
          |    ]
          |    filters = [ "A > 1" ]
          |    columns = [ "A", "B"]
          |    date.from = "@infoDate - 1"
          |    date.to = "@infoDate + 1"
          |    read.option {
          |      mergeSchema = "true"
          |    }
          |    track.days = 10
          |    information.date.start = "2022-01-19"
          |    source {
          |       source.key = "value1"
          |    }
          |    sink {
          |       sink.key = "value2"
          |    }
          |  }
          |]
          |""".stripMargin)
        .withFallback(ConfigFactory.load())

      val infoDateConfig = InfoDateConfig.fromConfig(conf)
      val tables = TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")

      assert(tables.size == 4)

      val tbl1 = tables.head
      val tbl2 = tables(1)
      val tbl3 = tables(2)
      val tbl4 = tables(3)

      assert(tbl1.query.isInstanceOf[Query.Table])
      assert(tbl1.query.asInstanceOf[Query.Table].dbTable == "db1.table1")
      assert(tbl1.jobMetaTableName == "db1.table1->mysink")
      assert(tbl1.writeOptions.contains("path"))
      assert(tbl1.writeOptions("path") == "/datalake/path")
      assert(tbl1.columns.isEmpty)
      assert(tbl1.filters.isEmpty)
      assert(tbl1.rangeFromExpr.isEmpty)
      assert(tbl1.rangeToExpr.isEmpty)
      assert(tbl1.sourceOverrideConf.isEmpty)
      assert(tbl1.sinkOverrideConf.isEmpty)

      assert(tbl2.query.isInstanceOf[Query.Sql])
      assert(tbl2.query.asInstanceOf[Query.Sql].sql == "SELECT * FROM users")
      assert(tbl2.jobMetaTableName == "output_table1")
      assert(tbl2.writeOptions.contains("topic.name"))
      assert(tbl2.writeOptions("topic.name") == "mytopic1")
      assert(tbl2.columns.isEmpty)
      assert(tbl2.filters.isEmpty)
      assert(tbl2.rangeFromExpr.isEmpty)
      assert(tbl2.rangeToExpr.isEmpty)

      assert(tbl3.query.isInstanceOf[Query.Path])
      assert(tbl3.query.asInstanceOf[Query.Path].path == "hdfs://something/something")
      assert(tbl3.jobMetaTableName == "output_table2")
      assert(tbl3.writeOptions.contains("table.name"))
      assert(tbl3.writeOptions("table.name") == "mytable2")
      assert(tbl3.columns.isEmpty)
      assert(tbl3.filters.isEmpty)
      assert(tbl3.rangeFromExpr.isEmpty)
      assert(tbl3.rangeToExpr.isEmpty)
      assert(tbl3.warnMaxExecutionTimeSeconds.isEmpty)

      assert(tbl4.query.isInstanceOf[Query.Table])
      assert(tbl4.query.asInstanceOf[Query.Table].dbTable == "db1.table2")
      assert(tbl4.jobMetaTableName == "output_table3")
      assert(tbl4.writeOptions.contains("path"))
      assert(tbl4.writeOptions("path") == "/datalake/path")
      assert(tbl4.writeOptions.contains("write.legacy"))
      assert(tbl4.writeOptions("write.legacy") == "true")
      assert(tbl4.readOptions.contains("mergeSchema"))
      assert(tbl4.readOptions("mergeSchema") == "true")
      assert(tbl4.trackDays == 10)
      assert(tbl4.infoDateStart == LocalDate.of(2022, 1, 19))
      assert(tbl4.warnMaxExecutionTimeSeconds.contains(50))

      assert(tbl4.transformations.length == 2)
      assert(tbl4.transformations.head.column == "a")
      assert(tbl4.transformations.head.expression.contains("2+2"))
      assert(tbl4.transformations(1).column == "b")
      assert(tbl4.transformations(1).expression.contains("cast(a) as string"))
      assert(tbl4.filters.length == 1)
      assert(tbl4.filters.head == "A > 1")
      assert(tbl4.columns.length == 2)
      assert(tbl4.columns.head == "A")
      assert(tbl4.columns(1) == "B")
      assert(tbl4.rangeFromExpr.contains("@infoDate - 1"))
      assert(tbl4.rangeToExpr.contains("@infoDate + 1"))

      assert(tbl4.sourceOverrideConf.nonEmpty)
      assert(tbl4.sourceOverrideConf.exists(c => c.getString("source.key") == "value1"))
      assert(tbl4.sinkOverrideConf.nonEmpty)
      assert(tbl4.sinkOverrideConf.exists(c => c.getString("sink.key") == "value2"))
    }

    "throw an exception in case of duplicate entries" in {
      val conf = ConfigFactory.parseString(
        """transfer.tables = [
          | {
          |    input.db.table = db1.table1
          |    output.path = /datalake/path
          | },
          | {
          |    input.db.table = db1.table2
          |    output.path = /datalake/path
          | },
          | {
          |    input.db.table = db1.table1
          |    output.path = /datalake/path
          | }
          |]
          |""".stripMargin)
        .withFallback(ConfigFactory.load())

      val infoDateConfig = InfoDateConfig.fromConfig(conf)

      val ex = intercept[IllegalArgumentException] {
        TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")
      }

      assert(ex.getMessage.contains("Duplicate table definitions for the transfer job: db1.table1->mysink"))
    }

    "throw an exception when job metastore table can't be determined" in {
      val conf = ConfigFactory.parseString(
        """transfer.tables = [
          | {
          |    input.sql = "SELECT * FROM users"
          |    output.path = /datalake/path
          | }
          |]
          |""".stripMargin)
        .withFallback(ConfigFactory.load())

      val infoDateConfig = InfoDateConfig.fromConfig(conf)
      val ex = intercept[IllegalArgumentException] {
        TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")
      }

      assert(ex.getMessage.contains("Cannot determine metastore table name for 'Sql(SELECT * FROM users) -> mysink"))
    }
  }

  "transfer table methods" when {
    val conf = ConfigFactory.parseString(
      """transfer.tables = [
        | {
        |    input.table = "table1"
        |    output.path = /datalake/path
        |
        |    transformations = [
        |      { col="a", expr="2+2" },
        |      { col="b", expr="cast(a) as string" },
        |    ]
        |    filters = [ "A > 1" ]
        |    columns = [ "A", "B"]
        |    date.from = "@infoDate - 1"
        |    date.to = "@infoDate + 1"
        |    read.option {
        |      mergeSchema = "true"
        |    }
        |    spark.config = {
        |      key1 = value1
        |    }
        |    track.days = 10
        |    information.date.start = "2022-01-19"
        |    source {
        |       source.key = "value1"
        |    }
        |    sink {
        |       sink.key = "value2"
        |    }
        | }
        |]
        |""".stripMargin)
      .withFallback(ConfigFactory.load())

    "getSourceTable()" should {
      "correctly create a source table" in {
        val infoDateConfig = InfoDateConfig.fromConfig(conf)
        val tables = TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")

        val sourceTable = tables.head.getSourceTable

        assert(tables.head.sparkConfig("key1") == "value1")
        assert(sourceTable.metaTableName == "table1->mysink")
        assert(sourceTable.query.isInstanceOf[Query.Table])
        assert(sourceTable.query.asInstanceOf[Query.Table].dbTable == "table1")
        assert(sourceTable.rangeFromExpr.contains("@infoDate - 1"))
        assert(sourceTable.rangeToExpr.contains("@infoDate + 1"))
        assert(sourceTable.transformations.length == 2)
        assert(sourceTable.filters.length == 1)
        assert(sourceTable.columns.length == 2)
        assert(sourceTable.overrideConf.exists(c => c.hasPath("source.key")))
        assert(!sourceTable.overrideConf.exists(c => c.hasPath("sink.key")))
      }
    }

    "getSinkTable()" should {
      "correctly create a sink table" in {
        val infoDateConfig = InfoDateConfig.fromConfig(conf)
        val tables = TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")

        val sinkTable = tables.head.getSinkTable

        assert(sinkTable.metaTableName == "table1->mysink")
        assert(sinkTable.outputTableName.contains("table1->mysink"))
        assert(sinkTable.rangeFromExpr.contains("@infoDate - 1"))
        assert(sinkTable.rangeToExpr.contains("@infoDate + 1"))
        assert(sinkTable.transformations.length == 2)
        assert(sinkTable.filters.length == 1)
        assert(sinkTable.columns.length == 2)
        assert(!sinkTable.overrideConf.exists(c => c.hasPath("source.key")))
        assert(sinkTable.overrideConf.exists(c => c.hasPath("sink.key")))
      }
    }

    "getMetaTable()" should {
      "correctly create a metastore table" in {
        val infoDateConfig = InfoDateConfig.fromConfig(conf)
        val tables = TransferTable.fromConfig(conf, infoDateConfig, "transfer.tables", "mysink")

        val metaTable = tables.head.getMetaTable

        assert(metaTable.name == "table1->mysink")
        assert(metaTable.format.isInstanceOf[DataFormat.Null])
        assert(metaTable.readOptions.contains("mergeSchema"))
        assert(metaTable.writeOptions.contains("path"))
        assert(metaTable.hiveTable.isEmpty)
        assert(metaTable.infoDateStart == LocalDate.of(2022, 1, 19))
        assert(metaTable.trackDays == 10)
      }
    }
  }

}
