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

class SinkTableSuite extends AnyWordSpec {
  "fromConfig" should {
    "create a list of source tables" in {
      val conf = ConfigFactory.parseString(
        """
          |sink.tables = [
          | {
          |  input.metastore.table = table11
          |  output.topic.name = "aaa"
          | },
          | {
          |  input.metastore.table = table22
          |  output.topic.name = "bbb"
          | },
          | {
          |  input.metastore.table = table33
          |  job.metastore.table = "output_table"
          | },
          | {
          |  input.metastore.table = table44
          |  warn.maximum.execution.time.seconds = 50
          |  transformations = [
          |    { col="a", expr="2+2" },
          |    { col="b", expr="cast(a) as string" },
          |  ]
          |  filters = [ "A > 1" ]
          |  columns = [ "A", "B"]
          |  date.from = "@infoDate - 1"
          |  date.to = "@infoDate + 1"
          | }
          |]
          |""".stripMargin)

      val sinkTables = SinkTable.fromConfig(conf, "sink.tables")

      assert(sinkTables.size == 4)

      val tbl1 = sinkTables.head
      val tbl2 = sinkTables(1)
      val tbl3 = sinkTables(2)
      val tbl4 = sinkTables(3)

      assert(tbl1.metaTableName == "table11")
      assert(tbl1.options.get("topic.name").contains("aaa"))
      assert(tbl1.columns.isEmpty)
      assert(tbl1.filters.isEmpty)
      assert(tbl1.rangeFromExpr.isEmpty)
      assert(tbl1.rangeToExpr.isEmpty)

      assert(tbl2.metaTableName == "table22")
      assert(tbl2.options.get("topic.name").contains("bbb"))
      assert(tbl2.columns.isEmpty)
      assert(tbl2.filters.isEmpty)
      assert(tbl2.rangeFromExpr.isEmpty)
      assert(tbl2.rangeToExpr.isEmpty)

      assert(tbl3.metaTableName == "table33")
      assert(tbl3.columns.isEmpty)
      assert(tbl3.filters.isEmpty)
      assert(tbl3.outputTableName.contains("output_table"))
      assert(tbl3.rangeFromExpr.isEmpty)
      assert(tbl3.rangeToExpr.isEmpty)
      assert(tbl3.warnMaxExecutionTimeSeconds.isEmpty)

      assert(tbl4.metaTableName == "table44")
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
    }

    "throw an exception in case of duplicate entries" in {
      val conf = ConfigFactory.parseString(
        """
          |sink.tables = [
          | {
          |  input.metastore.table = Table11
          | },
          | {
          |  input.metastore.table = table11
          | },
          | {
          |  input.metastore.table = table33
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SinkTable.fromConfig(conf, "sink.tables")
      }

      assert(ex.getMessage.contains("Duplicate sink table definitions for the sink job: table11"))
    }

    "throw an exception in case of missing 'col' in transformations" in {
      val conf = ConfigFactory.parseString(
        """
          |sink.tables = [
          | {
          |  input.metastore.table = Table11
          |  job.metastore.table = table12
          |  transformations = [ { expr = "2.2" } ]
          |  columns = [ "a", "b", "c" ]
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SinkTable.fromConfig(conf, "sink.tables")
      }

      assert(ex.getMessage.contains("'col' not set for the transformation"))
    }

    "throw an exception in case of missing 'expr' in transformations" in {
      val conf = ConfigFactory.parseString(
        """
          |sink.tables = [
          | {
          |  input.metastore.table = Table11
          |  job.metastore.table = table12
          |  transformations = [ { col = "2.2" } ]
          |  columns = [ "a", "b", "c" ]
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SinkTable.fromConfig(conf, "sink.tables")
      }

      assert(ex.getMessage.contains("Either 'expr' or 'comment' should be defined for for the transformation of '2.2' in sink.tables[0].transformations[0]"))
    }

  }

}
