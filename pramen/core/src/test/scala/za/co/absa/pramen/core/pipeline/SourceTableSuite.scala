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
import za.co.absa.pramen.api.Query

class SourceTableSuite extends AnyWordSpec {
  "fromConfig" should {
    "create a list of source tables" in {
      val conf = ConfigFactory.parseString(
        """
          |source.tables = [
          | {
          |  output.metastore.table = table11
          |  input.table = table12
          |  columns = [ "a", "b", "c" ]
          | },
          | {
          |  output.metastore.table = table22
          |  input.sql = "SELECT * FROM X"
          | },
          | {
          |  output.metastore.table = table33
          |  input.path = "/a/b/c"
          | },
          | {
          |  output.metastore.table = table44
          |  input.db.table = table14
          |  warn.maximum.execution.time.seconds = 50
          |  transformations = [
          |    { col="a", expr="2+2" },
          |    { col="b", expr="cast(a) as string" },
          |  ]
          |  filters = [ "A > 1" ],
          |  date.from = "@infoDate - 1"
          |  date.to = "@infoDate + 1"
          | }
          |]
          |""".stripMargin)

      val sourceTables = SourceTable.fromConfig(conf, "source.tables")

      assert(sourceTables.size == 4)

      val tbl1 = sourceTables.head
      val tbl2 = sourceTables(1)
      val tbl3 = sourceTables(2)
      val tbl4 = sourceTables(3)

      assert(tbl1.metaTableName == "table11")
      assert(tbl1.query.isInstanceOf[Query.Table])
      assert(tbl1.query.asInstanceOf[Query.Table].dbTable == "table12")
      assert(tbl1.columns == Seq("a", "b", "c"))
      assert(tbl1.filters.isEmpty)
      assert(tbl1.rangeFromExpr.isEmpty)
      assert(tbl1.rangeToExpr.isEmpty)

      assert(tbl2.metaTableName == "table22")
      assert(tbl2.query.isInstanceOf[Query.Sql])
      assert(tbl2.query.asInstanceOf[Query.Sql].query == "SELECT * FROM X")
      assert(tbl2.columns.isEmpty)
      assert(tbl2.filters.isEmpty)
      assert(tbl2.rangeFromExpr.isEmpty)
      assert(tbl2.rangeToExpr.isEmpty)

      assert(tbl3.metaTableName == "table33")
      assert(tbl3.query.isInstanceOf[Query.Path])
      assert(tbl3.query.asInstanceOf[Query.Path].path == "/a/b/c")
      assert(tbl3.columns.isEmpty)
      assert(tbl3.filters.isEmpty)
      assert(tbl3.rangeFromExpr.isEmpty)
      assert(tbl3.rangeToExpr.isEmpty)
      assert(tbl3.warnMaxExecutionTimeSeconds.isEmpty)

      assert(tbl4.metaTableName == "table44")
      assert(tbl4.query.isInstanceOf[Query.Table])
      assert(tbl4.query.asInstanceOf[Query.Table].dbTable == "table14")
      assert(tbl4.warnMaxExecutionTimeSeconds.contains(50))
      assert(tbl4.transformations.length == 2)
      assert(tbl4.transformations.head.column == "a")
      assert(tbl4.transformations.head.expression.contains("2+2"))
      assert(tbl4.transformations(1).column == "b")
      assert(tbl4.transformations(1).expression.contains("cast(a) as string"))
      assert(tbl4.columns == Seq.empty[String])
      assert(tbl4.filters == Seq("A > 1"))
      assert(tbl4.rangeFromExpr.contains("@infoDate - 1"))
      assert(tbl4.rangeToExpr.contains("@infoDate + 1"))
    }

    "throw an exception in case of an incorrect query type" in {
      val conf = ConfigFactory.parseString(
        """
          |source.tables = [
          | {
          |  output.metastore.table = table11
          |  xyz = table12
          |  columns = [ "a", "b", "c" ]
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SourceTable.fromConfig(conf, "source.tables")
      }

      assert(ex.getMessage.contains("No options are specified for the 'input' query. Usually, it is one of: 'input.sql', 'input.path', 'input.table', 'input.db.table' at source.tables[0]."))
    }

    "throw an exception in case of duplicate entries" in {
      val conf = ConfigFactory.parseString(
        """
          |source.tables = [
          | {
          |  output.metastore.table = Table11
          |  input.table = table12
          |  columns = [ "a", "b", "c" ]
          | },
          | {
          |  output.metastore.table = table11
          |  input.sql = "SELECT * FROM X"
          | },
          | {
          |  output.metastore.table = table33
          |  input.path = "/a/b/c"
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SourceTable.fromConfig(conf, "source.tables")
      }

      assert(ex.getMessage.contains("Duplicate source table definitions for the sourcing job: table11"))
    }

    "throw an exception in case of missing 'col' in transformations" in {
      val conf = ConfigFactory.parseString(
        """
          |source.tables = [
          | {
          |  output.metastore.table = Table11
          |  input.table = table12
          |  transformations = [ { expr = "2.2" } ]
          |  columns = [ "a", "b", "c" ]
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SourceTable.fromConfig(conf, "source.tables")
      }

      assert(ex.getMessage.contains("'col' not set for the transformation"))
    }

    "throw an exception in case of missing 'expr' in transformations" in {
      val conf = ConfigFactory.parseString(
        """
          |source.tables = [
          | {
          |  output.metastore.table = Table11
          |  input.table = table12
          |  transformations = [ { col = "2.2" } ]
          |  columns = [ "a", "b", "c" ]
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        SourceTable.fromConfig(conf, "source.tables")
      }

      assert(ex.getMessage.contains("Either 'expr' or 'comment' should be defined for for the transformation"))
    }

  }

}
