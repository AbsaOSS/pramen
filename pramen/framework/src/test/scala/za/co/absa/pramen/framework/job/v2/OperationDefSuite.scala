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

package za.co.absa.pramen.framework.job.v2

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.schedule.EveryDay
import za.co.absa.pramen.framework.app.config.InfoDateConfig
import za.co.absa.pramen.framework.fixtures.TempDirFixture
import za.co.absa.pramen.framework.job.v2.OperationType.{Ingestion, Sink, Transformation}

import java.time.LocalDate

class OperationDefSuite extends WordSpec with TempDirFixture {
  private val defaults = InfoDateConfig("INFO_DATE",
    "yyyy-MM-dd",
    LocalDate.of(2022, 1, 1),
    4,
    0,
    "@date",
    "lastSaturday(@runDate)",
    "beginOfMonth(@runDate)")

  "OperationDef.fromConfig()" should {
    "return None for the disabled operation" in {
      val conf = ConfigFactory.parseString(
        s"""name = "dummy_name"
           |type = "transformation"
           |schedule.type = "daily"
           |
           |disabled = "true"
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, defaults, "path", 0)

      assert(op.isEmpty)
    }

    "be able to serialize an ingestion operation" in {
      val conf = ConfigFactory.parseString(
        s"""name = "dummy_name"
           |type = "ingestion"
           |schedule.type = "daily"
           |
           |source = "jdbc1"
           |
           |info.date.expr = "dummy_expr"
           |tables = [
           |  {
           |    input.db.table = table1_db
           |    output.metastore.table = table1_sync
           |  }
           |]
           |
           |option.myoption1 = "test"
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, defaults, "path", 0).get

      assert(op.name == "dummy_name")
      assert(op.schedule.isInstanceOf[EveryDay])
      assert(op.outputInfoDateExpression.contains("dummy_expr"))
      assert(op.operationType.asInstanceOf[Ingestion].sourceName == "jdbc1")
      assert(op.operationType.asInstanceOf[Ingestion].sourceTables.size == 1)
      assert(op.operationType.asInstanceOf[Ingestion].sourceTables.head.metaTableName == "table1_sync")
      assert(op.dependencies.isEmpty)
      assert(op.schemaTransformations.isEmpty)
      assert(op.extraOptions("myoption1") == "test")
    }

    "be able to serialize a transformation operation" in {
      val conf = ConfigFactory.parseString(
        s"""name = "dummy_transformation"
           |type = "transformer"
           |schedule.type = "daily"
           |class = "myclass"
           |output.table = "dummy_table"
           |
           |dependencies = [
           |  {
           |    tables = [table1]
           |    date.from = "@infoDate - 1"
           |    date.to = "@infoDate"
           |    trigger.updates = true
           |  },
           |  {
           |    tables = [table2, table3]
           |    date.from = "@infoDate"
           |  }
           |]
           |
           |transformations = [
           |    {col = "A", expr = "cast(A as decimal(15,5))"}
           |]
           |
           |filters = [ "A > 0", "B < 2" ]
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, defaults, "path", 0).get

      assert(op.name == "dummy_transformation")
      assert(op.schedule.isInstanceOf[EveryDay])
      assert(op.outputInfoDateExpression == "@date")
      assert(op.operationType.asInstanceOf[Transformation].clazz == "myclass")
      assert(op.dependencies.length == 2)
      assert(op.dependencies.head.tables.contains("table1"))
      assert(op.dependencies.head.dateFromExpr.contains("@infoDate - 1"))
      assert(op.dependencies.head.dateUntilExpr.contains("@infoDate"))
      assert(op.dependencies.head.triggerUpdates)
      assert(op.dependencies(1).tables.contains("table2"))
      assert(!op.dependencies(1).triggerUpdates)
      assert(op.schemaTransformations.length == 1)
      assert(op.schemaTransformations.head.column == "A")
      assert(op.schemaTransformations.head.expression == "cast(A as decimal(15,5))")
      assert(op.filters.length == 2)
      assert(op.filters.head == "A > 0")
    }

    "be able to serialize an sink operation" in {
      val conf = ConfigFactory.parseString(
        s"""name = "dummy_name"
           |type = "sink"
           |schedule.type = "daily"
           |
           |sink = "kafka1"
           |
           |tables = [
           |  {
           |    input.metastore.table = table1_sync
           |    output.topic = table1_topic
           |  }
           |]
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, defaults, "path", 0).get

      assert(op.name == "dummy_name")
      assert(op.schedule.isInstanceOf[EveryDay])
      assert(op.operationType.asInstanceOf[Sink].sinkName == "kafka1")
      assert(op.operationType.asInstanceOf[Sink].sinkTables.size == 1)
      assert(op.operationType.asInstanceOf[Sink].sinkTables.head.metaTableName == "table1_sync")
      assert(op.operationType.asInstanceOf[Sink].sinkTables.head.options("topic") == "table1_topic")
    }
  }
}
