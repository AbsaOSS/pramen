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
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.pipeline.OperationType.{Ingestion, Transformation}
import za.co.absa.pramen.core.schedule.Schedule

import java.time.LocalDate

class OperationDefSuite extends AnyWordSpec with TempDirFixture {
  private val defaults = InfoDateConfig("INFO_DATE",
    "yyyy-MM-dd",
    LocalDate.of(2022, 1, 1),
    4,
    0,
    "@date",
    "lastSaturday(@runDate)",
    "beginOfMonth(@runDate)",
    "@runDate",
    "@runDate - 6",
    "beginOfMonth(@runDate)"
  )
  private val appConfig = ConfigFactory.parseString(
    s"""
       |pramen.parallel.tasks = 4
       |""".stripMargin
  )

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

      val op = OperationDef.fromConfig(conf, appConfig, defaults, "path", 0)

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
           |notification.targets = [ "hyperdrive1", "custom2" ]
           |
           |option.myoption1 = "test"
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, appConfig, defaults, "path", 0).get

      assert(op.name == "dummy_name")
      assert(op.schedule.isInstanceOf[Schedule.EveryDay])
      assert(op.outputInfoDateExpression.contains("dummy_expr"))
      assert(op.operationType.asInstanceOf[Ingestion].sourceName == "jdbc1")
      assert(op.operationType.asInstanceOf[Ingestion].sourceTables.size == 1)
      assert(op.operationType.asInstanceOf[Ingestion].sourceTables.head.metaTableName == "table1_sync")
      assert(op.allowParallel)
      assert(!op.alwaysAttempt)
      assert(op.warnMaxExecutionTimeSeconds.isEmpty)
      assert(op.killMaxExecutionTimeSeconds.isEmpty)
      assert(op.notificationTargets.size == 2)
      assert(op.notificationTargets.head == "hyperdrive1")
      assert(op.notificationTargets(1) == "custom2")
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
           |always.attempt = "true"
           |warn.maximum.execution.time.seconds = 50
           |kill.maximum.execution.time.seconds = 100
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
           |    {col = "A", expr = "cast(A as decimal(15,5))"},
           |    {col = "D", expr = "cast(A as decimal(14,4))", comment = "Test1"},
           |    {col = "C", expr = "drop", comment = "Test2"},
           |    {col = "A", comment = "Test3"},
           |]
           |
           |filters = [ "A > 0", "B < 2" ]
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, appConfig, defaults, "path", 0).get

      assert(op.name == "dummy_transformation")
      assert(op.schedule.isInstanceOf[Schedule.EveryDay])
      assert(op.outputInfoDateExpression == "@date")
      assert(op.operationType.asInstanceOf[Transformation].clazz == "myclass")
      assert(op.allowParallel)
      assert(op.alwaysAttempt)
      assert(op.dependencies.length == 2)
      assert(op.dependencies.head.tables.contains("table1"))
      assert(op.dependencies.head.dateFromExpr.contains("@infoDate - 1"))
      assert(op.dependencies.head.dateUntilExpr.contains("@infoDate"))
      assert(op.dependencies.head.triggerUpdates)
      assert(op.dependencies(1).tables.contains("table2"))
      assert(!op.dependencies(1).triggerUpdates)
      assert(op.warnMaxExecutionTimeSeconds.contains(50))
      assert(op.killMaxExecutionTimeSeconds.contains(100))
      assert(op.schemaTransformations.length == 4)
      assert(op.schemaTransformations.head.column == "A")
      assert(op.schemaTransformations.head.expression.contains("cast(A as decimal(15,5))"))
      assert(op.schemaTransformations.head.comment.isEmpty)
      assert(op.schemaTransformations(1).column == "D")
      assert(op.schemaTransformations(1).expression.contains("cast(A as decimal(14,4))"))
      assert(op.schemaTransformations(1).comment.contains("Test1"))
      assert(op.schemaTransformations(2).column == "C")
      assert(op.schemaTransformations(2).expression.contains("drop"))
      assert(op.schemaTransformations(2).comment.contains("Test2"))
      assert(op.schemaTransformations(3).column == "A")
      assert(op.schemaTransformations(3).expression.isEmpty)
      assert(op.schemaTransformations(3).comment.contains("Test3"))
      assert(op.filters.length == 2)
      assert(op.filters.head == "A > 0")
      assert(op.consumeThreads == 1)
    }

    "be able to serialize an sink operation" in {
      val conf = ConfigFactory.parseString(
        s"""name = "dummy_name"
           |type = "sink"
           |schedule.type = "daily"
           |
           |sink = "kafka1"
           |
           |parallel = false
           |
           |tables = [
           |  {
           |    input.metastore.table = table1_sync
           |    output.topic = table1_topic
           |  }
           |]
           |""".stripMargin
      )

      val op = OperationDef.fromConfig(conf, appConfig, defaults, "path", 0).get

      assert(op.name == "dummy_name")
      assert(op.schedule.isInstanceOf[Schedule.EveryDay])
      assert(op.operationType.asInstanceOf[OperationType.Sink].sinkName == "kafka1")
      assert(op.operationType.asInstanceOf[OperationType.Sink].sinkTables.size == 1)
      assert(op.operationType.asInstanceOf[OperationType.Sink].sinkTables.head.metaTableName == "table1_sync")
      assert(op.operationType.asInstanceOf[OperationType.Sink].sinkTables.head.options("topic") == "table1_topic")
      assert(!op.allowParallel)
    }

    "set a correct number of threads to consume by an operation and handles edge cases" in {
      def getDummyOperation(consumeThreads: Int) = {
        val conf = ConfigFactory.parseString(
          s"""name = "dummy_name"
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
             |  }
             |]
             |
             |consume.threads = $consumeThreads
             |""".stripMargin)
        OperationDef.fromConfig(conf, appConfig, defaults, "path", 0).get
      }

      val tooManyThreadsDefined = getDummyOperation(consumeThreads = 999)
      val negativeThreadsDefined = getDummyOperation(consumeThreads = -15)
      val zeroThreadsDefined = getDummyOperation(consumeThreads = 0)
      val okThreadCountDefined = getDummyOperation(consumeThreads = 2)

      assert(tooManyThreadsDefined.consumeThreads == 4)
      assert(negativeThreadsDefined.consumeThreads == 1)
      assert(zeroThreadsDefined.consumeThreads == 1)
      assert(okThreadCountDefined.consumeThreads == 2)
    }

  }
}
