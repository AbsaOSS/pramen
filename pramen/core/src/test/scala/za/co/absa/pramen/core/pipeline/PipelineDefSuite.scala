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

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.pipeline.OperationType.{Ingestion, Transformation}
import za.co.absa.pramen.core.schedule.Schedule
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate

class PipelineDefSuite extends AnyWordSpec with TempDirFixture {
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
    "beginOfMonth(@runDate)")

  "PipelineDef.fromConfig()" should {
    "be able to be read from config" in {
      withTempDirectory("pipeline_v2") { tempDir =>
        val conf = getConfig(tempDir)

        val pipeline = PipelineDef.fromConfig(conf, defaults)

        assert(pipeline.name == "pipeline_v2")
        assert(pipeline.operations.length == 2)

        val op1 = pipeline.operations.head
        val op2 = pipeline.operations(1)

        assert(op1.name == "op1")
        assert(op2.name == "op2")

        assert(op1.schedule.isInstanceOf[Schedule.EveryDay])
        assert(op1.expectedDelayDays == 1)
        assert(op1.dependencies.isEmpty)
        assert(op1.outputInfoDateExpression.contains("beginOfMonth(@runDate)"))
        assert(op1.processingTimestampColumn.contains("SYNC_TIMESTAMP"))

        assert(op1.schemaTransformations.isEmpty)

        assert(op2.expectedDelayDays == 0)
        assert(op2.schemaTransformations.length == 1)
        assert(op2.schemaTransformations.head.column == "B")
        assert(op2.schemaTransformations.head.expression == "cast(B as double)")

        val ingestion = op1.operationType.asInstanceOf[Ingestion]

        assert(ingestion.sourceName == "myjdbc")
        assert(ingestion.sourceTables.length == 2)
        assert(ingestion.sourceTables.head.metaTableName == "table1_sync")
        assert(ingestion.sourceTables(1).metaTableName == "table2_sync")

        val transformation = op2.operationType.asInstanceOf[Transformation]
        assert(transformation.clazz == "some.class")
      }
    }
  }

  def getConfig(basePath: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/pipeline_v2.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
