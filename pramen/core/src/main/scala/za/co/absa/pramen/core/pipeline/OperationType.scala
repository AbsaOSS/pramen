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

import com.typesafe.config.Config
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.utils.ConfigUtils

/** This is a base class for all Pramen jobs (new API). */
sealed trait OperationType

object OperationType {
  case class Ingestion(sourceName: String, sourceTables: Seq[SourceTable]) extends OperationType
  case class Transformation(clazz: String, outputTable: String) extends OperationType
  case class PythonTransformation(pythonClass: String, outputTable: String) extends OperationType
  case class Sink(sinkName: String, sinkTables: Seq[SinkTable]) extends OperationType
  case class Transfer(sourceName: String, sinkName: String, tables: Seq[TransferTable]) extends OperationType

  val TYPE_KEY = "type"
  val DEFAULT_TYPE_KEY = "pramen.default.operation.type"
  val SOURCE_KEY = "source"
  val SINK_KEY = "sink"
  val TABLES_KEY = "tables"
  val CLASS_KEY = "class"
  val PYTHON_CLASS_KEY = "python.class"
  val OUTPUT_TABLE_KEY = "output.table"

  def fromConfig(conf: Config, appConfig: Config, infoDateConfig: InfoDateConfig, parent: String): OperationType = {
    if (conf.hasPath(TYPE_KEY)) {
      getOperationTypeFromName(conf.getString(TYPE_KEY), conf, infoDateConfig, parent)
    } else {
      getDefaultOperationType(conf, appConfig, infoDateConfig, parent)
    }
  }

  private [core] def getDefaultOperationType(conf: Config, appConfig: Config, infoDateConfig: InfoDateConfig, parent: String): OperationType = {
    if (appConfig.hasPath(DEFAULT_TYPE_KEY)) {
      getOperationTypeFromName(appConfig.getString(DEFAULT_TYPE_KEY), conf, infoDateConfig, parent)
    } else {
      throw new IllegalArgumentException(s"Missing either $parent.$TYPE_KEY or $DEFAULT_TYPE_KEY")
    }
  }

  private[core] def getOperationTypeFromName(name: String, conf: Config, infoDateConfig: InfoDateConfig, parent: String): OperationType = {
    name match {
      case "ingestion" | "sourcing" | "extract" =>
        ConfigUtils.validatePathsExistence(conf, parent, Seq(SOURCE_KEY, TABLES_KEY))
        val source = conf.getString(SOURCE_KEY)

        val tables = SourceTable.fromConfig(conf, TABLES_KEY)
        Ingestion(source, tables)
      case "transformation" | "transformer" | "transform" =>
        ConfigUtils.validatePathsExistence(conf, parent, Seq(CLASS_KEY, OUTPUT_TABLE_KEY))

        val clazz = conf.getString(CLASS_KEY)
        val outputTable = conf.getString(OUTPUT_TABLE_KEY)
        Transformation(clazz, outputTable)
      case "python_transformation" | "python_transformer" =>
        ConfigUtils.validatePathsExistence(conf, parent, Seq(PYTHON_CLASS_KEY, OUTPUT_TABLE_KEY))

        val pythonClass = conf.getString(PYTHON_CLASS_KEY)
        val outputTable = conf.getString(OUTPUT_TABLE_KEY)
        PythonTransformation(pythonClass, outputTable)
      case "sink" | "load" =>
        ConfigUtils.validatePathsExistence(conf, parent, Seq(SINK_KEY, TABLES_KEY))
        val sink = conf.getString(SINK_KEY)

        val tables = SinkTable.fromConfig(conf, TABLES_KEY)
        Sink(sink, tables)
      case "transfer" | "source2sink" =>
        ConfigUtils.validatePathsExistence(conf, parent, Seq(SOURCE_KEY, SINK_KEY, TABLES_KEY))
        val source = conf.getString(SOURCE_KEY)
        val sink = conf.getString(SINK_KEY)

        val tables = TransferTable.fromConfig(conf, infoDateConfig, TABLES_KEY, sink)

        Transfer(source, sink, tables)
      case _ => throw new IllegalArgumentException(s"Unknown operation type: ${conf.getString(TYPE_KEY)} at $parent")
    }
  }
}
