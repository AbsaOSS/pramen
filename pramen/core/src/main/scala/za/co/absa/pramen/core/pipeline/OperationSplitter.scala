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
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.Transformer
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.config.Keys.SPECIAL_CHARACTERS_IN_COLUMN_NAMES
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.pipeline.OperationType._
import za.co.absa.pramen.core.pipeline.PythonTransformationJob._
import za.co.absa.pramen.core.process.ProcessRunnerImpl
import za.co.absa.pramen.core.sink.SinkManager
import za.co.absa.pramen.core.source.SourceManager
import za.co.absa.pramen.core.utils.ClassLoaderUtils

class OperationSplitter(conf: Config,
                        metastore: Metastore,
                        bookkeeper: Bookkeeper)(implicit spark: SparkSession) {
  def createJobs(operationDef: OperationDef): Seq[Job] = {
    operationDef.operationType match {
      case Ingestion(sourceName, sourceTables)            => createIngestion(operationDef, sourceName, sourceTables)
      case Transformation(clazz, outputTable)             => createTransformation(operationDef, clazz, outputTable)
      case PythonTransformation(pythonClass, outputTable) => createPythonTransformation(operationDef, pythonClass, outputTable)
      case Sink(sinkName, sinkTables)                     => createSink(operationDef, sinkName, sinkTables)
    }
  }

  def createIngestion(operationDef: OperationDef,
                      sourceName: String,
                      sourceTables: Seq[SourceTable])(implicit spark: SparkSession): Seq[Job] = {
    val specialCharacters = conf.getString(SPECIAL_CHARACTERS_IN_COLUMN_NAMES)
    val sourceBase = SourceManager.getSourceByName(sourceName, conf, None)

    sourceTables.map(sourceTable => {
      val source = sourceTable.overrideConf match {
        case Some(confOverride) => SourceManager.getSourceByName(sourceName, conf, Some(confOverride))
        case None               => sourceBase
      }

      val outputTable = metastore.getTableDef(sourceTable.metaTableName)

      new IngestionJob(operationDef, metastore, bookkeeper, source, sourceTable, outputTable, specialCharacters)
    })
  }

  def createTransformation(operationDef: OperationDef,
                           clazz: String,
                           outputTable: String)(implicit spark: SparkSession): Seq[Job] = {
    val transformer = ClassLoaderUtils.loadConfigurableClass[Transformer](clazz, conf)

    val outputMetaTable = metastore.getTableDef(outputTable)

    Seq(new TransformationJob(operationDef, metastore, bookkeeper, outputMetaTable, transformer))
  }

  def createPythonTransformation(operationDef: OperationDef,
                                 pythonClass: String,
                                 outputTable: String)(implicit spark: SparkSession): Seq[Job] = {
    val outputMetaTable = metastore.getTableDef(outputTable)
    val pramenPyLocation = conf.getString(PRAMEN_PY_LOCATION_KEY)
    val pramenPyExecutable = conf.getString(PRAMEN_PY_EXECUTABLE_KEY)
    val cmdLineTemplate = conf.getString(PRAMEN_PY_CMD_LINE_TEMPLATE_KEY)

    val pramenPyConfig = PramenPyConfig(pramenPyLocation, pramenPyExecutable, cmdLineTemplate)

    val keepLogLines = conf.getInt(KEEP_LOG_LINES_KEY)

    val processRunner = new ProcessRunnerImpl(keepLogLines,
      logStdOut = true,
      logStdErr = true,
      stdOutLogPrefix = "Pramen-Py(out)",
      stdErrLogPrefix = "Pramen-Py(err)",
      redirectErrorStream = false)

    Seq(new PythonTransformationJob(operationDef, metastore, bookkeeper, outputMetaTable, pythonClass, pramenPyConfig, processRunner))
  }

  def createSink(operationDef: OperationDef,
                 sinkName: String,
                 sinkTables: Seq[SinkTable])
                (implicit spark: SparkSession): Seq[Job] = {
    val sinkBase = SinkManager.getSinkByName(sinkName, conf, None)

    sinkTables.map(sinkTable => {
      val inputTable = metastore.getTableDef(sinkTable.metaTableName)

      val sink = sinkTable.overrideConf match {
        case Some(confOverride) => SinkManager.getSinkByName(sinkName, conf, Some(confOverride))
        case None               => sinkBase
      }

      val outputTableName = sinkTable.outputTableName.getOrElse(s"${sinkTable.metaTableName}->$sinkName")

      val outputTable = inputTable.copy(name = outputTableName)

      new SinkJob(operationDef, metastore, bookkeeper, outputTable, sink, sinkTable)
    })
  }
}
