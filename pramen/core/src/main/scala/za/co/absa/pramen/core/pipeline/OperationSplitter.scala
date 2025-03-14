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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.jobdef.{Schedule, SinkTable, SourceTable, TransferTable}
import za.co.absa.pramen.api.{DataFormat, Transformer}
import za.co.absa.pramen.core.app.config.GeneralConfig.TEMPORARY_DIRECTORY_KEY
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.config.Keys.{SOURCE_SPECIAL_CHARACTERS_IN_COLUMN_NAMES, SPECIAL_CHARACTERS_IN_COLUMN_NAMES}
import za.co.absa.pramen.core.databricks.DatabricksClient
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.notify.NotificationTargetManager
import za.co.absa.pramen.core.pipeline.OperationSplitter.{DISABLE_COUNT_QUERY, getDatabricksClient, getNotificationTarget, getPramenPyCmdlineConfig}
import za.co.absa.pramen.core.pipeline.OperationType._
import za.co.absa.pramen.core.pipeline.PythonTransformationJob._
import za.co.absa.pramen.core.process.ProcessRunner
import za.co.absa.pramen.core.sink.SinkManager
import za.co.absa.pramen.core.source.SourceManager
import za.co.absa.pramen.core.utils.{ClassLoaderUtils, ConfigUtils}

class OperationSplitter(conf: Config,
                        metastore: Metastore,
                        bookkeeper: Bookkeeper,
                        batchId: Long)(implicit spark: SparkSession) {
  private val log = LoggerFactory.getLogger(this.getClass)

  def createJobs(operationDef: OperationDef): Seq[Job] = {
    operationDef.operationType match {
      case Ingestion(sourceName, sourceTables)            => createIngestion(operationDef, sourceName, sourceTables)
      case Transformation(clazz, outputTable)             => createTransformation(operationDef, clazz, outputTable)
      case PythonTransformation(pythonClass, outputTable) => createPythonTransformation(operationDef, pythonClass, outputTable)
      case Sink(sinkName, sinkTables)                     => createSink(operationDef, sinkName, sinkTables)
      case Transfer(sourceName, sinkName, tables)         => createTransfer(operationDef, sourceName, sinkName, tables)
    }
  }

  def createIngestion(operationDef: OperationDef,
                      sourceName: String,
                      sourceTables: Seq[SourceTable])(implicit spark: SparkSession): Seq[Job] = {
    val globalSpecialCharacters = conf.getString(SPECIAL_CHARACTERS_IN_COLUMN_NAMES)
    val temporaryDirectory = ConfigUtils.getOptionString(conf, TEMPORARY_DIRECTORY_KEY)

    sourceTables.map(sourceTable => {
      val source = SourceManager.getSourceByName(sourceName, conf, sourceTable.overrideConf)

      val specialCharacters = ConfigUtils.getOptionString(source.config, SOURCE_SPECIAL_CHARACTERS_IN_COLUMN_NAMES).getOrElse(globalSpecialCharacters)

      val disableCountQuery = ConfigUtils.getOptionBoolean(source.config, DISABLE_COUNT_QUERY).getOrElse(false)
      val outputTable = metastore.getTableDef(sourceTable.metaTableName)

      val notificationTargets = operationDef.notificationTargets
        .map(targetName => getNotificationTarget(conf, targetName, sourceTable.conf))

      if (operationDef.schedule == Schedule.Incremental) {
        val latestOffsets = bookkeeper.getOffsetManager.getMaxInfoDateAndOffset(outputTable.name, None)
        new IncrementalIngestionJob(operationDef, metastore, bookkeeper, notificationTargets, latestOffsets, batchId, sourceName, source, sourceTable, outputTable, specialCharacters)
      } else {
        new IngestionJob(operationDef, metastore, bookkeeper, notificationTargets, sourceName, source, sourceTable, outputTable, specialCharacters, temporaryDirectory, disableCountQuery)
      }
    })
  }

  def createTransfer(operationDef: OperationDef,
                     sourceName: String,
                     sinkName: String,
                     tables: Seq[TransferTable])(implicit spark: SparkSession): Seq[Job] = {
    val specialCharacters = conf.getString(SPECIAL_CHARACTERS_IN_COLUMN_NAMES)
    val temporaryDirectory = ConfigUtils.getOptionString(conf, TEMPORARY_DIRECTORY_KEY)

    tables.map(transferTable => {
      val source = SourceManager.getSourceByName(sourceName, conf, transferTable.sourceOverrideConf)
      val sink = SinkManager.getSinkByName(sinkName, conf, transferTable.sinkOverrideConf)

      val disableCountQuery = ConfigUtils.getOptionBoolean(source.config, DISABLE_COUNT_QUERY).getOrElse(false)
      val outputTable = TransferTableParser.getMetaTable(transferTable)

      val notificationTargets = operationDef.notificationTargets
        .map(targetName => getNotificationTarget(conf, targetName, transferTable.conf))

      new TransferJob(operationDef, metastore, bookkeeper, notificationTargets, sourceName, source, transferTable, outputTable, sinkName, sink, specialCharacters, temporaryDirectory, disableCountQuery)
    })
  }

  def createTransformation(operationDef: OperationDef,
                           clazz: String,
                           outputTable: String)(implicit spark: SparkSession): Seq[Job] = {
    val transformer = ClassLoaderUtils.loadEntityConfigurableClass[Transformer](clazz, operationDef.operationConf, conf)

    val outputMetaTable = metastore.getTableDef(outputTable)

    val notificationTargets = operationDef.notificationTargets
      .map(targetName => getNotificationTarget(conf, targetName, operationDef.operationConf))

    val latestInfoDateOpt = if (operationDef.schedule == Schedule.Incremental) {
      bookkeeper.getOffsetManager.getMaxInfoDateAndOffset(outputTable, None).map(_.maximumInfoDate)
    } else None

    Seq(new TransformationJob(operationDef, metastore, bookkeeper, notificationTargets, outputMetaTable, clazz, transformer, latestInfoDateOpt))
  }

  def createPythonTransformation(operationDef: OperationDef,
                                 pythonClass: String,
                                 outputTable: String)(implicit spark: SparkSession): Seq[Job] = {
    val outputMetaTable = metastore.getTableDef(outputTable)

    val keepLogLines = conf.getInt(KEEP_LOG_LINES_KEY)

    val processRunner = ProcessRunner(keepLogLines,
      stdOutLogPrefix = "Pramen-Py(out)",
      stdErrLogPrefix = "Pramen-Py(err)")

    val databricksClientOpt = getDatabricksClient(conf)
    val pramenPyConfig = getPramenPyCmdlineConfig(conf)

    val notificationTargets = operationDef.notificationTargets
      .map(targetName => getNotificationTarget(conf, targetName, operationDef.operationConf))

    val latestInfoDateOpt = if (operationDef.schedule == Schedule.Incremental) {
      bookkeeper.getOffsetManager.getMaxInfoDateAndOffset(outputTable, None).map(_.maximumInfoDate)
    } else None

    Seq(new PythonTransformationJob(operationDef, metastore, bookkeeper, notificationTargets, outputMetaTable, pythonClass, pramenPyConfig, processRunner, databricksClientOpt, latestInfoDateOpt))
  }

  def createSink(operationDef: OperationDef,
                 sinkName: String,
                 sinkTables: Seq[SinkTable])
                (implicit spark: SparkSession): Seq[Job] = {
    sinkTables.map(sinkTable => {
      val inputTable = metastore.getTableDef(sinkTable.metaTableName)

      val sink = SinkManager.getSinkByName(sinkName, conf, sinkTable.overrideConf)

      val outputTableName = sinkTable.outputTableName.getOrElse(s"${sinkTable.metaTableName}->$sinkName")

      val outputTable = inputTable.copy(name = outputTableName, format = DataFormat.Null(), hiveTable = None)

      val notificationTargets = operationDef.notificationTargets
        .map(targetName => getNotificationTarget(conf, targetName, sinkTable.conf))

      new SinkJob(operationDef, metastore, bookkeeper, notificationTargets, outputTable, sinkName, sink, sinkTable)
    })
  }
}

object OperationSplitter {
  val NOTIFICATION_TARGET_KEY = "notification.target"
  val NOTIFICATION_KEY = "notification"
  val DISABLE_COUNT_QUERY = "disable.count.query"

  private val log = LoggerFactory.getLogger(this.getClass)

  private[core] def getNotificationTarget(appConf: Config,
                                          targetName: String,
                                          tableConf: Config)(implicit sparkSession: SparkSession): JobNotificationTarget = {
    val confOverride = ConfigUtils.getOptionConfig(tableConf, NOTIFICATION_TARGET_KEY)
    val options = ConfigUtils.getExtraOptions(tableConf, NOTIFICATION_KEY)
    val target = NotificationTargetManager.getByName(targetName, appConf, Option(confOverride))
    JobNotificationTarget(targetName, options, target)
  }

  private[core] def getPramenPyCmdlineConfig(conf: Config): Option[PramenPyCmdConfig] = {
    if (conf.hasPath(PRAMEN_PY_LOCATION_KEY)) {
      val pramenPyLocation = conf.getString(PRAMEN_PY_LOCATION_KEY)
      val pramenPyExecutable = conf.getString(PRAMEN_PY_EXECUTABLE_KEY)
      val cmdLineTemplate = conf.getString(PRAMEN_PY_CMD_LINE_TEMPLATE_KEY)

      val pramenPyConfig = PramenPyCmdConfig(pramenPyLocation, pramenPyExecutable, cmdLineTemplate)
      Some(pramenPyConfig)
    } else {
      log.info(s"Could not create command line config for Pramen-Py. Missing '$PRAMEN_PY_LOCATION_KEY' option.")
      None
    }
  }

  private[core] def getDatabricksClient(conf: Config): Option[DatabricksClient] = {
    if (DatabricksClient.canCreate(conf)) {
      val client = DatabricksClient.fromConfig(conf)
      Some(client)
    } else {
      log.info("Could not create databricks client for Pramen-Py. Missing mandatory options.")
      None
    }
  }
}
