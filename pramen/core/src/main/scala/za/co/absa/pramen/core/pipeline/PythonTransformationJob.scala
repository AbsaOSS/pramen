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
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{DataFormat, Reason}
import za.co.absa.pramen.core.app.config.GeneralConfig.TEMPORARY_DIRECTORY_KEY
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.databricks.{DatabricksClient, PramenPyJobTemplate}
import za.co.absa.pramen.core.exceptions.ProcessFailedException
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.MetastoreImpl.DEFAULT_RECORDS_PER_PARTITION
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.pipeline.PythonTransformationJob._
import za.co.absa.pramen.core.process.ProcessRunner
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.core.utils.StringUtils.escapeString

import java.io.{BufferedWriter, File, FileWriter}
import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

object PythonTransformationJob {
  val PRAMEN_PY_CMD_LINE_TEMPLATE_KEY = "pramen.py.cmd.line.template"
  val PRAMEN_PY_LOCATION_KEY = "pramen.py.location"
  val PRAMEN_PY_EXECUTABLE_KEY = "pramen.py.executable"

  val KEEP_LOG_LINES_KEY = "pramen.py.keep.log.lines"

  val MINIMUM_RECORDS_OPTION = "minimum.records"

  val LOCATION_VAR = "@location"
  val EXECUTABLE_VAR = "@executable"
  val PYTHON_CLASS_VAR = "@pythonClass"
  val METASTORE_CONFIG_VAR = "@metastoreConfig"
  val INFO_DATE_VAR = "@infoDate"

}

class PythonTransformationJob(operationDef: OperationDef,
                              metastore: Metastore,
                              bookkeeper: Bookkeeper,
                              notificationTargets: Seq[JobNotificationTarget],
                              outputTable: MetaTable,
                              pythonClass: String,
                              pramenPyCmdConfigOpt: Option[PramenPyCmdConfig],
                              processRunner: ProcessRunner,
                              databricksClientOpt: Option[DatabricksClient])
                             (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper,notificationTargets, outputTable) {

  private val minimumRecords: Int = operationDef.extraOptions.getOrElse(MINIMUM_RECORDS_OPTION, "0").toInt

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    validateTransformationAlreadyRanCases(infoDate, dependencyWarnings) match {
      case Some(result) => result
      case None => JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings, Seq.empty[String])
    }
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    if (outputTable.format.isTransient) {
      Reason.NotReady(s"Python transformations cannot output to transient tables. Please, change teh format of ${outputTable.name}")
    } else {
      Reason.Ready
    }
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    if (pramenPyCmdConfigOpt.isDefined)
      runPythonCmdLine(pramenPyCmdConfigOpt.get, infoDate, conf)
    else if (databricksClientOpt.isDefined)
      runPythonOnDatabricks(databricksClientOpt.get, infoDate, conf)
    else
      throw new RuntimeException("Neither command line options nor databricks client configured correctly for Pramen-Py.")

    try {
      RunResult(metastore.getTable(outputTable.name, Option(infoDate), Option(infoDate)))
    } catch {
      case ex: AnalysisException => throw new RuntimeException(s"Output data not found in the metastore for $infoDate", ex)
    }
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    df
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    // Data already saved by Pramen-Py. Just loading the table and getting stats
    val stats = try {
      metastore.getStats(outputTable.name, infoDate)
    } catch {
      case ex: AnalysisException => throw new RuntimeException(s"Output data not found in the metastore for $infoDate", ex)
    }

    if (stats.recordCount == 0 && minimumRecords > 0) {
      throw new RuntimeException(s"Output table is empty in the metastore for $infoDate")
    }

    if (stats.recordCount < minimumRecords) {
      throw new RuntimeException(s"The transformation returned too few records (${stats.recordCount} < $minimumRecords).")
    }

    val jobFinished = Instant.now()

    bookkeeper.setRecordCount(outputTable.name,
      infoDate,
      infoDate,
      infoDate,
      stats.recordCount,
      stats.recordCount,
      jobStarted.getEpochSecond,
      jobFinished.getEpochSecond,
      isTableTransient = false)

    SaveResult(stats)
  }

  private[core] def runPythonCmdLine(pramenPyCmdConfig: PramenPyCmdConfig, infoDate: LocalDate, conf: Config): Unit = {
    val metastoreConfigLocation = getMetastoreConfig(infoDate, conf)

    log.info(s"Using template: ${pramenPyCmdConfig.cmdLineTemplate}")
    val cmd = pramenPyCmdConfig.cmdLineTemplate
      .replace(LOCATION_VAR, pramenPyCmdConfig.location)
      .replace(EXECUTABLE_VAR, pramenPyCmdConfig.executable)
      .replace(PYTHON_CLASS_VAR, pythonClass)
      .replace(METASTORE_CONFIG_VAR, metastoreConfigLocation)
      .replace(INFO_DATE_VAR, infoDate.toString)

    val exitCode = try {
      processRunner.run(cmd)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"The process has exited with an exception.", ex)
    }

    if (exitCode != 0)
      throw ProcessFailedException(s"The process has exited with error code $exitCode.", processRunner.getLastStdoutLines, processRunner.getLastStderrLines)
  }

  private[core] def runPythonOnDatabricks(databricksClient: DatabricksClient, infoDate: LocalDate, conf: Config): Unit = {
    val metastoreConfig = getYamlConfig(infoDate)
    val configPath = getTemporaryPathForYamlConfig(conf)

    val jobDefinition = PramenPyJobTemplate.render(conf, pythonClass, configPath, infoDate)

    try {
      databricksClient.createFile(metastoreConfig, configPath, overwrite = true)
      databricksClient.runTransientJob(jobDefinition)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"The Databricks job has failed.", ex)
    }
  }

  private[core] def getMetastoreConfig(infoDate: LocalDate, conf: Config): String = {
    val tempFile = File.createTempFile("metastore", ".yaml")

    tempFile.deleteOnExit()

    val bw = new BufferedWriter(new FileWriter(tempFile))

    try {
      val yaml = getYamlConfig(infoDate)
      log.info(s"Pramen-Py config:\n$yaml")
      bw.write(yaml)
    } finally {
      bw.close()
    }

    tempFile.getAbsolutePath
  }

  private[core] def getYamlConfig(infoDate: LocalDate): String = {
    def addTransformation(): String = {
      val sparkConfig = addOptions(operationDef.sparkConfig, "spark_config")
      val options = addOptions(operationDef.extraOptions, "options")

      s"""run_transformers:
         |- info_date: $infoDate
         |  output_table: ${outputTable.name}
         |  name: $pythonClass
         |$sparkConfig
         |$options
         |""".stripMargin
    }

    def addOptions(options: Map[String, String], key: String): String = {
      if (options.isEmpty) {
        s"  $key: {}"
      } else {
        val opts = options
          .toArray
          .sortBy(_._1)
          .map {
            case (key, value) =>
              val k = escapeString(key)
              val v = escapeString(value)
              s"    $k: $v"
          }
          .sorted
          .mkString("\n")
        s"  $key:\n$opts"
      }
    }

    def addMetastore(): String = {
      metastore.getRegisteredMetaTables
        .map(getTable)
        .mkString("metastore_tables:\n", "\n", "")
    }

    def getTable(mt: MetaTable): String = {
      val description = if (mt.description.isEmpty) "" else s"\n  description: ${escapeString(mt.description)}"
      val recordsPerPartition = mt.format match {
        case f: DataFormat.Parquet => s"\n  records_per_partition: ${f.recordsPerPartition.getOrElse(DEFAULT_RECORDS_PER_PARTITION)}"
        case f: DataFormat.Delta => s"\n  records_per_partition: ${f.recordsPerPartition.getOrElse(DEFAULT_RECORDS_PER_PARTITION)}"
        case _ => ""
      }

      val path = mt.format match {
        case f: DataFormat.Parquet => s"\n  path: ${f.path}"
        case f: DataFormat.Delta => s"\n  path: ${f.query.query}"
        case _ => ""
      }

      val readOptions = addOptions(mt.readOptions, "reader_options")
      val writeOptions = addOptions(mt.writeOptions, "writer_options")

      s"""- name: ${escapeString(mt.name)}$description
         |  format: ${mt.format.name}$path$recordsPerPartition
         |  info_date_settings:
         |    column: ${escapeString(mt.infoDateColumn)}
         |    format: ${escapeString(mt.infoDateFormat)}
         |    start: ${mt.infoDateStart.toString}
         |$readOptions
         |$writeOptions""".stripMargin
    }

    val sb = new mutable.StringBuilder()

    sb.append(addTransformation())
    sb.append(addMetastore())

    sb.toString
  }

  private[core] def getTemporaryPathForYamlConfig(conf: Config) = {
    val temporaryDirectoryBase = if (conf.hasPath(TEMPORARY_DIRECTORY_KEY) && conf.getString(TEMPORARY_DIRECTORY_KEY).nonEmpty) {
      conf.getString(TEMPORARY_DIRECTORY_KEY).stripSuffix("/")
    } else {
      throw new IllegalArgumentException(s"Python transformation require temporary directory to be defined at: $TEMPORARY_DIRECTORY_KEY")
    }

    val randomNumber = Random.nextInt(1000000)

    val pathForConfig = s"$temporaryDirectoryBase/pramen_py_configs/$randomNumber/config.yaml"
      .stripPrefix("/dbfs")
      .stripPrefix("dbfs:")

    s"dbfs:$pathForConfig"
  }
}
