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

package za.co.absa.pramen.core.sink

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{DataFormat, ExternalChannelFactory, MetaTableDef, MetastoreReader, Query, Sink, SinkResult}
import za.co.absa.pramen.core.exceptions.CmdFailedException
import za.co.absa.pramen.core.process.{ProcessRunner, ProcessRunnerImpl}
import za.co.absa.pramen.core.sink.CmdLineSink.{CMD_LINE_KEY, CmdLineDataParams}
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils, SparkUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal

/**
  * This sink allows sending data (actually, the path to data) to an command line tool.
  *
  * If you want to prepare data for the sink in a temporary Hadoop location set these options:
  *  - 'temp.hadoop.path' is used to prepare data.
  *  - 'format' ('parquet' by default).
  *
  * If you don't want data to be prepared, you need to define a RegEx expression to get the number of items written:
  *  - 'record.count.regex' An expression to extract the number of records written successfully
  *  - 'zero.records.success.regex' An expression for the success and zero records written
  *    (if it is different from 'record.count.regex')
  *  - 'failure.regex' An expression for the explicit failure (in addition to non-zero status code)
  *
  * Additional options:
  *  - 'include.log.lines' the number of log lines to include in the notification (1000 by default).
  *  - 'output.filter.regex' A list of RegEx expressions to filter the output and not include in log files.
  *
  * Options that have 'option' prefix will be passed to the writer.
  *
  * Otherwise, the data can be accessed by the command line tool directly from the metastore.
  *
  * ==Example sink definition:==
  * {{{
  *  {
  *    name = "cmd_line"
  *    factory.class = "za.co.absa.pramen.core.sink.CmdLineSink"
  *
  *    temp.hadoop.path = "/tmp/cmd_line_sink"
  *    format = "csv"
  *
  *    include.log.lines = 1000
  *
  *    option {
  *      sep = "|"
  *      quoteAll = "false"
  *      header = "true"
  *    }
  *  }
  * }}}
  *
  * Here is an example of a sink definition in a pipeline. As for any other operation you can specify
  * dependencies, transformations, filters and columns to select.
  *
  * ==Example operation:==
  * {{{
  *  {
  *    name = "Command Line sink"
  *    type = "sink"
  *    sink = "cmd_line"
  *
  *    schedule.type = "daily"
  *
  *    dependencies = [
  *      {
  *        tables = [ dependent_table ]
  *        date.from = "@infoDate"
  *      }
  *    ]
  *
  *    tables = [
  *      {
  *        input.metastore.table = metastore_table
  *        output.cmd.line = "/my_apps/cmd_line_tool --path @dataPath --date @infoDate"
  *
  *        # Date range to read the source table for. By default the job information date is used.
  *        # But you can define an arbitrary expression based on the information date.
  *        # More: see the section of documentation regarding date expressions, an the list of functions allowed.
  *        date {
  *          from = "@infoDate"
  *          to = "@infoDate"
  *        }
  *
  *        transformations = [
  *         { col = "col1", expr = "lower(some_string_column)" }
  *        ],
  *        filters = [
  *          "some_numeric_column > 100"
  *        ]
  *        columns = [ "col1", "col2", "col2", "some_numeric_column" ]
  *      }
  *    ]
  *  }
  * }}}
  *
  */
class CmdLineSink(sinkConfig: Config,
                  val processRunner: ProcessRunner,
                  val dataParams: Option[CmdLineDataParams]
                 ) extends Sink {
  private val log = LoggerFactory.getLogger(this.getClass)


  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])
                   (implicit spark: SparkSession): SinkResult = {
    if (!options.contains(CMD_LINE_KEY)) {
      throw new IllegalArgumentException(s"Missing required parameter: 'output.$CMD_LINE_KEY'.")
    }

    val cmdLineTemplate = options(CMD_LINE_KEY)

    dataParams match {
      case Some(d) =>
        val count = df.count()

        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, d.tempHadoopPath)

        fsUtils.withTempDirectory(new Path(d.tempHadoopPath)) { tempPath =>
          df.write
            .format(d.format)
            .mode(SaveMode.Overwrite)
            .options(d.formatOptions)
            .save(tempPath.toString)

          log.info(s"$count records saved to $tempPath.")

          val cmdLine = getCmdLine(cmdLineTemplate, Option(tempPath), Option(tempPath), infoDate)

          runCmd(cmdLine)

          log.info(s"$count records sent to the cmd line sink ($cmdLine).")
        }
        SinkResult(count)
      case None    =>
        val metaTable = metastore.getTableDef(tableName)
        val (dataPath, partitionPath) = getPaths(metaTable, infoDate)

        val cmdLine = getCmdLine(cmdLineTemplate, dataPath, partitionPath, infoDate)

        runCmd(cmdLine)

        val count = processRunner.recordCount.getOrElse(0L)

        log.info(s"$count records sent to the cmd line sink ($cmdLine).")
        SinkResult(count)
    }
  }

  private[core] def getPaths(metaTable: MetaTableDef, infoDate: LocalDate): (Option[Path], Option[Path]) = {
    val basePathOpt = metaTable.format match {
      case DataFormat.Parquet(path, _) =>
        Option(path)
      case DataFormat.Delta(query, _) =>
        query match {
          case Query.Path(path) =>
            Option(path)
          case _ => None
        }
      case _ =>
        None
    }

    basePathOpt match {
      case Some(basePath) =>
        (Option(new Path(basePath)), Option(SparkUtils.getPartitionPath(infoDate, metaTable.infoDateColumn, metaTable.infoDateFormat, basePath)))
      case None =>
        (None, None)
    }
  }

  private[core] def getCmdLine(cmdLineTemplate: String,
                               dataPath: Option[Path],
                               partitionPath: Option[Path],
                               infoDate: LocalDate): String = {
    log.info(s"CmdLine template: $cmdLineTemplate")

    val cmdWithDates = cmdLineTemplate.replace("@infoDate", infoDate.toString)
      .replace("@infoMonth", infoDate.format(DateTimeFormatter.ofPattern("yyyy-MM")))

    val cmdWithDataPath = dataPath match {
      case Some(path) =>
        if (Option(path.toUri.getAuthority).isDefined) {
          val bucket = path.toUri.getAuthority
          val prefixOrg = path.toUri.getPath
          val prefix = if (prefixOrg.startsWith("/", 0))
            prefixOrg.substring(1)
          else
            prefixOrg

          cmdWithDates
            .replace("@bucket", bucket)
            .replace("@prefix", prefix)
            .replace("@dataPath", path.toString)
            .replace("@dataUri", path.toUri.toString)
        } else {
          cmdWithDates.replace("@dataPath", path.toString)
            .replace("@dataUri", path.toUri.toString)
        }
      case None       =>
        cmdWithDates
    }

    partitionPath match {
      case Some(path) =>
        if (Option(path.toUri.getAuthority).isDefined) {
          val bucket = path.toUri.getAuthority
          val prefixOrg = path.toUri.getPath
          val prefix = if (prefixOrg.startsWith("/", 0))
            prefixOrg.substring(1)
          else
            prefixOrg

          cmdWithDataPath
            .replace("@bucket", bucket)
            .replace("@partitionPrefix", prefix)
            .replace("@partitionPath", path.toString)
        } else {
          cmdWithDataPath.replace("@partitionPath", path.toString)
        }
      case None       =>
        cmdWithDataPath
    }
  }

  private[core] def runCmd(cmdLine: String): Unit = {
    val exitCode = try {
      processRunner.run(cmdLine)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"The process has exited with an exception.", ex)
    }

    if (exitCode != 0)
      throw CmdFailedException(s"The process has exited with error code $exitCode.", processRunner.getLastStdoutLines)
  }
}

object CmdLineSink extends ExternalChannelFactory[CmdLineSink] {
  case class CmdLineDataParams(
                                tempHadoopPath: String,
                                format: String,
                                formatOptions: Map[String, String]
                              )

  val TEMP_HADOOP_PATH_KEY = "temp.hadoop.path"
  val FORMAT_KEY = "format"
  val INCLUDE_LOG_LINES = "include.log.lines"
  val CMD_LINE_KEY = "cmd.line"

  val RECORD_COUNT_REGEX_KEY = "record.count.regex"
  val ZERO_RECORDS_SUCCESS_REGEX_KEY = "zero.records.success.regex"
  val FAILURE_REGEX_KEY = "failure.regex"
  val OUTPUT_FILTER_REGEX_KEY = "output.filter.regex"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): CmdLineSink = {
    val dataParams = if (conf.hasPath(TEMP_HADOOP_PATH_KEY)) {
      ConfigUtils.validatePathsExistence(conf, parentPath, Seq(TEMP_HADOOP_PATH_KEY, FORMAT_KEY))
      val extraOptions = ConfigUtils.getExtraOptions(conf, "option")

      Some(CmdLineDataParams(
        conf.getString(TEMP_HADOOP_PATH_KEY),
        conf.getString(FORMAT_KEY),
        extraOptions
      ))
    } else {
      None
    }

    val includeLogLines = ConfigUtils.getOptionInt(conf, INCLUDE_LOG_LINES).getOrElse(1000)
    val recordCountRegex = ConfigUtils.getOptionString(conf, RECORD_COUNT_REGEX_KEY)
    val zeroRecordsSuccessRegex = ConfigUtils.getOptionString(conf, ZERO_RECORDS_SUCCESS_REGEX_KEY)
    val failureRegex = ConfigUtils.getOptionString(conf, FAILURE_REGEX_KEY)
    val outputFilterRegex = ConfigUtils.getOptListStrings(conf, OUTPUT_FILTER_REGEX_KEY)


    val runner = new ProcessRunnerImpl(includeLogLines,
      true,
      true,
      "Cmd",
      "Cmd",
      true,
      recordCountRegex,
      zeroRecordsSuccessRegex,
      failureRegex,
      outputFilterRegex
    )

    new CmdLineSink(conf, runner, dataParams)
  }
}
