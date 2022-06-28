/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.sink

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink}
import za.co.absa.pramen.framework.exceptions.CmdFailedException
import za.co.absa.pramen.framework.process.{ProcessRunner, ProcessRunnerImpl}
import za.co.absa.pramen.framework.sink.CmdLineSink.CMD_LINE_KEY
import za.co.absa.pramen.framework.utils.{ConfigUtils, FsUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal

/**
  * This sink allows sending data (actually, the path to data) to an command line tool.
  *
  * Mandatory options:
  *  - 'temp.hadoop.path' is used to prepare data.
  *  - 'format' ('parquet' by default).
  *
  * Additional options:
  *  - 'include.log.lines' the number of log lines to include in the notification (1000 by default).
  *
  * Options that have 'option' prefix will be passed to the writer.
  *
  * Otherwise, the data can be accessed by the command line tool directly from the metastore.
  *
  * Example sink definition:
  * {{{
  *  {
  *    name = "cmd_line"
  *    factory.class = "za.co.absa.pramen.framework.sink.CmdLineSink"
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
class CmdLineSink(processRunner: ProcessRunner,
                  tempHadoopPath: String,
                  format: String,
                  formatOptions: Map[String, String]) extends Sink {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])
                   (implicit spark: SparkSession): Long = {
    if (!options.contains(CMD_LINE_KEY)) {
      throw new IllegalArgumentException(s"Missing required parameter: 'output.$CMD_LINE_KEY'.")
    }

    val cmdLineTemplate = options(CMD_LINE_KEY)

    val count = df.count()

    if (count > 0) {
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempHadoopPath)

      fsUtils.withTempDirectory(new Path(tempHadoopPath)) { tempPath =>
        df.write
          .format(format)
          .mode(SaveMode.Overwrite)
          .options(formatOptions)
          .save(tempPath.toString)

        log.info(s"$count records saved to $tempPath.")

        val cmdLine = getCmdLine(cmdLineTemplate, tempPath, infoDate)

        runCmd(cmdLine)

        log.info(s"$count records sent to the cmd line sink ($cmdLine).")
      }

      count
    } else {
      log.info(s"Notting to send to $cmdLineTemplate.")
      0L
    }
  }

  private[framework] def getCmdLine(cmdLineTemplate: String,
                                    dataPath: Path,
                                    infoDate: LocalDate): String = {
    log.info(s"CmdLine template: $cmdLineTemplate")

    cmdLineTemplate.replace("@infoDate", infoDate.toString)
      .replace("@infoMonth", infoDate.format(DateTimeFormatter.ofPattern("yyyy-MM")))
      .replace("@dataPath", dataPath.toString)
      .replace("@dataUri", dataPath.toUri.toString)
  }

  private[framework] def runCmd(cmdLine: String): Unit = {
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
  val TEMP_HADOOP_PATH_KEY = "temp.hadoop.path"
  val FORMAT_KEY = "format"
  val INCLUDE_LOG_LINES = "include.log.lines"

  val CMD_LINE_KEY = "cmd.line"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): CmdLineSink = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(TEMP_HADOOP_PATH_KEY, FORMAT_KEY))

    val tempPath = conf.getString(TEMP_HADOOP_PATH_KEY)
    val format = conf.getString(FORMAT_KEY)
    val includeLogLines = ConfigUtils.getOptionInt(conf, INCLUDE_LOG_LINES).getOrElse(1000)
    val extraOptions = ConfigUtils.getExtraOptions(conf, "option")

    val runner = new ProcessRunnerImpl(includeLogLines, true, true, "Cmd", "Cmd", true)

    new CmdLineSink(runner, tempPath, format, extraOptions)
  }
}
