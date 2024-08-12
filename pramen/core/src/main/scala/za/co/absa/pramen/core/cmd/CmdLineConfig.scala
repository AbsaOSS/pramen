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

package za.co.absa.pramen.core.cmd

import com.typesafe.config.{Config, ConfigValueFactory}
import scopt.OptionParser
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.app.config.InfoDateConfig.TRACK_DAYS
import za.co.absa.pramen.core.app.config.RuntimeConfig._
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.config.Keys.LOG_EFFECTIVE_CONFIG

import java.time.LocalDate
import scala.collection.JavaConverters.asJavaIterableConverter

case class CmdLineConfig(
                          configPathNames: Seq[String] = Seq.empty,
                          files: Seq[String] = Seq.empty[String],
                          operations: Seq[String] = Seq.empty[String],
                          currentDate: Option[LocalDate] = None,
                          rerunInfoDate: Option[LocalDate] = None,
                          checkOnlyLateData: Option[Boolean] = None,
                          checkOnlyNewData: Option[Boolean] = None,
                          parallelTasks: Option[Int] = None,
                          dryRun: Option[Boolean] = None,
                          useLock: Option[Boolean] = None,
                          undercover: Option[Boolean] = None,
                          dateFrom: Option[LocalDate] = None,
                          dateTo: Option[LocalDate] = None,
                          mode: Option[String] = None,
                          inverseOrder: Option[Boolean] = None,
                          verbose: Option[Boolean] = None,
                          overrideLogLevel: Option[String] = None,
                          logEffectiveConfig: Option[Boolean] = None
                        )

object CmdLineConfig {
  private val infoDateFormatter = InfoDateConfig.defaultDateFormatter

  def apply(args: Array[String]): CmdLineConfig = {
    val optionCmd = parseCmdLine(args)
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  def parseCmdLine(args: Array[String]): Option[CmdLineConfig] = {
    val parser = new CmdParser("spark-submit pipeline-runner.jar za.co.absa.pramen.core.runner.PipelineRunner")

    parser.parse(args, CmdLineConfig())
  }

  def applyCmdLineToConfig(conf: Config, cmd: CmdLineConfig): Config = {
    val dateFormatter = InfoDateConfig.defaultDateFormatter
    var accumulatedConfig = conf

    if (cmd.operations.nonEmpty)
      accumulatedConfig = accumulatedConfig
        .withValue(RUN_TABLES, ConfigValueFactory.fromIterable(cmd.operations.asJava))

    for (infoDate <- cmd.rerunInfoDate)
      accumulatedConfig = accumulatedConfig
        .withValue(IS_RERUN, ConfigValueFactory.fromAnyRef(true))
        .withValue(CURRENT_DATE, ConfigValueFactory.fromAnyRef(dateFormatter.format(infoDate)))

    for (date <- cmd.currentDate)
      accumulatedConfig = accumulatedConfig
        .withValue(CURRENT_DATE, ConfigValueFactory.fromAnyRef(dateFormatter.format(date)))

    for (checkLateOnly <- cmd.checkOnlyLateData)
      accumulatedConfig = accumulatedConfig
        .withValue(CHECK_ONLY_LATE_DATA, ConfigValueFactory.fromAnyRef(checkLateOnly))

    for (checkNewOnly <- cmd.checkOnlyNewData)
      accumulatedConfig = accumulatedConfig
        .withValue(CHECK_ONLY_NEW_DATA, ConfigValueFactory.fromAnyRef(checkNewOnly))

    for (dateFrom <- cmd.dateFrom)
      accumulatedConfig = accumulatedConfig
        .withValue(LOAD_DATE_FROM, ConfigValueFactory.fromAnyRef(dateFormatter.format(dateFrom)))

    for (dateTo <- cmd.dateTo)
      accumulatedConfig = accumulatedConfig
        .withValue(LOAD_DATE_TO, ConfigValueFactory.fromAnyRef(dateFormatter.format(dateTo)))
        .withValue(CURRENT_DATE, ConfigValueFactory.fromAnyRef(dateFormatter.format(dateTo)))
        .withValue(TRACK_DAYS, ConfigValueFactory.fromAnyRef(0))

    for (undercover <- cmd.undercover)
      accumulatedConfig = accumulatedConfig.withValue(UNDERCOVER, ConfigValueFactory.fromAnyRef(undercover))

    for (dryRun <- cmd.dryRun)
      accumulatedConfig = accumulatedConfig.withValue(DRY_RUN, ConfigValueFactory.fromAnyRef(dryRun))

    for (useLock <- cmd.useLock)
      accumulatedConfig = accumulatedConfig.withValue(USE_LOCK, ConfigValueFactory.fromAnyRef(useLock))

    for (inverseOrder <- cmd.inverseOrder)
      accumulatedConfig = accumulatedConfig.withValue(IS_INVERSE_ORDER, ConfigValueFactory.fromAnyRef(inverseOrder))

    for (verbose <- cmd.verbose)
      accumulatedConfig = accumulatedConfig.withValue(VERBOSE, ConfigValueFactory.fromAnyRef(verbose))

    for (parallelTasks <- cmd.parallelTasks)
      accumulatedConfig = accumulatedConfig.withValue(Keys.PARALLEL_TASKS, ConfigValueFactory.fromAnyRef(parallelTasks))

    for (mode <- cmd.mode)
      accumulatedConfig = accumulatedConfig.withValue(RUN_MODE, ConfigValueFactory.fromAnyRef(mode))

    for (logEffectiveConfig <- cmd.logEffectiveConfig)
      accumulatedConfig = accumulatedConfig.withValue(LOG_EFFECTIVE_CONFIG, ConfigValueFactory.fromAnyRef(logEffectiveConfig))

    accumulatedConfig
  }

  private class CmdParser(programName: String) extends OptionParser[CmdLineConfig](programName) {
    head("\nPramen Workflow Runner")

    opt[String]("workflow").optional().action((value, config) =>
      config.copy(configPathNames = Seq(value)))
      .text("Path to a workflow configuration")

    opt[String]("workflows").optional()
      .action((value, config) =>
        if (config.configPathNames.nonEmpty) {
          throw new IllegalArgumentException(s"Cannot use both --workflow and --workflows options together")
        } else {
          config.copy(configPathNames = value.split(',').map(_.trim))
        }
      )
      .text("Path to a comma separated list of workflow configurations")

    checkConfig(c =>
      if (c.configPathNames.isEmpty) {
        failure("Either --workflow or --workflows option must be specified")
      } else {
        success
      }
    )

    opt[String]("files").optional().action((value, config) =>
      config.copy(files = value.split(',').map(_.trim)))
      .text("A comma-separated list of files to get from HDFS or S3 (use Hadoop prefix s3a://, hdfs://). " +
        "Files are fetched to the local current directory before reading workflow configuration.")

    opt[String]("ops").optional().action((value, config) =>
      config.copy(operations = value.split(',').map(_.trim)))
      .text("A comma-separated list of output table names that correspond to operations to run.")

    opt[Int]("parallel-tasks").optional().action((value, config) =>
      config.copy(parallelTasks = Option(value)))
      .text("Maximum number of parallel tasks.")
      .validate { v =>
        if (v > 0) success
        else failure("Invalid number of parallel tasks. Must be greater than 0.")
      }

    opt[String]("date").optional().action((value, config) =>
      config.copy(currentDate = Option(LocalDate.parse(value, infoDateFormatter))))
      .text("Override the current date (the date the job is running)")

    opt[String]("date-from").optional().action((value, config) =>
      config.copy(dateFrom = Option(LocalDate.parse(value, infoDateFormatter))))
      .text("A date to start loading data from")

    opt[String]("date-to").optional().action((value, config) =>
      config.copy(dateTo = Option(LocalDate.parse(value, infoDateFormatter))))
      .text("A date to finish loading data to")
      .children(
        opt[String]("run-mode").optional().action((value, config) =>
          config.copy(mode = Option(value)))
          .text("Mode of processing for date ranges. One of 'fill_gaps', 'check_updates', 'force'")
          .validate(v =>
            if (v == "fill_gaps" || v == "check_updates" || v == "force") success
            else failure("Invalid run mode. Must be one of 'fill_gaps', 'check_updates', 'force'"))
      )

    opt[Boolean]("inverse-order").optional().action((value, config) =>
      config.copy(inverseOrder = Option(value)))
      .text("If true, ranged runs will be done in inverse order (from newest to oldest)")

    opt[String]("rerun").optional().action((value, config) =>
      config.copy(rerunInfoDate = Option(LocalDate.parse(value, infoDateFormatter))))
      .text("Information date to recompute (if specified all jobs in the workflow will be re-run)")

    opt[Unit]("dry-run").optional().action((_, config) =>
      config.copy(dryRun = Some(true)))
      .text("If true, no actual data processing will be done.")

    opt[Boolean]("use-lock").optional().action((value, config) =>
      config.copy(useLock = Option(value)))
      .text("If true (default) a lock is used when writing to a table")

    opt[Unit]("undercover").optional().action((_, config) =>
      config.copy(undercover = Some(true)))
      .text("If true, no updates will be done to the bookkeeping data (Ensure you are know what you are doing!)")

    opt[Unit]("check-late-only").optional().action((_, config) =>
      config.copy(checkOnlyLateData = Some(true)))
      .text("When specified, only late data from the source will be checked. No checks for new on-time data will be performed.")

    opt[Unit]("check-new-only").optional().action((_, config) =>
      config.copy(checkOnlyNewData = Some(true)))
      .text("When specified, only new data from the source will be checked. The 'track.days' option will be ignored.")

    opt[Unit]("verbose").optional().action((_, config) =>
      config.copy(verbose = Some(true)))
      .text("When specified, more detailed logs will be generated.")

    opt[String]("override-log-level").optional().action((value, config) =>
      config.copy(overrideLogLevel = Option(value)))
      .text("Override environment configured root log level.")

    opt[Boolean]("log-config").optional().action((value, config) =>
        config.copy(logEffectiveConfig = Option(value)))
      .text("When true (default), Pramen logs the effective configuration.")

    help("help").text("prints this usage text")
  }

}
