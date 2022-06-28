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

package za.co.absa.pramen.runner.cmd

import com.typesafe.config.{Config, ConfigValueFactory}
import scopt.OptionParser
import za.co.absa.pramen.framework.app.config.InfoDateConfig.TRACK_DAYS
import za.co.absa.pramen.framework.app.config.RuntimeConfig._
import za.co.absa.pramen.framework.model.Constants
import za.co.absa.pramen.framework.model.Constants.DATE_FORMAT_INTERNAL

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters.asJavaIterableConverter

case class CmdLineConfig(
                          configPathName: String = "",
                          files: Seq[String] = Seq.empty[String],
                          operations: Seq[String] = Seq.empty[String],
                          currentDate: Option[LocalDate] = None,
                          rerunInfoDate: Option[LocalDate] = None,
                          checkOnlyLateData: Option[Boolean] = None,
                          checkOnlyNewData: Option[Boolean] = None,
                          dryRun: Option[Boolean] = None,
                          useLock: Option[Boolean] = None,
                          undercover: Option[Boolean] = None,
                          dateFrom: Option[LocalDate] = None,
                          dateTo: Option[LocalDate] = None,
                          mode: String = "",
                          trackUpdates: Option[Boolean] = None,
                          inverseOrder: Option[Boolean] = None,
                          tableNum: Option[Int] = None
                        )

object CmdLineConfig {
  private val infoDateFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT_INTERNAL)

  def apply(args: Array[String]): CmdLineConfig = {
    val optionCmd = parseCmdLine(args)
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  def parseCmdLine(args: Array[String]): Option[CmdLineConfig] = {
    val parser = new CmdParser("spark-submit pipeline-runner.jar " +
      "za.co.absa.pramen.syncrunner.SyncWatcherRunner " +
      "--workflow <Workflow Configuration Path> " +
      "[--files <comma-separated list of files> " +
      "[--ops output_table1,output_table2,...]" +
      "[--date <Current date override (yyyy-MM-dd)>] " +
      "[--rerun <The date to force rerun (yyyy-MM-dd)>] " +
      "[--check-late-only]" +
      "[--check-new-only] " +
      "[--dry-run <true/false>] " +
      "[--use-lock <true/false>] " +
      "[--undercover <true/false>] " +
      "[--date-from <date_from>]" +
      "[--date-to <date_to>]" +
      "[--run-mode { fill_gaps | check_updates | force }]" +
      "[--inverse-order <true/false>]" +
      "[--track-updates <true/false>]"
    )

    parser.parse(args, CmdLineConfig())
  }

  def applyCmdLineToConfig(conf: Config, cmd: CmdLineConfig): Config = {
    val dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_FORMAT_INTERNAL)

    val conf2 = if (cmd.operations.nonEmpty) {
      conf.withValue(RUN_TABLES, ConfigValueFactory.fromIterable(cmd.operations.asJava))
    } else conf

    val conf3 = cmd.rerunInfoDate match {
      case Some(infoDate) =>
        conf2
          .withValue(IS_RERUN, ConfigValueFactory.fromAnyRef(true))
          .withValue(CURRENT_DATE, ConfigValueFactory.fromAnyRef(dateFormatter.format(infoDate)))
      case None           => conf2
    }

    val conf4 = cmd.currentDate match {
      case Some(date) => conf3
        .withValue(CURRENT_DATE, ConfigValueFactory.fromAnyRef(dateFormatter.format(date)))
      case None       => conf3
    }

    val conf5 = cmd.checkOnlyLateData match {
      case Some(checkLateOnly) => conf4.withValue(CHECK_ONLY_LATE_DATA,
        ConfigValueFactory.fromAnyRef(checkLateOnly))
      case None                => conf4
    }

    val conf6 = cmd.checkOnlyNewData match {
      case Some(checkNewOnly) => conf5.withValue(CHECK_ONLY_NEW_DATA,
        ConfigValueFactory.fromAnyRef(checkNewOnly))
      case None               => conf5
    }

    val conf7 = cmd.dateFrom match {
      case Some(dateFrom) => conf6
        .withValue(LOAD_DATE_FROM,
          ConfigValueFactory.fromAnyRef(dateFormatter.format(dateFrom)))
      case None           => conf6
    }

    val conf8 = cmd.dateTo match {
      case Some(dateTo) => conf7
        .withValue(LOAD_DATE_TO,
          ConfigValueFactory.fromAnyRef(dateFormatter.format(dateTo)))
        .withValue(CURRENT_DATE,
          ConfigValueFactory.fromAnyRef(dateFormatter.format(dateTo)))
        .withValue(TRACK_DAYS,
          ConfigValueFactory.fromAnyRef(0))
      case None         => conf7
    }

    val conf9 = cmd.trackUpdates match {
      case Some(trackUpdates) => conf8
        .withValue(TRACK_UPDATES,
          ConfigValueFactory.fromAnyRef(trackUpdates))
      case None               => conf8
    }

    val conf10 = cmd.undercover match {
      case Some(v) => conf9.withValue(UNDERCOVER, ConfigValueFactory.fromAnyRef(v))
      case None    => conf9
    }

    val conf11 = cmd.dryRun match {
      case Some(v) => conf10.withValue(DRY_RUN, ConfigValueFactory.fromAnyRef(v))
      case None    => conf10
    }

    val conf12 = cmd.useLock match {
      case Some(v) => conf11.withValue(USE_LOCK, ConfigValueFactory.fromAnyRef(v))
      case None    => conf11
    }

    val conf13 = cmd.inverseOrder match {
      case Some(v) => conf12.withValue(IS_INVERSE_ORDER, ConfigValueFactory.fromAnyRef(v))
      case None    => conf12
    }

    if (cmd.mode.nonEmpty) {
      conf13.withValue(RUN_MODE, ConfigValueFactory.fromAnyRef(cmd.mode))
    } else {
      conf13
    }
  }

  private class CmdParser(programSyntax: String) extends OptionParser[CmdLineConfig](programSyntax) {
    head("\nPramen Workflow Runner", "")
    var rawFormat: Option[String] = None

    opt[String]("workflow").optional().action((value, config) =>
      config.copy(configPathName = value))
      .text("Path to a workflow configuration")

    opt[String]("files").optional().action((value, config) =>
      config.copy(files = value.split(',').map(_.trim)))
      .text("A comma-separated list of files to get from HDFS or S3 (use Hadoop prefix s3a://, hdfs://). " +
        "Files are fetched to the local current directory before reading workflow configuration.")

    opt[String]("ops").optional().action((value, config) =>
      config.copy(operations = value.split(',').map(_.trim)))
      .text("A comma-separated list of output table names that correspond to operations to run.")

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
          config.copy(mode = value))
          .text("Mode of processing for date ranges. One of 'fill_gaps', 'check_updates', 'force'")
          .validate(v =>
            if (v == "fill_gaps" || v == "check_updates" || v == "force") success
            else failure("Invalid run mode. Must be one of 'fill_gaps', 'check_updates', 'force'"))
      )

    opt[Boolean]("track-updates").optional().action((value, config) =>
      config.copy(trackUpdates = Option(value)))
      .text("If false, Pramen won't check information dates for which data has already loaded")

    opt[Boolean]("inverse-order").optional().action((value, config) =>
      config.copy(inverseOrder = Option(value)))
      .text("If true, ranged runs will be done in inverse order (from newest to oldest)")

    opt[String]("rerun").optional().action((value, config) =>
      config.copy(rerunInfoDate = Option(LocalDate.parse(value, infoDateFormatter))))
      .text("Information date to recompute (if specified all jobs in the workflow will be re-run)")

    opt[Int]("table-num").optional().action((value, config) =>
      config.copy(tableNum = Option(value)))
      .text("Run the job only for the specified table number")

    opt[Boolean]("dry-run").optional().action((value, config) =>
      config.copy(dryRun = Option(value)))
      .text("If true, no actual data processing will be done.")

    opt[Boolean]("use-lock").optional().action((value, config) =>
      config.copy(useLock = Option(value)))
      .text("If true (default) a lock is used when writing to a table")

    opt[Boolean]("undercover").optional().action((value, config) =>
      config.copy(undercover = Option(value)))
      .text("If true, no updates will be done to the bookkeeping data (Ensure you are know what you are doing!)")

    opt[Unit]("check-late-only").optional().action((_, config) =>
      config.copy(checkOnlyLateData = Some(true)))
      .text("When specified, only late data from the source will be checked. No checks for new on-time data will be performed.")

    opt[Unit]("check-new-only").optional().action((_, config) =>
      config.copy(checkOnlyNewData = Some(true)))
      .text("When specified, only new data from the source will be checked. The 'track.days' option will be ignored.")

    help("help").text("prints this usage text")
  }

}