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

package za.co.absa.pramen.framework.config

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.app.config.BookkeeperConfig
import za.co.absa.pramen.framework.model.Constants.DATE_FORMAT_INTERNAL
import za.co.absa.pramen.framework.utils.ConfigUtils
import za.co.absa.pramen.framework.utils.DateUtils.convertStrToDate

import java.time.LocalDate

case class WatcherConfig(
                          ingestionName: String,
                          environmentName: String,
                          bookkeepingConfig: BookkeeperConfig,
                          temporaryDirectory: String,

                          infoDateFormat: String,
                          infoDateStart: LocalDate,
                          outputInfoDateExpr: Option[String],
                          infoDateDefaultExpressions: InfoDateExpressions,
                          trackDays: Int,
                          expectedDelayDays: Int,
                          inputPeriodShiftDays: Int,
                          inputPeriodFromExpr: Option[String],
                          inputPeriodToExpr: Option[String],
                          currentDate: Option[LocalDate],
                          loadDateTo: Option[LocalDate],
                          rerunInfoDate: Option[LocalDate],
                          runOnlyTableNum: Option[Int],
                          alwaysOverwriteLastChunk: Boolean,
                          trackUpdates: Boolean,
                          dryRun: Boolean,
                          useLock: Boolean,
                          undercover: Boolean,
                          warnNoData: Boolean,
                          emailIfNoChanges: Boolean,
                          checkOnlyLateData: Boolean,
                          checkOnlyNewData: Boolean,
                          ignoreSourceDeletions: Boolean,
                          ignoreLastUpdatedDate: Boolean,
                          parallelTasks: Int,
                          waitForTableEnabled: Boolean,
                          waitForTableSeconds: Int
                        )

object WatcherConfig {
  private val log = LoggerFactory.getLogger(this.getClass)

  val INGESTION_NAME = "pramen.ingestion.name"
  val ENVIRONMENT_NAME = "pramen.environment.name"

  val TEMPORARY_DIRECTORY = "pramen.temporary.directory"

  val INFORMATION_DATE_FORMAT_APP = "pramen.information.date.format"
  val INFORMATION_DATE_START = "pramen.information.date.start"

  val TRACK_DAYS = "pramen.track.days"
  val EXPECTED_DELAY_DAYS = "pramen.expected.delay.days"
  val INPUT_PERIOD_SHIFT_DAYS = "pramen.input.period.shift.days"
  val INPUT_PERIOD_FROM_EXPR = "pramen.input.period.from.expr"
  val INPUT_PERIOD_TO_EXPR = "pramen.input.period.to.expr"

  val DRY_RUN = "pramen.dry.run"
  val RUN_USE_LOCK = "pramen.use.lock"
  val RUN_UNDERCOVER = "pramen.undercover"
  val WARN_NO_DATA = "pramen.warn.if.no.data "
  val NON_ZERO_EXIT_CODE_IF_NO_DATA = "pramen.non.zero.exit.code.if.no.data"
  val EMAIL_IF_NO_CHANGES = "pramen.email.if.no.changes"

  val RERUN_INFO_DATE = "pramen.rerun.info.date"
  val CURRENT_DATE = "pramen.current.date"
  val LOAD_DATE_TO = "pramen.load.date.to"
  val ALWAYS_OVERWRITE_LAST_CHUNK = "pramen.always.overwrite.last.chunk"
  val TRACK_UPDATES = "pramen.track.updates"
  val RUN_ONLY_TABLE = "pramen.run.only.table"

  val OUTPUT_INFO_DATE_EXPR = "pramen.output.info.date.expr"

  val CHECK_ONLY_LATE_DATA = "pramen.check.only.late.data"
  val CHECK_ONLY_NEW_DATA = "pramen.check.only.new.data"
  val IGNORE_SOURCE_DELETIONS = "pramen.ignore.source.deletions"
  val IGNORE_LAST_UPDATED_DATE = "pramen.ignore.last.updated.date"

  val PARALLEL_TASKS_KEY = "pramen.parallel.tasks"
  val WAIT_FOR_TABLE_ENABLED = "pramen.wait.for.output.table.enabled"
  val WAIT_FOR_TABLE_SECONDS = "pramen.wait.for.output.table.seconds"

  def load(conf: Config): WatcherConfig = {
    val infoDateFormat = conf.getString(INFORMATION_DATE_FORMAT_APP)

    def getDate(dateStr: String): LocalDate = {
      convertStrToDate(dateStr, DATE_FORMAT_INTERNAL, infoDateFormat)
    }

    val pramenConfig = WatcherConfig(
      ingestionName = conf.getString(INGESTION_NAME),
      environmentName = conf.getString(ENVIRONMENT_NAME),
      bookkeepingConfig = BookkeeperConfig.fromConfig(conf),
      temporaryDirectory = conf.getString(TEMPORARY_DIRECTORY),
      infoDateFormat = infoDateFormat,
      infoDateStart = getDate(conf.getString(INFORMATION_DATE_START)),
      outputInfoDateExpr = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE_EXPR),
      infoDateDefaultExpressions = InfoDateExpressions.fromConfig(conf),
      trackDays = conf.getInt(TRACK_DAYS),
      expectedDelayDays = conf.getInt(EXPECTED_DELAY_DAYS),
      inputPeriodShiftDays = conf.getInt(INPUT_PERIOD_SHIFT_DAYS),
      inputPeriodFromExpr =ConfigUtils.getOptionString(conf, INPUT_PERIOD_FROM_EXPR),
      inputPeriodToExpr = ConfigUtils.getOptionString(conf, INPUT_PERIOD_TO_EXPR),
      currentDate = ConfigUtils.getOptionString(conf, CURRENT_DATE).map(getDate),
      loadDateTo = ConfigUtils.getOptionString(conf, LOAD_DATE_TO).map(getDate),
      rerunInfoDate = ConfigUtils.getOptionString(conf, RERUN_INFO_DATE).map(getDate),
      runOnlyTableNum = ConfigUtils.getOptionInt(conf, RUN_ONLY_TABLE),
      alwaysOverwriteLastChunk = conf.getBoolean(ALWAYS_OVERWRITE_LAST_CHUNK),
      trackUpdates = conf.getBoolean(TRACK_UPDATES),
      dryRun = conf.getBoolean(DRY_RUN),
      useLock = conf.getBoolean(RUN_USE_LOCK),
      undercover = ConfigUtils.getOptionBoolean(conf, RUN_UNDERCOVER).getOrElse(false),
      warnNoData = conf.getBoolean(WARN_NO_DATA),
      emailIfNoChanges = conf.getBoolean(EMAIL_IF_NO_CHANGES),
      checkOnlyLateData = conf.getBoolean(CHECK_ONLY_LATE_DATA),
      checkOnlyNewData = conf.getBoolean(CHECK_ONLY_NEW_DATA),
      ignoreSourceDeletions = conf.getBoolean(IGNORE_SOURCE_DELETIONS),
      ignoreLastUpdatedDate = conf.getBoolean(IGNORE_LAST_UPDATED_DATE),
      parallelTasks = conf.getInt(PARALLEL_TASKS_KEY),
      waitForTableEnabled = conf.getBoolean(WAIT_FOR_TABLE_ENABLED),
      waitForTableSeconds = conf.getInt(WAIT_FOR_TABLE_SECONDS)
    )

    val pramenConfigWithInvariants = if (!pramenConfig.bookkeepingConfig.bookkeepingEnabled) {
      if (pramenConfig.trackDays != 0) {
        log.warn("The 'track.days' option is not available when bookkeeping is DISABLED. Use '--date' command line option to load data for the non-current day.")
      }
      pramenConfig.copy(useLock = false, ignoreLastUpdatedDate = true, checkOnlyNewData = true, checkOnlyLateData = false, trackDays = 0)
    } else {
      pramenConfig
    }

    pramenConfigWithInvariants
  }
}
