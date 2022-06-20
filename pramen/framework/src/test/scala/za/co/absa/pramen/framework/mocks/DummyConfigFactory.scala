/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.framework.mocks

import za.co.absa.pramen.framework.app.config.{BookkeeperConfig, HadoopFormat}
import za.co.absa.pramen.framework.config.{InfoDateExpressions, WatcherConfig}
import za.co.absa.pramen.framework.reader.model.JdbcConfig

import java.time.LocalDate

object DummyConfigFactory {
  def getDummyConfig(ingestionName: String = "test",
                     environmentName: String = "local",
                     bookkeepingEnabled: Boolean = true,
                     bookkeepingLocation: Option[String] = None,
                     bookkeepingHadoopFormat: HadoopFormat = HadoopFormat.Delta,
                     bookkeepingConnectionString: Option[String] = None,
                     bookkeepingDbName: Option[String] = None,
                     bookkeepingJdbcConfig: Option[JdbcConfig] = None,
                     temporaryDirectory: String = "/tmp/dummy",
                     infoDateFormat: String = "yyyy-MM-dd",
                     infoDateStart: LocalDate = LocalDate.of(2020, 8, 10),
                     outputInfoDateExpr: Option[String] = None,
                     infoDateDefaultExpressions: InfoDateExpressions = InfoDateExpressions("", "", ""),
                     trackDays: Int = 2,
                     expectedDelayDays: Int = 1,
                     inputPeriodShiftDays: Int = 0,
                     inputPeriodFromExpr: Option[String] = None,
                     inputPeriodToExpr: Option[String] = None,
                     currentDate: Option[LocalDate] = None,
                     loadDateTo: Option[LocalDate] = None,
                     rerunInfoDate: Option[LocalDate] = None,
                     runOnlyTableNum: Option[Int] = None,
                     alwaysOverwriteLastChunk: Boolean = false,
                     trackUpdates: Boolean = true,
                     dryRun: Boolean = false,
                     useLock: Boolean = true,
                     undercover: Boolean = false,
                     warnNoData: Boolean = true,
                     emailIfNoChanges: Boolean = true,
                     checkOnlyLateData: Boolean = false,
                     checkOnlyNewData: Boolean = false,
                     ignoreSourceDeletions: Boolean = false,
                     ignoreLastUpdatedDate: Boolean = false,
                     parallelTasks: Int = 0,
                     waitForTableEnabled: Boolean = false,
                     waitForTableSeconds: Int = 3
                    ): WatcherConfig = {
    WatcherConfig(ingestionName,
      environmentName,
      BookkeeperConfig(bookkeepingEnabled, bookkeepingLocation, bookkeepingHadoopFormat, bookkeepingConnectionString, bookkeepingDbName, bookkeepingJdbcConfig),
      temporaryDirectory,
      infoDateFormat,
      infoDateStart,
      outputInfoDateExpr,
      infoDateDefaultExpressions,
      trackDays,
      expectedDelayDays,
      inputPeriodShiftDays,
      inputPeriodFromExpr,
      inputPeriodToExpr,
      currentDate,
      loadDateTo,
      rerunInfoDate,
      runOnlyTableNum,
      alwaysOverwriteLastChunk,
      trackUpdates,
      dryRun,
      useLock,
      undercover,
      warnNoData,
      emailIfNoChanges = emailIfNoChanges,
      checkOnlyLateData,
      checkOnlyNewData,
      ignoreSourceDeletions,
      ignoreLastUpdatedDate,
      parallelTasks = parallelTasks,
      waitForTableEnabled = waitForTableEnabled,
      waitForTableSeconds = waitForTableSeconds
    )
  }
}
