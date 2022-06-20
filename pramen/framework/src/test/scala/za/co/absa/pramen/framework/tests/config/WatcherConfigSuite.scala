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

package za.co.absa.pramen.framework.tests.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.app.config.BookkeeperConfig._
import za.co.absa.pramen.framework.app.config.HadoopFormat
import za.co.absa.pramen.framework.config.WatcherConfig

class WatcherConfigSuite extends WordSpec {
  "Configuration loader " should {
    "serialize when bookkeeping is disabled" in {
      val conf: Config = ConfigFactory.parseString(
        s"""$BOOKKEEPING_ENABLED = "false"
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
        .resolve()

      val w = WatcherConfig.load(conf)

      assert(!w.bookkeepingConfig.bookkeepingEnabled)
    }

    "Configuration should serialize when bookkeeping is enabled" in {
      val conf: Config = ConfigFactory.parseString(
        s"""$BOOKKEEPING_ENABLED = "true"
           |$BOOKKEEPING_CONNECTION_STRING = "mongodb://127.0.0.1"
           |$BOOKKEEPING_DB_NAME = "syncpramen"
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
        .resolve()

      val w = WatcherConfig.load(conf)

      assert(w.bookkeepingConfig.bookkeepingEnabled)
    }

    "throw an exception if bookkeping options are not specified" in {
      val conf: Config = ConfigFactory.parseString(
        s"""$BOOKKEEPING_ENABLED = "true"
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[RuntimeException] {
        WatcherConfig.load(conf)
      }

      assert(ex.getMessage.contains("One of the following should be defined: pramen.bookkeeping.jdbc.url"))
    }

    "values from reference.conf should be set by default when bookkeeping is enabled" in {
      val conf: Config = ConfigFactory.parseString(
        s"""$BOOKKEEPING_ENABLED = "true"
           |$BOOKKEEPING_CONNECTION_STRING = "mongodb://127.0.0.1"
           |$BOOKKEEPING_DB_NAME = "syncpramen"
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
        .resolve()

      val w = WatcherConfig.load(conf)

      assert(w.ingestionName == "Unspecified")
      assert(w.environmentName == "DEV")
      assert(w.bookkeepingConfig.bookkeepingLocation.isEmpty)
      assert(w.bookkeepingConfig.bookkeepingHadoopFormat == HadoopFormat.Text)
      assert(w.bookkeepingConfig.bookkeepingConnectionString.contains("mongodb://127.0.0.1"))
      assert(w.bookkeepingConfig.bookkeepingDbName.contains("syncpramen"))
      assert(w.bookkeepingConfig.bookkeepingJdbcConfig.isEmpty)
      assert(w.temporaryDirectory.isEmpty)

      assert(w.infoDateFormat == "yyyy-MM-dd")
      assert(w.infoDateStart.toString == "2020-01-01")
      assert(w.outputInfoDateExpr.isEmpty)

      assert(w.infoDateDefaultExpressions.defaultDailyExpr == "@runDate")
      assert(w.infoDateDefaultExpressions.defaultWeeklyExpr == "lastMonday(@runDate)")
      assert(w.infoDateDefaultExpressions.defaultMonthlyExpr == "beginOfMonth(@runDate)")

      assert(w.trackDays == 4)
      assert(w.expectedDelayDays == 0)
      assert(w.inputPeriodShiftDays == 0)
      assert(w.currentDate.isEmpty)
      assert(w.loadDateTo.isEmpty)
      assert(w.rerunInfoDate.isEmpty)
      assert(w.runOnlyTableNum.isEmpty)
      assert(!w.alwaysOverwriteLastChunk)
      assert(w.trackUpdates)
      assert(!w.dryRun)
      assert(w.useLock)
      assert(!w.undercover)
      assert(w.warnNoData)
      assert(w.emailIfNoChanges)
      assert(!w.checkOnlyLateData)
      assert(!w.checkOnlyNewData)
      assert(!w.ignoreSourceDeletions)
      assert(!w.ignoreLastUpdatedDate)
      assert(w.parallelTasks == 1)
      assert(!w.waitForTableEnabled)
      assert(w.waitForTableSeconds == 600)
    }

    "values from reference.conf should be set by default when bookkeeping is disabled" in {
      val conf: Config = ConfigFactory.parseString(
        s"""$BOOKKEEPING_ENABLED = "false"
           |$BOOKKEEPING_HADOOP_FORMAT = "delta"
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
        .resolve()

      val w = WatcherConfig.load(conf)

      assert(w.ingestionName == "Unspecified")
      assert(w.environmentName == "DEV")
      assert(w.bookkeepingConfig.bookkeepingLocation.isEmpty)
      assert(w.bookkeepingConfig.bookkeepingHadoopFormat == HadoopFormat.Delta)
      assert(w.bookkeepingConfig.bookkeepingConnectionString.isEmpty)
      assert(w.bookkeepingConfig.bookkeepingDbName.isEmpty)
      assert(w.bookkeepingConfig.bookkeepingJdbcConfig.isEmpty)
      assert(w.temporaryDirectory.isEmpty)

      assert(w.infoDateFormat == "yyyy-MM-dd")
      assert(w.infoDateStart.toString == "2020-01-01")
      assert(w.outputInfoDateExpr.isEmpty)

      assert(w.trackDays == 0)
      assert(w.expectedDelayDays == 0)
      assert(w.inputPeriodShiftDays == 0)
      assert(w.currentDate.isEmpty)
      assert(w.loadDateTo.isEmpty)
      assert(w.rerunInfoDate.isEmpty)
      assert(w.runOnlyTableNum.isEmpty)
      assert(!w.alwaysOverwriteLastChunk)
      assert(w.trackUpdates)
      assert(!w.dryRun)
      assert(!w.useLock)
      assert(!w.undercover)
      assert(w.warnNoData)
      assert(w.emailIfNoChanges)
      assert(!w.checkOnlyLateData)
      assert(w.checkOnlyNewData)
      assert(!w.ignoreSourceDeletions)
      assert(w.ignoreLastUpdatedDate)
      assert(w.parallelTasks == 1)
      assert(!w.waitForTableEnabled)
      assert(w.waitForTableSeconds == 600)
    }
  }

}
