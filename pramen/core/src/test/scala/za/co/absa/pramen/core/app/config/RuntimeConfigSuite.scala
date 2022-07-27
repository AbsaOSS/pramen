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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

import java.time.LocalDate

class RuntimeConfigSuite extends WordSpec {
  "GeneralConfig" should {
    "deserialize the config properly" in {
      val configStr =
        s"""pramen {
           |  dry.run = true
           |  runtime {
           |    is.rerun = true
           |    inverse.order = true
           |    run.tables = [ tbl1, tbl2 ]
           |  }
           |  undercover = true
           |  use.lock = false
           |  track.updates = false
           |  check.only.late.data = true
           |  check.only.new.data = true
           |  warn.if.no.data = false
           |  non.zero.exit.code.if.no.data = true
           |  email.if.no.changes = false
           |  current.date = 2020-12-31
           |  load.date.from = 2020-12-31
           |  load.date.to = 2021-01-10
           |  parallel.tasks = 4
           |  stop.spark.session = false
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())
        .resolve()

      val runtimeConfig = RuntimeConfig.fromConfig(config)

      assert(runtimeConfig.isDryRun)
      assert(runtimeConfig.isRerun)
      assert(runtimeConfig.isInverseOrder)
      assert(runtimeConfig.runTables.contains("tbl1"))
      assert(runtimeConfig.runTables.contains("tbl2"))
      assert(runtimeConfig.isUndercover)
      assert(!runtimeConfig.useLocks)
      assert(runtimeConfig.checkOnlyLateData)
      assert(runtimeConfig.checkOnlyNewData)
      assert(!runtimeConfig.warnIfNoData)
      assert(runtimeConfig.nonZeroExitCodeIfNoData)
      assert(!runtimeConfig.emailIfNoChanges)
      assert(runtimeConfig.runDate.toString == "2020-12-31")
      assert(runtimeConfig.runDateTo.get.toString == "2021-01-10")
      assert(runtimeConfig.parallelTasks == 4)
      assert(!runtimeConfig.stopSparkSession)
    }

    "have default values" in {
      val config = ConfigFactory.load()

      val runtimeConfig = RuntimeConfig.fromConfig(config)

      assert(!runtimeConfig.isDryRun)
      assert(!runtimeConfig.isRerun)
      assert(!runtimeConfig.isInverseOrder)
      assert(runtimeConfig.runTables.isEmpty)
      assert(!runtimeConfig.isUndercover)
      assert(runtimeConfig.useLocks)
      assert(!runtimeConfig.checkOnlyLateData)
      assert(!runtimeConfig.checkOnlyNewData)
      assert(runtimeConfig.warnIfNoData)
      assert(!runtimeConfig.nonZeroExitCodeIfNoData)
      assert(runtimeConfig.emailIfNoChanges)
      assert(runtimeConfig.runDate.toString == LocalDate.now().toString)
      assert(runtimeConfig.runDateTo.isEmpty)
      assert(runtimeConfig.parallelTasks == 1)
      assert(runtimeConfig.stopSparkSession)
    }
  }
}