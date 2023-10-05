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

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class RuntimeConfigSuite extends AnyWordSpec {
  "RuntimeConfig" should {
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
           |  email.if.no.changes = false
           |  current.date = 2020-12-31
           |  load.date.from = 2020-12-31
           |  load.date.to = 2021-01-10
           |  parallel.tasks = 4
           |  stop.spark.session = true
           |}
           |""".stripMargin

      val config = getUseCase(configStr)

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
      assert(!runtimeConfig.emailIfNoChanges)
      assert(runtimeConfig.runDate.toString == "2020-12-31")
      assert(runtimeConfig.runDateTo.get.toString == "2021-01-10")
      assert(runtimeConfig.parallelTasks == 4)
      assert(runtimeConfig.stopSparkSession)
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
      assert(runtimeConfig.emailIfNoChanges)
      assert(runtimeConfig.runDate.toString == LocalDate.now().toString)
      assert(runtimeConfig.runDateTo.isEmpty)
      assert(runtimeConfig.parallelTasks == 1)
      assert(!runtimeConfig.stopSparkSession)
    }
  }

  "throw RuntimeException when number of parallel tasks is negative" in {
    val config = getUseCase("pramen.parallel.tasks = -2")

    val ex = intercept[RuntimeException] {
      RuntimeConfig.fromConfig(config)
    }

    assert(ex.getMessage.contains("Cannot run negative (or zero) number of tasks in parallel"))
  }

  "throw RuntimeException when number of parallel tasks is zero" in {
    val config = getUseCase("pramen.parallel.tasks = 0")

    val ex = intercept[RuntimeException] {
      RuntimeConfig.fromConfig(config)
    }

    assert(ex.getMessage.contains("Cannot run negative (or zero) number of tasks in parallel"))
  }

  def getUseCase(rawConfig: String = ""): Config = {
    ConfigFactory.parseString(rawConfig)
      .withFallback(ConfigFactory.load())
      .resolve()
  }

}
