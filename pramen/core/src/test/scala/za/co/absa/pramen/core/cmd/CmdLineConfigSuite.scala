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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.app.config.InfoDateConfig.TRACK_DAYS
import za.co.absa.pramen.core.app.config.RuntimeConfig._
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.config.Keys.LOG_EFFECTIVE_CONFIG
import za.co.absa.pramen.core.utils.ConfigUtils

class CmdLineConfigSuite extends AnyWordSpec {

  private val emptyConfig = ConfigFactory.empty
  private val populatedConfig = ConfigFactory.parseString(
    s"""$DRY_RUN = false
       |$CURRENT_DATE = 2020-08-10
       |$IS_RERUN = true
       |""".stripMargin
  )

  "CmdLineConfig.parseCmdLine()" when {
    "no command line arguments are provided" should {
      "return an empty value" in {
        val cmd = CmdLineConfig.parseCmdLine(Array.empty[String])
        assert(cmd.isEmpty)
      }
    }

    "mandatory parameters are provided" should {
      "parse workflow location" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
        assert(cmd.nonEmpty)
        assert(cmd.get.configPathNames.contains("dummy.config"))
        assert(cmd.get.dryRun.isEmpty)
        assert(cmd.get.undercover.isEmpty)
        assert(cmd.get.useLock.isEmpty)
      }

      "parse workflow locations" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflows", "dummy1.config,dummy2.config"))
        assert(cmd.nonEmpty)
        assert(cmd.get.configPathNames.contains("dummy1.config"))
        assert(cmd.get.configPathNames.contains("dummy2.config"))
        assert(cmd.get.dryRun.isEmpty)
        assert(cmd.get.undercover.isEmpty)
        assert(cmd.get.useLock.isEmpty)
      }

      "both workflow and workflows are not allowed together" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--workflows", "dummy1.config,dummy2.config"))
        assert(cmd.isEmpty)
      }

      "parse the list of files to fetch" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--files", "s3://mybucket/myprefix/myfile,hdfs://mycluster/myfolder/myfile"))
        assert(cmd.nonEmpty)
        assert(cmd.get.files == Seq("s3://mybucket/myprefix/myfile", "hdfs://mycluster/myfolder/myfile"))
      }

      "parse dry-run flag when dry run = true" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--dry-run"))
        assert(cmd.nonEmpty)
        assert(cmd.get.dryRun.nonEmpty)
        assert(cmd.get.dryRun.get)
      }

      "ignore dry-run if it is not specified" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
        assert(cmd.nonEmpty)
        assert(cmd.get.dryRun.isEmpty)
      }

      "parse check late data only if --check-late-only = true" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--check-late-only"))
        assert(cmd.nonEmpty)
        assert(cmd.get.checkOnlyLateData.nonEmpty)
        assert(cmd.get.checkOnlyLateData.get)
      }

      "parse check new data only if --check-new-only = true" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--check-new-only"))
        assert(cmd.nonEmpty)
        assert(cmd.get.checkOnlyNewData.nonEmpty)
        assert(cmd.get.checkOnlyNewData.get)
      }

      "return None when wrong date format is passed to --date" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date", "16/08/2020"))
        assert(cmd.isEmpty)
      }

      "return None when wrong date format is passed to --rerun" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--rerun", "16/08/2020"))
        assert(cmd.isEmpty)
      }

      "support log level override" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--override-log-level", "INFO"))
        assert(cmd.nonEmpty)
        assert(cmd.get.overrideLogLevel.contains("INFO"))
      }

      "support specification of parallel tasks" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--parallel-tasks", "2"))
        assert(cmd.nonEmpty)
        assert(cmd.get.parallelTasks.get == 2)
      }

      "return None if number of parallel tasks is invalid" in {
        val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--parallel-tasks", "-12"))
        assert(cmd.isEmpty)
      }
    }
  }

  "CmdLineConfig.applyCmdLineToConfig()" should {
    "return a modified config if the list of operations is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--ops", "table_1,table_2,table_3"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(RUN_TABLES))
      assert(ConfigUtils.getOptListStrings(config, RUN_TABLES) == Seq("table_1", "table_2", "table_3"))
    }

    "return a modified config if number of parallel tasks is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--parallel-tasks", "2"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(Keys.PARALLEL_TASKS))
      assert(config.getInt(Keys.PARALLEL_TASKS) == 2)
    }

    "return the original config if number of parallel tasks is not specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(!config.hasPath(Keys.PARALLEL_TASKS))
    }

    "return the original config if dry-run is not specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(!config.hasPath(CURRENT_DATE))
      assert(!config.hasPath(DRY_RUN))
      assert(!config.hasPath(IS_RERUN))
    }

    "return a modified config if dry-run is true" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--dry-run"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(DRY_RUN))
      assert(config.getBoolean(DRY_RUN))
      assert(!config.hasPath(CURRENT_DATE))
      assert(!config.hasPath(IS_RERUN))
    }

    "return the original config if verbose is not mentioned" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(!config.hasPath(VERBOSE))
    }

    "return a modified config if verbose is true" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--verbose"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(VERBOSE))
      assert(config.getBoolean(VERBOSE))
    }

    "return a modified config if rerun info date is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--rerun", "2020-08-16"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(CURRENT_DATE))
      assert(config.getString(CURRENT_DATE) == "2020-08-16")
      assert(config.hasPath(IS_RERUN))
      assert(config.getBoolean(IS_RERUN))
    }

    "return a modified config if current date override is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date", "2020-08-15"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(CURRENT_DATE))
      assert(config.getString(CURRENT_DATE) == "2020-08-15")
      assert(!config.hasPath(IS_RERUN))
    }

    "return a proper dates when both rerun and current dates are specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date", "2020-08-15", "--rerun", "2020-08-16"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(CURRENT_DATE))
      assert(config.getString(CURRENT_DATE) == "2020-08-15")
      assert(config.hasPath(IS_RERUN))
      assert(config.getBoolean(IS_RERUN))
    }

    "return a modified config if date-from override is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date-from", "2020-08-15"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(LOAD_DATE_FROM))
      assert(config.getString(LOAD_DATE_FROM) == "2020-08-15")
    }

    "return a modified config if date-to override is specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date-to", "2020-08-15", "--inverse-order", "true", "--run-mode", "fill_gaps"))
      val config = CmdLineConfig.applyCmdLineToConfig(emptyConfig, cmd.get)

      assert(config.hasPath(LOAD_DATE_TO))
      assert(config.getString(LOAD_DATE_TO) == "2020-08-15")
      assert(config.hasPath(TRACK_DAYS))
      assert(config.getString(TRACK_DAYS) == "0")
      assert(config.hasPath(IS_INVERSE_ORDER))
      assert(config.getBoolean(IS_INVERSE_ORDER))
      assert(config.getString(RUN_MODE) == "fill_gaps")
    }

    "return the original config if no cmd line arguments are provided" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--inverse-order", "false"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(CURRENT_DATE))
      assert(config.hasPath(DRY_RUN))

      assert(!config.getBoolean(DRY_RUN))
      assert(config.getString(CURRENT_DATE) == "2020-08-10")

      assert(config.hasPath(IS_INVERSE_ORDER))
      assert(!config.getBoolean(IS_INVERSE_ORDER))
    }

    "return the modified config if cmd line arguments are provided" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--rerun", "2020-08-16", "--dry-run", "--check-late-only"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(CURRENT_DATE))
      assert(config.hasPath(DRY_RUN))

      assert(config.getBoolean(DRY_RUN))
      assert(config.getBoolean(CHECK_ONLY_LATE_DATA))
      assert(config.getString(CURRENT_DATE) == "2020-08-16")
    }

    "return the modified config if undercover = true" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--undercover"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(UNDERCOVER))

      assert(config.getBoolean(UNDERCOVER))
    }

    "return the modified config if useLock = true" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--use-lock", "true"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(USE_LOCK))

      assert(config.getBoolean(USE_LOCK))
    }

    "return the modified config if useLock = false" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--use-lock", "false"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(USE_LOCK))

      assert(!config.getBoolean(USE_LOCK))
    }

    "return the original config if undercover is not specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(!config.hasPath(UNDERCOVER))
    }

    "return the original config if log-config is not specified" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(!config.hasPath(LOG_EFFECTIVE_CONFIG))
    }

    "return the modified config if log-config = true" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--log-config", "true"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(LOG_EFFECTIVE_CONFIG))

      assert(config.getBoolean(LOG_EFFECTIVE_CONFIG))
    }

    "return the modified config if log-config = false" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--log-config", "false"))
      val config = CmdLineConfig.applyCmdLineToConfig(populatedConfig, cmd.get)

      assert(config.hasPath(LOG_EFFECTIVE_CONFIG))

      assert(!config.getBoolean(LOG_EFFECTIVE_CONFIG))
    }

    "return None on invalid run mode" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--date-to", "2020-08-15", "--run-mode", "abcd"))

      assert(cmd.isEmpty)
    }

    "return None on dependent option failure" in {
      val cmd = CmdLineConfig.parseCmdLine(Array("--workflow", "dummy.config", "--run-mode", "fill_gaps"))

      assert(cmd.isEmpty)
    }
  }

}
