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

package za.co.absa.pramen.runner

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.RunnerCommons._
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.state.PipelineStateImpl
import za.co.absa.pramen.core.utils.ConfigUtils

import scala.collection.mutable.ListBuffer

object PipelineRunner {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val workflowExitCodes = new ListBuffer[Int]

  // If the main method is executed from an external component that runs Pramen jobs,
  // this provides exit codes for each of pipelines executed.
  def getExitCodes: Seq[Int] = workflowExitCodes.toSeq

  def main(args: Array[String]): Unit = {
    val configs: Seq[Config] = getMainContext(args)
    val isExitCodeEnabled = configs.head.getBoolean(Keys.EXIT_CODE_ENABLED)
    var overallExitCode = PipelineStateImpl.EXIT_CODE_SUCCESS

    configs.foreach { conf =>
      val needLogEffectiveConfig = ConfigUtils.getOptionBoolean(conf, Keys.LOG_EFFECTIVE_CONFIG).getOrElse(true)
      if (needLogEffectiveConfig) {
        ConfigUtils.logEffectiveConfigProps(conf, Keys.CONFIG_KEYS_TO_REDACT, Keys.KEYS_TO_REDACT)
      } else {
        log.info(s"Logging of the effective configuration is disabled by ${Keys.LOG_EFFECTIVE_CONFIG}=false.")
      }

      val exitCode = AppRunner.runPipeline(conf)
      workflowExitCodes.append(exitCode)
      overallExitCode |= exitCode
    }

    if (isExitCodeEnabled && overallExitCode != PipelineStateImpl.EXIT_CODE_SUCCESS) {
      System.exit(overallExitCode)
    }
  }
}
