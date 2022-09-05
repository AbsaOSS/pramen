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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.app.config.RuntimeConfig.VERBOSE
import za.co.absa.pramen.core.utils.{ConfigUtils, JavaXConfig}
import za.co.absa.pramen.runner.cmd.CmdLineConfig
import za.co.absa.pramen.runner.config.Constants

import java.io.File

object RunnerCommons {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getMainContext(args: Array[String]): Config = {
    val cmdLineConfig = CmdLineConfig(args)

    if (cmdLineConfig.files.nonEmpty) {
      val hadoopConfig = new Configuration()
      copyFilesToLocal(cmdLineConfig.files, hadoopConfig)
    }

    val conf: Config = if (cmdLineConfig.configPathName.isEmpty) {
      log.warn("No '--workflow <file.conf>' is provided. Assuming configuration is present in 'application.conf'.")
      ConfigFactory.load()
    } else {
      getConfig(cmdLineConfig.configPathName, cmdLineConfig)
    }

    JavaXConfig.setJavaXProperties(conf)

    ConfigUtils.logEffectiveConfigProps(conf, Constants.CONFIG_KEYS_TO_REDACT, Constants.CONFIG_WORDS_TO_REDACT)

    if (!conf.getBoolean(VERBOSE)) {
      // Switch logging level to WARN
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
    }

    conf
  }

  def getConfig(configPath: String, cmd: CmdLineConfig): Config = {
    val originalConfig = ConfigFactory.load()
    log.info(s"Loading $configPath...\n")
    val conf = ConfigFactory.parseFile(new File(configPath))
      .withFallback(originalConfig)
      .resolve()

    CmdLineConfig.applyCmdLineToConfig(conf, cmd)
  }

  def copyFilesToLocal(files: Seq[String], hadoopConfig: Configuration): Unit = {
    val currentPath = new Path(".")

    files.foreach(file => {
      log.info(s"Fetching '$file'...")
      val fs = new Path(file).getFileSystem(hadoopConfig)


      fs.copyToLocalFile(new Path(file), currentPath)
    })
  }
}
