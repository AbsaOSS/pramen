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

package za.co.absa.pramen.core

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.app.config.RuntimeConfig.VERBOSE
import za.co.absa.pramen.core.cmd.CmdLineConfig
import za.co.absa.pramen.core.utils.JavaXConfig

import java.io.File
import java.nio.file.{FileSystems, Files, Paths}
import scala.util.Try

object RunnerCommons {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getMainContext(args: Array[String]): Seq[Config] = {
    val rootLogger = Logger.getRootLogger

    val cmdLineConfig = CmdLineConfig(args)

    cmdLineConfig.overrideLogLevel.foreach(level => rootLogger.setLevel(Level.toLevel(level)))

    if (cmdLineConfig.files.nonEmpty) {
      val hadoopConfig = new Configuration()
      copyFilesToLocal(cmdLineConfig.files, hadoopConfig)
    }

    val configs: Seq[Config] = getConfigs(cmdLineConfig.configPathNames, cmdLineConfig)
    val primaryConfig = configs.head

    JavaXConfig.setJavaXProperties(primaryConfig)

    if (!primaryConfig.getBoolean(VERBOSE)) {
      // Switch logging level to WARN
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)

      Try {
        // Using reflection to invoke for Spark 3.3+:
        // Configurator.setLevel("org", "warn")
        // Configurator.setLevel("akka", "warn")

        val clazz = Class.forName("org.apache.logging.log4j.core.config.Configurator")

        val method = clazz.getMethod("setLevel", classOf[String], classOf[String])

        method.invoke(null, "org", "warn")
        method.invoke(null, "akka", "warn")
      }
    }

    configs
  }

  def getConfigs(configPaths: Seq[String], cmd: CmdLineConfig): Seq[Config] = {
    if (configPaths.isEmpty) {
      Seq(getConfig(None, cmd))
    } else {
      configPaths.map(path => getConfig(Some(path), cmd))
    }
  }

  def getConfig(configPath: Option[String], cmd: CmdLineConfig): Config = {
    val originalConfig = ConfigFactory.load()

    val conf = configPath match {
      case Some(path) =>
        log.info(s"Loading $path...\n")
        val effectivePath = getExistingWorkflowPath(path)

        ConfigFactory
          .parseFile(new File(effectivePath))
          .withFallback(originalConfig)
          .resolve()
      case None =>
        log.warn("No '--workflow <file.conf>' is provided. Assuming configuration is present in 'application.conf'.")
        originalConfig
          .resolve()
    }

    CmdLineConfig.applyCmdLineToConfig(conf, cmd)
  }

  def getExistingWorkflowPath(pathStr: String): String = {
    val path = Paths.get(pathStr)

    if (Files.exists(path)) {
      return pathStr
    }

    val slash = FileSystems.getDefault.getSeparator
    if (pathStr.contains(slash)) {
      val fileInCurrentDir = path.getFileName
      if (Files.exists(fileInCurrentDir)) {
        log.warn(s"The workflow file $pathStr does not exist. Loading $fileInCurrentDir\n")
        return fileInCurrentDir.toString
      }
    }

    throw new IllegalArgumentException(s"The workflow configuration '$pathStr' does not exist at the driver node.")
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
