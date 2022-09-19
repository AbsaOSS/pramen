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

package za.co.absa.pramen.extras.utils

import com.typesafe.config._
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object ConfigUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getOptionBoolean(conf: Config, path: String): Option[Boolean] = {
    if (conf.hasPath(path)) {
      Option(conf.getBoolean(path))
    } else {
      None
    }
  }

  def getOptionInt(conf: Config, path: String): Option[Int] = {
    if (conf.hasPath(path)) {
      Option(conf.getInt(path))
    } else {
      None
    }
  }

  def getOptionString(conf: Config, path: String): Option[String] = {
    if (conf.hasPath(path)) {
      Option(conf.getString(path))
    } else {
      None
    }
  }

  def getOptListStrings(conf: Config, path: String): Seq[String] = {
    if (conf.hasPath(path)) {
      conf.getStringList(path).asScala.toList
    } else {
      Nil
    }
  }

  def getExtraOptions(conf: Config, prefix: String): Map[String, String] = {
    if (conf.hasPath(prefix)) {
      ConfigUtils.getFlatConfig(conf.getConfig(prefix))
        .map { case (k, v) => (k, v.toString) }
    } else {
      Map()
    }
  }

  def logExtraOptions(description: String, extraOptions: Map[String, String], redactedKeys: Set[String] = Set.empty[String]): Unit = {
    if (extraOptions.nonEmpty) {
      val p = "\""
      log.info(description)
      extraOptions.foreach { case (key, value) =>
        if (redactedKeys.contains(key.toLowerCase())) {
          log.info(s"$key = [redacted]")
        } else {
          log.info(s"$key = $p$value$p")
        }
      }
    }
  }

  /**
    * Given a configuration returns a new configuration which has all sensitive keys redacted.
    *
    * @param keysToRedact A set of keys to be redacted.
    */
  def getRedactedConfig(conf: Config, keysToRedact: Set[String]): Config = {
    def withAddedKey(accumulatedConfig: Config, key: String): Config = {
      if (conf.hasPath(key)) {
        accumulatedConfig.withValue(key, ConfigValueFactory.fromAnyRef("[redacted]"))
      } else {
        accumulatedConfig
      }
    }

    val redactingConfig = keysToRedact.foldLeft(ConfigFactory.empty)(withAddedKey)

    redactingConfig.withFallback(conf)
  }

  /**
    * Given a configuration returns a new configuration which has all sensitive keys redacted.
    * A key is considered sensitive if it contains one of specified tokens.
    *
    * @param tokensToRedact A set of keys to be redacted.
    */
  def getRedactedFlatConfig(flatConf: Map[String, AnyRef], tokensToRedact: Set[String]): Map[String, AnyRef] = {
    flatConf.map {
      case (k, v) =>
        val redactedValue = getRedactedValue(k, v, tokensToRedact)
        (k, redactedValue)
    }
  }

  def getRedactedValue(key: String, value: AnyRef, tokensToRedact: Set[String]): AnyRef = {
    val needRedact = tokensToRedact.exists(w => key.toLowerCase.contains(w.toLowerCase()))
    if (needRedact) {
      "[redacted]"
    } else {
      value
    }
  }

  /**
    * Flattens TypeSafe config tree and returns the effective configuration.
    *
    * @return the effective configuration as a map
    */
  def getFlatConfig(conf: Config): Map[String, AnyRef] = {
    conf.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    }).toMap
  }

  /**
    * Logs the effective configuration while redacting sensitive keys
    * in HOCON format.
    *
    * @param keysToRedact A set of keys for which values shouldn't be logged.
    */
  def logEffectiveConfigHocon(conf: Config, keysToRedact: Set[String] = Set()): Unit = {
    val redactedConfig = getRedactedConfig(conf, keysToRedact)

    val renderOptions = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)

    val rendered = redactedConfig.root().render(renderOptions)

    log.info(s"Effective configuration:\n$rendered")
  }

  def validatePathsExistence(conf: Config, parent: String, paths: Seq[String]): Unit = {
    val pathsNotFound = paths.filterNot(path => conf.hasPath(path))

    if (pathsNotFound.nonEmpty) {
      val missingPaths = pathsNotFound.map(path =>
        if (parent.isEmpty) s"$path" else s"$parent.$path"
      ).mkString(", ")
      throw new IllegalArgumentException(s"Mandatory configuration options are missing: $missingPaths")
    }
  }

  def validateOneOfPathsExistence(conf: Config, parent: String, paths: Seq[String]): Unit = {
    val haveOneOfPaths = paths.exists(path => conf.hasPath(path))

    if (!haveOneOfPaths) {
      val missingPaths = paths.map(path =>
        if (parent.isEmpty) s"$path" else s"$parent.$path"
      ).mkString(", ")
      throw new IllegalArgumentException(s"One of the following configuration paths must be defined: $missingPaths")
    }
  }
}
