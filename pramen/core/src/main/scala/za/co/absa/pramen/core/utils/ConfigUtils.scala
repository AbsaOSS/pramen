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

package za.co.absa.pramen.core.utils

import com.typesafe.config._
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.pipeline.PipelineDef.PIPELINE_NAME_KEY
import za.co.absa.pramen.core.utils.StringUtils.{escapeString, trimLeft}

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
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

  def getOptionLong(conf: Config, path: String): Option[Long] = {
    if (conf.hasPath(path)) {
      Option(conf.getLong(path))
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

  def getOptionConfig(conf: Config, path: String): Config = {
    if (conf.hasPath(path)) {
      conf.getConfig(path)
    } else {
      ConfigFactory.empty()
    }
  }

  /**
    * Parses an array of objects and returns a sequence of Config objects.
    * If the path does not exist, returns an empty Seq.
    * {{{
    *   my.path = [
    *     { ... },
    *     { ... }
    *   ]
    * }}}
    *
    * The above example is similar to the behavior of `Config.getConfigList(path)`
    *
    * If the path contains named subkeys, they get merged into a single list, e.g.:
    * {{{
    *   my.path.abc = [
    *     { ... },
    *     { ... }
    *   ]
    *   my.path.def = [
    *     { ... },
    *     { ... }
    *   ]
    * }}}
    *
    * In the above example the result will be 4 objects by mergins 'abc' and 'def' nodes.
    *
    * @param conf The configuration object.
    * @param path The path to the array in the configuration tree.
    * @return The sequence of Config objects corresponding to elements on the array.
    */
  def getOptionConfigList(conf: Config, path: String): Seq[Config] = {
    if (conf.hasPath(path)) {
      // The path should contain either
      // my.path = [
      //   { ... },
      //   { ... }
      // ]
      // or
      // my.path.abc = = [
      //   { ... },
      //   { ... }
      // ]
      // my.path.def = = [
      //   { ... },
      //   { ... }
      // ]
      // e.g - either an array of Object or an object that has subkeys of 'abc' and 'def'.
      // There is no way to check it in advance, so the exception handling is used in this case.
      try {
        val subArrays = conf.getObject(path).keySet().toArray

        subArrays.flatMap(key => {
          val subPath = s"$path.$key"
          conf.getConfigList(subPath).asScala.toSeq
        }).toSeq
      } catch {
        case _: Throwable =>
          conf.getConfigList(path).asScala.toSeq
      }
    } else {
      Seq.empty[Config]
    }
  }

  def getDate(conf: Config, path: String, format: String): LocalDate = {
    val dateString = conf.getString(path)
    val fmt = DateTimeFormatter.ofPattern(format)

    LocalDate.parse(dateString, fmt)
  }

  def getDateOpt(conf: Config, path: String, format: String): Option[LocalDate] = {
    if (conf.hasPath(path)) {
      val dateString = conf.getString(path)
      val fmt = DateTimeFormatter.ofPattern(format)

      Option(LocalDate.parse(dateString, fmt))
    } else {
      None
    }
  }

  def getDaysOfWeek(conf: Config, path: String): Seq[DayOfWeek] = {
    val weekDayNums = conf.getIntList(path).asScala

    weekDayNums.map(num => DayOfWeek.of(num)).toSeq
  }

  def getListStringsByPrefix(conf: Config, prefix: String): Seq[String] = {
    var i = 1
    val lst = new ListBuffer[String]
    while (conf.hasPath(s"$prefix.$i")) {
      lst += conf.getString(s"$prefix.$i")
      i += 1
    }
    lst.toList
  }

  def getOptListStrings(conf: Config, path: String): Seq[String] = {
    if (conf.hasPath(path)) {
      conf.getStringList(path).asScala.toList
    } else {
      Nil
    }
  }

  def getExtraOptions(conf: Config, prefix: String): Map[String, String] = {
    // This is the same as getFlatConfig(), but handles ConfigList
    // in the special way that is easier to convert ack to a configuration.
    // Otherwise, the ConfigList is rendered as a non-parsable JSON that cannot be
    // converted back to Config in all cases.
    // Search for "work with list of objects" unit test that illustrates the problem and
    // demonstrates how the conversion could work.
    def getFlatConfigSpecial(conf: Config): Map[String, AnyRef] = {
      val renderOptions = ConfigRenderOptions.defaults()
        .setComments(false)
        .setOriginComments(false)
        .setJson(true)
        .setFormatted(false)
      conf.entrySet().asScala.map({ entry =>
        val k = entry.getKey
        val v: ConfigValue = entry.getValue
        v match {
          case list: ConfigList =>
            val lst = list.unwrapped()

            if (!lst.isEmpty && lst.get(0).isInstanceOf[java.util.Map[_, _]]) {
              k -> v.render(renderOptions)
            } else  {
              k -> v.unwrapped()
            }
          case _ =>
            k -> v.unwrapped()
        }
      }).toMap
    }

    val optionsConfig = if (prefix.isEmpty) {
      conf
    } else {
      if (conf.hasPath(prefix)) {
        conf.getConfig(prefix)
      } else {
        ConfigFactory.empty()
      }
    }

    getFlatConfigSpecial(optionsConfig)
      .map { case (k, v) => (k, v.toString) }
  }

  def getExtraOptions(options: Map[String, String],
                      prefix: String): Map[String, String] = {
    val start = s"$prefix."
    val len = start.length
    options.flatMap {
      case (k, v) => if (k.startsWith(start)) {
        Some(k.substring(len) -> v)
      } else {
        None
      }
    }
  }

  def getExtraConfig(conf: Config, prefix: String): Config = {
    if (conf.hasPath(prefix)) {
      conf.getConfig(prefix)
    } else {
      ConfigFactory.empty()
    }
  }

  def renderExtraOptions(extraOptions: Map[String, String],
                         redactedWords: Set[String] = Set.empty[String])
                        (action: String => Unit): Unit = {
    if (extraOptions.nonEmpty) {

      extraOptions.foreach { case (key, value) =>
        val v = renderRedactedKeyValue(key, value, redactedWords)

        action(v)
      }
    }
  }

  def renderRedactedKeyValue(key: String, value: String, redactedWords: Set[String]): String = {
    val p = "\""
    val lowerCaseKey = key.toLowerCase()
    if (redactedWords.exists(word => lowerCaseKey.contains(word))) {
      s"$key = [redacted]"
    } else {
      s"$key = $p$value$p"
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
   * Flattens TypeSafe config tree and returns the effective configuration.
   *
   * Config objects and lists are - instead of being kept as values - further flattened into keys such
   * that every key represents a primitive value or an array of primitive values.
   *
   * @return the flattened configuration as a map
   */
  def getFlatConfigOfPrimitiveValues(conf: Config): Map[String, AnyRef] = {
    def isConfigValuePrimitive(configValue: ConfigValue): Boolean = {
      val valueType = configValue.valueType()

      valueType == ConfigValueType.NUMBER || valueType == ConfigValueType.BOOLEAN ||
      valueType == ConfigValueType.STRING || valueType == ConfigValueType.NULL
    }

    def isConfigListPrimitive(configList: ConfigList): Boolean = {
      configList.asScala.forall(isConfigValuePrimitive)
    }

    def flattenConfigValue(configKey: String, configValue: ConfigValue): Map[String, AnyRef] = {
      configValue match {
        case configObject: ConfigObject =>
          flattenConfigObject(configKey, configObject)
        case configList: ConfigList if !isConfigListPrimitive(configList) =>
          flattenConfigList(configKey, configList)
        case _ =>
          Map(configKey -> configValue.unwrapped())
      }
    }

    def flattenConfigObject(configKey: String, configObject: ConfigObject): Map[String, AnyRef] = {
      configObject.asScala.toMap
        .flatMap {
          case (objectKey, configValue) => flattenConfigValue(s"$configKey.$objectKey", configValue)
        }
    }

    def flattenConfigList(configKey: String, configList: ConfigList): Map[String, AnyRef] = {
      configList.asScala.zipWithIndex
        .flatMap {
          case (configValue, index) => flattenConfigValue(s"$configKey[$index]", configValue)
        }
        .toMap
    }

    conf.entrySet().asScala.flatMap({ entry =>
      flattenConfigValue(entry.getKey, entry.getValue)
    }).toMap
  }

  def convertToMap(conf: Config): Map[String, AnyRef] = {
    def convertValue(configValue: ConfigValue): AnyRef = {
      configValue match {
        case configObject: ConfigObject => configObject.asScala.mapValues(convertValue).toMap
        case configList: ConfigList => configList.asScala.map(convertValue).toList
        case _ => configValue.unwrapped()
      }
    }

    val rootConfigObject = conf.root().asScala

    rootConfigObject.mapValues(convertValue).toMap
  }

  /**
    * Renders the effective configuration while redacting sensitive keys
    * in Java Properties format.
    *
    * @param keysToRedact   A set of keys for which values shouldn't be logged.
    * @param tokensToRedact A set of words in a key for which values shouldn't be logged.
    */
  def renderEffectiveConfigProps(conf: Config,
                                 keysToRedact: Set[String] = Set(),
                                 tokensToRedact: Set[String] = Set()): String = {
    val redactedFlatConfig = getRedactedFlatConfig(
      getFlatConfigOfPrimitiveValues(
        getRedactedConfig(conf, keysToRedact)),
      tokensToRedact)

    redactedFlatConfig.map {
      case (k, v) => s"$k = $v"
    }.toArray
      .sortBy(identity)
      .mkString("\n")
  }

  /**
    * Renders the effective configuration while redacting sensitive keys
    * in HOCON format.
    *
    * @param keysToRedact A set of keys for which values shouldn't be logged.
    */
  def renderEffectiveConfigHocon(conf: Config,
                                 keysToRedact: Set[String] = Set()): String = {
    val redactedConfig = getRedactedConfig(conf, keysToRedact)

    val renderOptions = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)

    redactedConfig.root().render(renderOptions)
  }

  /**
    * Logs the effective configuration while redacting sensitive keys
    * in Properties format.
    *
    * @param conf           A configuration.
    * @param keysToRedact   A set of keys for which values shouldn't be logged.
    * @param tokensToRedact A set of tokens which makes keys sensitive it it contains one of them.
    */
  def logEffectiveConfigProps(conf: Config,
                              keysToRedact: Set[String] = Set(),
                              tokensToRedact: Set[String] = Set()): Unit = {
    val rendered = renderEffectiveConfigProps(conf, keysToRedact, tokensToRedact)
    val pipelineNameOpt = getOptionString(conf, PIPELINE_NAME_KEY)

    pipelineNameOpt match {
      case Some(pipelineName) => log.info(s"Effective configuration for '$pipelineName':\n$rendered")
      case None               => log.info(s"Effective configuration:\n$rendered")
    }
  }

  /**
    * Puts a config key and value to the system properties if it is not defined there.
    *
    * @param conf A configuration.
    * @param key  Configuration key.
    */
  def setSystemPropertyStringFallback(conf: Config, key: String): Unit = {
    if (System.getProperty(key) == null && conf.hasPath(key)) {
      System.setProperty(key, conf.getString(key))
    }
  }

  /**
    * Puts a file location from a configuration to the system properties ensuring the file exists.
    *
    * A file provided can be at an absolute path (for client mode), e.g. /home/aabb/file.conf,
    * but other locations are investigated as well in this order.
    *
    * - The exact path passed (/home/aabb/file.conf).
    * - The file in the current directory, when spark-submit uses --files with aliases (file.conf).
    *
    * @param conf A configuration.
    * @param key  Configuration key.
    */
  def setSystemPropertyFileFallback(conf: Config, key: String): Unit = {
    if (System.getProperty(key) == null && conf.hasPath(key)) {
      val pathFileName = conf.getString(key)
      if (Files.exists(Paths.get(pathFileName))) {
        log.info(s"File exists: $pathFileName")
        System.setProperty(key, pathFileName)
      } else {
        log.info(s"File does not exist: $pathFileName")
        val fileNameInCurDir = Paths.get(pathFileName).getFileName
        if (Files.exists(fileNameInCurDir)) {
          log.info(s"File exists: ${fileNameInCurDir.toString} (in the current directory, not in $pathFileName)")
          System.setProperty(key, fileNameInCurDir.toString)
        } else {
          log.error(s"File does not exist: $pathFileName (nor ${fileNameInCurDir.toString} in the current directory)")
        }
      }
    }
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

  /**
    * Converts a configuration to Java properties format.
    * @param conf A configuration.
    * @return a Java properties object.
    */
  def toProperties(conf: Config): Properties = {
    val flatConfig = ConfigUtils.getFlatConfig(conf)
    val props = new Properties()
    flatConfig.foreach { case (k, v) =>
      props.setProperty(k, v.toString)
    }
    props
  }

  /**
    * Converts a configuration to YAML format.
    * @param conf A configuration.
    * @return a string representation of a YAML configuration.
    */
  def toYaml(conf: Config): String = {
    def getPath(parentPath: String, childItem: String): String = {
      if (parentPath.isEmpty) {
        childItem
      } else {
        s"$parentPath.$childItem"
      }
    }

    // Processes a configuration item depending on its type
    def processConfigItem(pad: String, path: String, node: AnyRef): String = {
      node match {
        case m: java.util.Map[String @unchecked, AnyRef @unchecked] =>
          processStruct(pad, path, m.asScala)
        case a: java.util.ArrayList[AnyRef @unchecked] =>
          s"$pad$path:${processArray(pad, path, a.toArray)}"
        case s =>
          val str = escapeString(s.toString)
          s"$pad$path: $str"
      }
    }

    def processArray(pad: String, path: String, array: Seq[AnyRef]): String = {
      var isScalar = true

      val values = array.map {
        case o: java.util.Map[String @unchecked, AnyRef @unchecked] =>
          isScalar = false
          s"$pad- ${trimLeft(processConfigItem(pad, "", o))}"
        case o: java.util.ArrayList[AnyRef @unchecked] =>
          isScalar = false
          s"$pad-${processArray(pad, path, o.toArray)}"
        case s =>
          escapeString(s.toString)
      }

      if (isScalar) {
        values.mkString(" [ ", ", ", " ]")
      } else {
        s"\n${values.mkString("\n")}"
      }
    }

    // HOCON structs are unordered key-value pairs, represented as a Map
    def processStruct(pad: String, path: String, obj: mutable.Map[String, AnyRef], isRoot: Boolean = false): String = {
      if (obj.size < 1) {
        ""
      } else if (obj.size == 1) {
        val (key, w) = obj.head
        processConfigItem(pad, getPath(path, key), w)
      } else {
        val values = obj.toArray.sortBy(a => a._1).map { case (key, w) =>
          val newPad = if (isRoot) "" else "  "
          processConfigItem(pad + newPad, key, w)
        }.mkString("\n")
        if (path.isEmpty) {
          values
        } else {
          s"$pad$path:\n$values"
        }
      }
    }

    val root = conf.root().unwrapped().asScala

    processStruct("", "", root, isRoot = true)
  }

  /**
    * Writes the contents of a configuration to a file in HOCON format.
    *
    * @param conf     A configuration.
    * @param fileName The output file name.
    */
  def writeConfigToFile(conf: Config, fileName: String): Unit = {
    val pw = new PrintWriter(new File(fileName))
    pw.write(conf.root().render())
    pw.close()
  }
}
