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

package za.co.absa.pramen.core.databricks

import com.typesafe.config.Config
import za.co.absa.pramen.core.pipeline.PythonTransformationJob.{INFO_DATE_VAR, METASTORE_CONFIG_VAR, PYTHON_CLASS_VAR}
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

class PramenPyJobTemplate(config: Config, pythonClass: String, metastoreConfigLocation: String, infoDate: LocalDate) {
  import PramenPyJobTemplate._

  private[databricks] def render(): Map[String, Any] = {
    val jobConfig = config.getConfig(JOB_KEY)
    val jobMap = ConfigUtils.convertToMap(jobConfig)
    val jobWithResolvedVariables = replaceVariablesInMap(jobMap)

    jobWithResolvedVariables
  }

  private[databricks] def replaceVariablesInMap(map: Map[String, Any]): Map[String, Any] = {
    // in typesafe Config, keys can be set to null (this function will be maily called on Maps created from
    // typesafe Config objects)
    // we use this function to filter the nulls out
    def isNull(x: Any): Boolean = x == null

    def replaceVariablesInText(text: String): String = {
      text
        .replace(PYTHON_CLASS_VAR, pythonClass)
        .replace(METASTORE_CONFIG_VAR, metastoreConfigLocation)
        .replace(INFO_DATE_VAR, infoDate.toString)
    }

    def replaceVariables(value: Any): Any = {
      value match {
        case map: Map[_, _] => map
          .filterNot(keyVal => isNull(keyVal._2))
          .mapValues(replaceVariables)
          .toMap
        case seq: Seq[_] => seq
          .map(replaceVariables)
          .filterNot(isNull)
          .toList
        case string: String => replaceVariablesInText(string)
        case other: Any => other
      }
    }

    map
      .filterNot(keyVal => isNull(keyVal._2))
      .mapValues(replaceVariables)
      .toMap
  }

}

object PramenPyJobTemplate {
  val JOB_KEY = "pramen.py.databricks.job"

  def render(config: Config, pythonClass: String, metastoreConfigLocation: String, infoDate: LocalDate): Map[String, Any] = {
    val template = new PramenPyJobTemplate(config, pythonClass, metastoreConfigLocation, infoDate)

    template.render()
  }
}
