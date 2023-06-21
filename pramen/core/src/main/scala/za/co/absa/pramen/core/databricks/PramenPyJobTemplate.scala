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
  }

}

object PramenPyJobTemplate {
  val JOB_KEY = "pramen.py.databricks.job"

  def render(config: Config, pythonClass: String, metastoreConfigLocation: String, infoDate: LocalDate): Map[String, Any] = {
    val template = new PramenPyJobTemplate(config, pythonClass, metastoreConfigLocation, infoDate)

    template.render()
  }
}