package za.co.absa.pramen.core.databricks

import com.typesafe.config.Config
import za.co.absa.pramen.core.pipeline.PythonTransformationJob.{INFO_DATE_VAR, METASTORE_CONFIG_VAR, PYTHON_CLASS_VAR}
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

class PramenPyJobTemplate(config: Config, pythonClass: String, metastoreConfigLocation: String, infoDate: LocalDate) {
  import PramenPyJobTemplate._

  def render(): Map[String, Any] = {
    createBaseDefinition ++ createNotebookTaskDefinition ++ createClusterDefinition
  }

  private def createBaseDefinition: Map[String, Any] = {
    Map(
      "run_name" -> s"Pramen-Py $pythonClass ($infoDate)"
    )
  }

  private def createClusterDefinition: Map[String, Any] = {
    if (config.hasPath(EXISTING_CLUSTER_ID_KEY)) {
      Map(
        "existing_cluster_id" -> config.getString(EXISTING_CLUSTER_ID_KEY)
      )
    } else {
      val newClusterDefinition = config.getConfig(NEW_CLUSTER_KEY)
      ConfigUtils.unwrap(newClusterDefinition)
    }
  }

  private def createNotebookTaskDefinition: Map[String, Any] = {
    val notebookTaskDefinition = ConfigUtils.unwrap(config.getConfig(NOTEBOOK_TASK_KEY))

    val baseParameters = notebookTaskDefinition("base_parameters") match {
      case parameters: Map[String, String] => parameters.mapValues(replaceParameters)
      case other => other
    }

    notebookTaskDefinition ++ Map("base_paramaters" -> baseParameters)
  }

  private def replaceParameters(template: String): String = {
    template
      .replace(PYTHON_CLASS_VAR, pythonClass)
      .replace(METASTORE_CONFIG_VAR, metastoreConfigLocation)
      .replace(INFO_DATE_VAR, infoDate.toString)
  }

}

object PramenPyJobTemplate {
  val NEW_CLUSTER_KEY = "pramen.py.databricks.new.cluster"
  val EXISTING_CLUSTER_ID_KEY = "pramen.py.databricks.existing.cluster.id"
  val NOTEBOOK_TASK_KEY = "pramen.py.notebook.task"
}