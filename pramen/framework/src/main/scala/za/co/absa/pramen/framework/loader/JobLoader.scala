package za.co.absa.pramen.framework.loader

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.{Job, JobFactory}
import za.co.absa.pramen.framework.utils.ClassLoaderUtils

import scala.util.control.NonFatal

object JobLoader {
  def loadJob(factoryName: String, config: Config, spark: SparkSession): Option[Job] = {
    val factory = ClassLoaderUtils.loadSingletonClassOfType[JobFactory[Job]](factoryName)
    try {
      Option(factory.apply(config, spark))
    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException(s"Unable to build a job using its factory: $factoryName", ex)
    }
  }

  def loadJobs(jobFactoryClasses: Seq[String])
              (implicit conf: Config, spark: SparkSession): Seq[Job] = {
    jobFactoryClasses.map(className =>
      JobLoader.loadJob(className, conf, spark) match {
        case Some(job) => job
        case None => throw new IllegalArgumentException(s"Unable to load Job from factory $className.")
      }
    )
  }
}
