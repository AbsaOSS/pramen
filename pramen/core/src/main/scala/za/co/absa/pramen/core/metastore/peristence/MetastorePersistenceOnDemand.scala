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

package za.co.absa.pramen.core.metastore.peristence

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.task.{RunStatus, TaskRunner}
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate
import scala.collection.mutable

class MetastorePersistenceOnDemand(tempPath: String,
                                   tableName: String,
                                   cachePolicy: CachePolicy
                                  )(implicit spark: SparkSession) extends MetastorePersistence {
  val transientPersistence = new MetastorePersistenceTransient(tempPath, tableName, cachePolicy)

  import MetastorePersistenceOnDemand._

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    (infoDateFrom, infoDateTo) match {
      case (Some(from), Some(to)) if from == to =>
        if (MetastorePersistenceTransient.hasDataForTheDate(tableName, from)) {
          MetastorePersistenceTransient.getDataForTheDate(tableName, from)
        } else
          runOnDemandJob(tableName, from)
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException("Metastore 'on_demand' format does not support ranged queries.")
      case _ =>
        throw new IllegalArgumentException("Metastore 'on_demand' format requires info date for querying its contents.")
    }

  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    transientPersistence.saveTable(infoDate, df, numberOfRecordsEstimate)
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    throw new UnsupportedOperationException("On demand format does not support getting record count and size statistics.")
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate, hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("On demand format does not support Hive tables.")
  }

  override def repairHiveTable(hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("On demand format does not support Hive tables.")
  }
}

object MetastorePersistenceOnDemand {
  private val onDemandJobs = new mutable.HashMap[String, Job]()
  private var taskRunnerOpt: Option[TaskRunner] = None

  private[core] def setTaskRunner(taskRunner_ : TaskRunner): Unit = synchronized {
    taskRunnerOpt = Option(taskRunner_)
  }

  private[core] def addOnDemandJob(job: Job): Unit = synchronized {
    onDemandJobs += job.outputTable.name.toLowerCase -> job
  }

  private[core] def runOnDemandJob(outputTableName: String,
                                   infoDate: LocalDate)
                                  (implicit sparkSession: SparkSession): DataFrame = synchronized {
    val job = onDemandJobs.get(outputTableName.toLowerCase)
    job match {
      case Some(job) => runJob(job, infoDate)
      case None => throw new IllegalArgumentException(s"On-demand job with output table name '$outputTableName' not found or haven't registered yet.")
    }
  }

  private[core] def reset(): Unit = synchronized {
    onDemandJobs.clear()
    taskRunnerOpt = None
  }

  private[core] def runJob(job: Job,
                           infoDate: LocalDate)
                          (implicit sparkSession: SparkSession): DataFrame = {
    taskRunnerOpt match {
      case Some(taskRunner) =>
        taskRunner.runOnDemand(job, infoDate) match {
          case _: RunStatus.Succeeded => MetastorePersistenceTransient.getDataForTheDate(job.outputTable.name, infoDate)
          case _ => throw new IllegalStateException("On-demand job failed to run.")
        }
      case None =>
        throw new IllegalStateException("Task runner is not set.")
    }
  }
}
