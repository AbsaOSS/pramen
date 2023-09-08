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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.FsUtils
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate
import scala.collection.mutable

class MetastorePersistenceTransient(tempPath: String,
                                    tableName: String,
                                    cachePolicy: CachePolicy
                                   )(implicit spark: SparkSession) extends MetastorePersistence {
  import MetastorePersistenceTransient._

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    (infoDateFrom, infoDateTo) match {
      case (Some(from), Some(to)) if from == to =>
        getDataForTheDate(tableName, from)
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException("Metastore 'transient' format does not support ranged queries.")
      case _ =>
        throw new IllegalArgumentException("Metastore 'transient' format requires info date for querying its contents.")
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val (dfOut, sizeBytesOpt) = cachePolicy match {
      case CachePolicy.NoCache =>
        addRawDataFrame(tableName, infoDate, df)
      case CachePolicy.Cache =>
        addCachedDataframe(tableName, infoDate, df)
      case CachePolicy.Persist =>
        addPersistedDataFrame(tableName, infoDate, df, tempPath)
    }

    val recordCount = numberOfRecordsEstimate match {
      case Some(n) => n
      case None => dfOut.count()
    }

    MetaTableStats(
      recordCount,
      sizeBytesOpt
    )
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    throw new UnsupportedOperationException("Transient format does not support getting record count and size statistics.")
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Transient format does not support Hive tables.")
  }

  override def repairHiveTable(hiveTableName: String,
                               queryExecutor: QueryExecutor,
                               hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Transient format does not support Hive tables.")
  }
}

object MetastorePersistenceTransient {
  private val log = LoggerFactory.getLogger(this.getClass)

  case class MetastorePartition(tableName: String, infoDateStr: String)

  private val rawDataframes = new mutable.HashMap[MetastorePartition, DataFrame]()
  private val cachedDataframes = new mutable.HashMap[MetastorePartition, DataFrame]()
  private val persistedLocations = new mutable.HashMap[MetastorePartition, String]()
  private var spark: SparkSession = _

  private[core] def addRawDataFrame(tableName: String, infoDate: LocalDate, df: DataFrame): (DataFrame, Option[Long]) = synchronized {
    spark = df.sparkSession
    val partition = getMetastorePartition(tableName, infoDate)

    rawDataframes += partition -> df

    (df, None)
  }

  private[core] def addCachedDataframe(tableName: String, infoDate: LocalDate, df: DataFrame): (DataFrame, Option[Long]) = {
    val partition = getMetastorePartition(tableName, infoDate)

    val cachedDf = df.cache()

    this.synchronized {
      spark = df.sparkSession
      cachedDataframes += partition -> cachedDf
    }

    (cachedDf, None)
  }

  private[core] def addPersistedDataFrame(tableName: String, infoDate: LocalDate, df: DataFrame, tempDir: String): (DataFrame,  Option[Long]) = {
    val partition = getMetastorePartition(tableName, infoDate)
    this.synchronized {
      spark = df.sparkSession
    }

    val partitionFolder = s"temp_partition_date=$infoDate"
    val outputPath = new Path(tempDir, partitionFolder).toString

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputPath)

    fsUtils.createDirectoryRecursive(new Path(outputPath))

    df.write.mode(SaveMode.Overwrite).parquet(outputPath)

    val sizeBytes = fsUtils.getDirectorySize(outputPath)

    this.synchronized {
      persistedLocations += partition -> outputPath
    }

    (spark.read.parquet(outputPath), Option(sizeBytes))
  }

  private[core] def getDataForTheDate(tableName: String, infoDate: LocalDate)(implicit spark: SparkSession): DataFrame = synchronized {
    val partition = getMetastorePartition(tableName, infoDate)

    if (MetastorePersistenceTransient.rawDataframes.contains(partition)) {
      log.info(s"Using non-cached dataframe for '$tableName' for '$infoDate'...")
      MetastorePersistenceTransient.rawDataframes(partition)
    } else if (MetastorePersistenceTransient.cachedDataframes.contains(partition)) {
      log.info(s"Using cached dataframe for '$tableName' for '$infoDate'...")
      MetastorePersistenceTransient.cachedDataframes(partition)
    } else if (MetastorePersistenceTransient.persistedLocations.contains(partition)) {
      val path = MetastorePersistenceTransient.persistedLocations(partition)
      log.info(s"Reading persisted transient table from $path...")
      spark.read.parquet(path)
    } else {
      throw new IllegalStateException(s"No data for transient table '$tableName' for '$infoDate'")
    }
  }

  private[core] def cleanup(): Unit = synchronized {
    rawDataframes.clear()
    cachedDataframes.foreach { case (_, df) => df.unpersist() }
    cachedDataframes.clear()

    if (spark != null) {
      persistedLocations.foreach { case (_, path) =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

        log.info(s"Deleting $path...")
        fsUtils.deleteDirectoryRecursively(new Path(path))
      }
      persistedLocations.clear()
    }
  }

  private def getMetastorePartition(tableName: String, infoDate: LocalDate): MetastorePartition = {
    MetastorePersistenceTransient.MetastorePartition(tableName.toLowerCase, infoDate.toString)
  }
}
