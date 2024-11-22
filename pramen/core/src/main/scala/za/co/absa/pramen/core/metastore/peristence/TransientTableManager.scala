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

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.app.config.GeneralConfig.TEMPORARY_DIRECTORY_KEY
import za.co.absa.pramen.core.utils.FsUtils

import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.util.Random

object TransientTableManager {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val rawDataframes = new mutable.HashMap[MetastorePartition, DataFrame]()
  private val cachedDataframes = new mutable.HashMap[MetastorePartition, DataFrame]()
  private val persistedLocations = new mutable.HashMap[MetastorePartition, String]()
  private val schemas = new mutable.HashMap[String, StructType]()
  private var spark: SparkSession = _

  private[core] def addRawDataFrame(tableName: String, infoDate: LocalDate, df: DataFrame): (DataFrame, Option[Long]) = synchronized {
    spark = df.sparkSession
    val partition = getMetastorePartition(tableName, infoDate)

    log.info(s"Adding '$partition' to the non-cached data frames...")

    rawDataframes += partition -> df
    schemas += partition.tableName -> df.schema

    (df, None)
  }

  private[core] def addCachedDataframe(tableName: String, infoDate: LocalDate, df: DataFrame): (DataFrame, Option[Long]) = {
    val partition = getMetastorePartition(tableName, infoDate)

    val cachedDf = df.cache()

    log.info(s"Adding '$partition' to the cached data frames...")

    this.synchronized {
      spark = df.sparkSession
      cachedDataframes += partition -> cachedDf
      schemas += partition.tableName -> df.schema
    }

    (cachedDf, None)
  }

  private[core] def addPersistedDataFrame(tableName: String, infoDate: LocalDate, df: DataFrame, tempDir: String): (DataFrame,  Option[Long]) = {
    val partition = getMetastorePartition(tableName, infoDate)
    this.synchronized {
      spark = df.sparkSession
    }

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

    val partitionFolder = s"temp_partition_date=$infoDate"
    val uniquePath = createTimedTempDir(new Path(tempDir), fsUtils)
    val outputPath = new Path(uniquePath, partitionFolder).toString

    fsUtils.createDirectoryRecursive(new Path(outputPath))

    df.write.mode(SaveMode.Overwrite).parquet(outputPath)

    val sizeBytes = fsUtils.getDirectorySize(outputPath)

    log.info(s"Adding '$partition' to the persistent cache at '$outputPath'...")

    this.synchronized {
      persistedLocations += partition -> outputPath
      schemas += partition.tableName -> df.schema
    }

    (spark.read.parquet(outputPath), Option(sizeBytes))
  }

  private[core] def createTimedTempDir(parentDir: Path, fsUtils: FsUtils): Path = {
    fsUtils.createDirectoryRecursive(parentDir)

    var tempDir = new Path(parentDir, s"${Instant.now().toEpochMilli}_${Random.nextInt()}")

    while (fsUtils.exists(tempDir)) {
      Thread.sleep(1)
      tempDir = new Path(parentDir, s"${Instant.now().toEpochMilli}_${Random.nextInt()}")
    }

    fsUtils.fs.mkdirs(tempDir)

    tempDir
  }

  private[core] def hasDataForTheDate(tableName: String, infoDate: LocalDate): Boolean = synchronized {
    val partition = getMetastorePartition(tableName, infoDate)

    rawDataframes.contains(partition) || cachedDataframes.contains(partition) || persistedLocations.contains(partition)
  }

  private[core] def getDataForTheDate(tableName: String, infoDate: LocalDate)(implicit spark: SparkSession): DataFrame = {
    val partition = getMetastorePartition(tableName, infoDate)

    this.synchronized{
      if (rawDataframes.contains(partition)) {
        log.info(s"Using non-cached dataframe for '$tableName' for '$infoDate'...")
        return rawDataframes(partition)
      }
    }

    this.synchronized{
      if (cachedDataframes.contains(partition)) {
        log.info(s"Using cached dataframe for '$tableName' for '$infoDate'...")
        return cachedDataframes(partition)
      }
    }

    val pathOpt = this.synchronized{
      if (persistedLocations.contains(partition)) {
        val path = persistedLocations(partition)
        log.info(s"Reading persisted transient table from $path...")
        Option(path)
      } else {
        None
      }
    }

    pathOpt match {
      case Some(path) => return spark.read.parquet(path)
      case None => // nothing to do
    }

    getEmptyDfForTable(tableName).getOrElse(
      throw new IllegalStateException(s"No data for transient table '$tableName' for '$infoDate'")
    )
  }

  private[core] def getEmptyDfForTable(tableName: String): Option[DataFrame] = {
    val schemaOpt = getSchemaForTable(tableName)

    schemaOpt.map { schema =>
      val emptyRDD = spark.sparkContext.emptyRDD[Row]
      spark.createDataFrame(emptyRDD, schema)
    }
  }

  private[core] def getSchemaForTable(tableName: String): Option[StructType] = this.synchronized {
    schemas.get(tableName.toLowerCase)
  }

  private[core] def reset(): Unit = synchronized {
    rawDataframes.clear()
    cachedDataframes.foreach { case (_, df) => df.unpersist() }
    cachedDataframes.clear()
    schemas.clear()

    if (spark != null) {
      persistedLocations.foreach { case (_, path) =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

        log.info(s"Deleting $path...")
        fsUtils.deleteDirectoryRecursively(new Path(path))
      }
      persistedLocations.clear()
    }
  }

  private[core] def getMetastorePartition(tableName: String, infoDate: LocalDate): MetastorePartition = {
    MetastorePartition(tableName.toLowerCase, infoDate.toString)
  }

  private[core] def getTempDirectory(cachePolicy: CachePolicy, conf: Config): Option[String] = {
    if (cachePolicy == CachePolicy.Persist) {
      if (conf.hasPath(TEMPORARY_DIRECTORY_KEY) && conf.getString(TEMPORARY_DIRECTORY_KEY).nonEmpty) {
        Option(conf.getString(TEMPORARY_DIRECTORY_KEY)).map { basePath =>
          new Path(basePath, "cache").toString
        }
      } else {
        throw new IllegalArgumentException(s"Transient metastore tables with persist cache policy require temporary directory to be defined at: $TEMPORARY_DIRECTORY_KEY")
      }
    } else {
      None
    }
  }
}
