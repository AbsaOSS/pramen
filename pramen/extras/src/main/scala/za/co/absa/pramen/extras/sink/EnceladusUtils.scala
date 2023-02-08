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

package za.co.absa.pramen.extras.sink

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.extras.query.QueryExecutor
import za.co.absa.pramen.extras.utils.FsUtils
import za.co.absa.pramen.extras.utils.PartitionUtils.unpackCustomPartitionPattern

import java.time.LocalDate
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class EnceladusUtils(rawPartitionPattern: String,
                     publishPartitionPattern: String,
                     infoDateColumn: String)
                    (implicit spark: SparkSession,
                       queryExecutor: QueryExecutor) {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getNextEnceladusVersion(infoDate: LocalDate,
                              rawBasePath: Path,
                              publishBasePathOpt: Option[Path],
                              hiveTableOpt: Option[String]): Try[Int] = {
    val rawMaxVersion = getMaxVersionInRaw(rawBasePath, rawPartitionPattern, infoDateColumn, infoDate).getOrElse(0)

    val maxVersionInPublish = publishBasePathOpt match {
      case Some(publishBasePath) =>
        log.info(s"Detecting info version from the publish base path: $publishBasePath")
        getMaxVersionInPublish(publishBasePath, publishPartitionPattern, infoDateColumn, infoDate)
      case None                  =>
        hiveTableOpt match {
          case Some(hiveTable) =>
            log.info(s"Detecting info version from the hive table: $hiveTable")
            getMaxVersionInPublish(hiveTable, publishPartitionPattern, infoDateColumn, infoDate)
          case None            =>
            Failure(new IllegalArgumentException(s"No publish path or hive table specified for $rawBasePath."))
        }
    }

    maxVersionInPublish.map {
      case Some(versionInPublish) =>
        versionInPublish + 1
      case None =>
        rawMaxVersion + 1
    }
  }

  def getMaxVersionInRaw(rawBasePath: Path,
                         partitionPattern: String,
                         infoDateColumn: String,
                         infoDate: LocalDate): Option[Int] = {

    val rawPath = getParentPartitionPath(rawBasePath, partitionPattern, infoDateColumn, infoDate)

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, rawBasePath.toString)

    if (!fsUtils.exists(rawPath)) {
      log.info(s"Raw path $rawPath does not exist.")
      return None
    }

    val versionR = "^v(\\d+)$".r

    getMaxVersionFromDirs(rawPath, versionR, fsUtils).get
  }

  def getMaxVersionInPublish(publishBasePath: Path,
                             partitionPattern: String,
                             infoDateColumn: String,
                             infoDate: LocalDate): Try[Option[Int]] = {
    val rawPath = getParentPartitionPath(publishBasePath, partitionPattern, infoDateColumn, infoDate)

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, publishBasePath.toString)

    if (!fsUtils.exists(rawPath)) {
      log.info(s"Publish path $rawPath does not exist.")
      return Success(None)
    }

    val versionR = "^.*=(\\d+)$".r

    getMaxVersionFromDirs(rawPath, versionR, fsUtils)
  }

  private def getMaxVersionFromDirs(rawPath: Path, versionExtractRegEx: Regex, fsUtils: FsUtils): Try[Option[Int]] = {
    Try {
      val versions = fsUtils.getDirectories(rawPath)
        .flatMap(_.getName match {
          case versionExtractRegEx(version) => Some(version.toInt)
          case _                 => None
        })
      if (versions.isEmpty) {
        None
      } else {
        Option(versions.max)
      }
    }
  }

  def getMaxVersionInPublish(hiveTable: String,
                             partitionPattern: String,
                             infoDateColumn: String,
                             infoDate: LocalDate): Try[Option[Int]] = {
    // spark.sql("show partitions dm_absa_advantage.behrew_cashsend_daily").filter(col("partition").startsWith("process_date=2021-06")).show(false)
    val query = s"SHOW PARTITIONS $hiveTable"
    Failure(new NotImplementedError("Not implemented yet"))
  }

  private[extras] def getParentPartitionPath(basePath: Path,
                                             pathPattern: String,
                                             infoDateColumn: String,
                                             infoDate: LocalDate): Path = {
    val partitionPath = unpackCustomPartitionPattern(pathPattern, infoDateColumn, infoDate, 1)

    val specificPath = new Path(basePath, partitionPath)

    specificPath.getParent
  }
}
