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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory
import za.co.absa.pramen.extras.query.QueryExecutor
import za.co.absa.pramen.extras.utils.FsUtils
import za.co.absa.pramen.extras.utils.PartitionUtils.unpackCustomPartitionPattern

import java.time.LocalDate
import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class EnceladusUtils(rawPartitionPattern: String,
                     publishPartitionPattern: String,
                     infoDateColumn: String)
                    (implicit spark: SparkSession,
                     queryExecutor: QueryExecutor) {
  import InfoVersionStatus._

  private val log = LoggerFactory.getLogger(this.getClass)

  val rawVersionRegEx: Regex = "^.*v(\\d+)$".r
  val publishVersionRegEx: Regex = "^.*=(\\d+)$".r

  def getNextEnceladusVersion(infoDate: LocalDate,
                              rawBasePath: Path,
                              publishBasePathOpt: Option[Path],
                              hiveTableOpt: Option[String]): Try[Int] = {
    val maxVersionInPublish = getMaxEnceladusVersion(infoDate, rawBasePath, publishBasePathOpt, hiveTableOpt)

    generateNewInfoVersion(maxVersionInPublish)
  }

  def getMaxEnceladusVersion(infoDate: LocalDate,
                             rawBasePath: Path,
                             publishBasePathOpt: Option[Path],
                             hiveTableOpt: Option[String]): InfoVersionStatus = {
    publishBasePathOpt match {
      case Some(publishBasePath) =>
        log.info(s"Detecting info version from the publish base path: $publishBasePath")
        getMaxVersionInPublish(publishBasePath, infoDateColumn, infoDate)
      case None                  =>
        hiveTableOpt match {
          case Some(hiveTable) =>
            log.info(s"Detecting info version from the hive table: $hiveTable")
            getMaxVersionFromHive(hiveTable, infoDateColumn, infoDate)
          case None            =>
            DetectionFailure(new IllegalArgumentException(s"No publish path or hive table specified for $rawBasePath."))
        }
    }
  }

  def generateNewInfoVersion(existingInfoVersionStatus: InfoVersionStatus): Try[Int] = {
    existingInfoVersionStatus match {
      case Detected(versionInPublish) =>
        log.info(s"Detected info version in publish: $versionInPublish")
        Success(versionInPublish + 1)
      case NotPresent                 =>
        Success(1)
      case DetectionFailure(ex)       =>
        Failure(ex)
    }
  }

  def getMaxVersionInRaw(rawBasePath: Path,
                         infoDateColumn: String,
                         infoDate: LocalDate): InfoVersionStatus = {

    val rawPath = getParentPartitionPath(rawBasePath, rawPartitionPattern, infoDateColumn, infoDate)

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, rawBasePath.toString)

    if (!fsUtils.exists(rawBasePath)) {
      return DetectionFailure(new IllegalArgumentException(s"Raw path does not exist: $rawBasePath"))
    }

    if (!fsUtils.exists(rawPath)) {
      log.info(s"Raw path does not exist: $rawPath")
      return NotPresent
    }

    getMaxVersionFromDirs(rawPath, rawVersionRegEx, fsUtils)
  }

  def getMaxVersionInPublish(publishBasePath: Path,
                             infoDateColumn: String,
                             infoDate: LocalDate): InfoVersionStatus = {
    Try {
      val publishPath = getParentPartitionPath(publishBasePath, publishPartitionPattern, infoDateColumn, infoDate)

      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, publishBasePath.toString)

      if (!fsUtils.exists(publishPath.getParent)) {
        throw new IllegalArgumentException(s"Publish path does not exist: ${publishPath.getParent}")
      }

      if (fsUtils.exists(publishPath)) {
        getMaxVersionFromDirs(publishPath, publishVersionRegEx, fsUtils)
      } else {
        log.info(s"Publish path does not exist: $publishPath")
        NotPresent
      }
    }
  }

  private[extras] def getMaxVersionFromHive(hiveTable: String,
                                            infoDateColumn: String,
                                            infoDate: LocalDate): InfoVersionStatus = {
    implicit val encoder: ExpressionEncoder[String] = ExpressionEncoder[String]
    val query = s"SHOW PARTITIONS $hiveTable"
    val startingWith = s"$infoDateColumn=$infoDate"

    Try {
      queryExecutor
        .query(query)
        .filter(col("partition").contains(startingWith))
        .as[String]
        .collect()
    }.map { partitions =>
      if (partitions.isEmpty) {
        NotPresent
      } else {
        getMaxVersionFromList(partitions, publishVersionRegEx)
      }
    }
  }

  private[extras] def getMaxVersionFromDirs(rawPath: Path,
                                            versionExtractRegEx: Regex,
                                            fsUtils: FsUtils): InfoVersionStatus = {
    Try {
      fsUtils.getDirectories(rawPath)
    }.map { partitions =>
      getMaxVersionFromList(partitions.map(_.toString), versionExtractRegEx)
    }
  }

  private[extras] def getMaxVersionFromList(partitions: Seq[String], versionExtractRegEx: Regex): InfoVersionStatus = {
    Try {
      val versions = partitions.flatMap(partition => partition match {
        case versionExtractRegEx(version) => Some(version.toInt)
        case _                            => None
      })
      if (versions.isEmpty) {
        NotPresent
      } else {
        Detected(versions.max)
      }
    }
  }

  private[extras] def getParentPartitionPath(basePath: Path,
                                             pathPattern: String,
                                             infoDateColumn: String,
                                             infoDate: LocalDate): Path = {
    val partitionPath = unpackCustomPartitionPattern(pathPattern, infoDateColumn, infoDate, 1)

    val specificPath = new Path(basePath, partitionPath)

    specificPath.getParent
  }

  implicit private[extras] def fromTry(tryValue: Try[InfoVersionStatus]): InfoVersionStatus = {
    tryValue match {
      case Success(value) => value
      case Failure(ex)    => DetectionFailure(ex)
    }
  }
}
