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

package za.co.absa.pramen.core.metastore

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{DataFormat, Query}
import za.co.absa.pramen.core.metastore.model.DataFormatKind

object FormatDetectors {
  private val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  def detectFormat(formats: Seq[DataFormat])(implicit spark: SparkSession): Option[DataFormat] = {
    if (formats.length == 1) {
      Option(formats.head)
    } else if (formats.isEmpty) {
      throw new IllegalArgumentException(s"No formats specified for the metastore table.")
    } else {
      val path = formats.flatMap(getPathFromFormat).headOption
      path match {
        case Some(p) =>
          detectFormatKind(p).map(selectFormat(formats, _))
        case None    =>
          throw new IllegalArgumentException(s"Cannot detect format if path is not specified.")
      }
    }
  }

  /**
    * Selects the data format definition that corresponds to the given data format kind.
    *
    * The first format in the list that matches the requested kind is returned.
    * An exception is thrown when detected format is not on the list.
    *
    * @param formats the candidate data format definitions to choose from.
    * @param kind    the detected kind of data format to select.
    * @return the first data format definition from the list that matches the given kind.
    * @throws IllegalArgumentException if no data format of the requested kind is present in the list.
    */
  def selectFormat(formats: Seq[DataFormat], kind: DataFormatKind): DataFormat = {
    kind match {
      case DataFormatKind.Parquet =>
        if (formats.exists(_.isInstanceOf[DataFormat.Parquet]))
          formats.find(_.isInstanceOf[DataFormat.Parquet]).get
        else
          throw new IllegalArgumentException(s"Detected 'parquet' as data format, but it is not in the list: ${formats.map(_.name).mkString(", ")}")
      case DataFormatKind.Iceberg =>
        if (formats.exists(_.isInstanceOf[DataFormat.Iceberg]))
          formats.find(_.isInstanceOf[DataFormat.Iceberg]).get
        else
          throw new IllegalArgumentException(s"Detected 'iceberg' as data format, but it is not in the list: ${formats.map(_.name).mkString(", ")}")
      case DataFormatKind.Delta   =>
        if (formats.exists(_.isInstanceOf[DataFormat.Delta]))
          formats.find(_.isInstanceOf[DataFormat.Delta]).get
        else
          throw new IllegalArgumentException(s"Detected 'delta' as data format, but it is not in the list: ${formats.map(_.name).mkString(", ")}")
    }
  }

  /**
    * Extracts the storage path from the given data format definition, if available.
    *
    * The path is resolved as follows:
    *  - For Parquet, the configured path is returned.
    *  - For Iceberg, the optional table location is returned.
    *  - For Delta, the path is returned only if the query is defined as a path; otherwise `None` is returned.
    *
    * @param format the data format to extract the path from.
    * @return the storage path of the data format, or `None` if the format does not define a path.
    * @throws IllegalArgumentException if the data format is not supported for format auto-detection.
    */
  def getPathFromFormat(format: DataFormat): Option[String] = {
    format match {
      case f: DataFormat.Parquet => Option(f.path)
      case f: DataFormat.Iceberg => f.location
      case f: DataFormat.Delta   =>
        f.query match {
          case Query.Path(path) => Option(path)
          case _                => None
        }
      case f                     => throw new IllegalArgumentException(s"Unsupported data format for auto-detection: ${f.name}")
    }
  }

  /**
    * Detects the kind of data format stored at the given path by inspecting its directory layout.
    *
    * The detection rules are applied in the following order:
    *  - If the path does not exist, the format cannot be determined.
    *  - If the path contains a `_delta_log` directory, the format is detected as Delta.
    *  - If the path contains both `metadata` and `data` directories, the format is detected as Iceberg.
    *  - If the path contains neither a `metadata` nor a `data` directory, the format is detected as Parquet.
    *  - If only one of the `metadata` or `data` directories is present, the format cannot be determined.
    *
    * @param pathStr the path to the data location to inspect.
    * @param spark   the Spark session whose Hadoop configuration is used to access the file system.
    * @return the detected data format kind, or `None` if the path does not exist, is empty, or the format cannot be determined.
    */
  def detectFormatKind(pathStr: String)(implicit spark: SparkSession): Option[DataFormatKind] = {
    val path = new Path(pathStr)

    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (!fs.exists(path)) {
      None
    } else {
      val deltaLogPath = new Path(path, "_delta_log")
      val metadataPath = new Path(path, "metadata")
      val dataPath = new Path(path, "data")

      val hasMetadataFolder = fs.exists(metadataPath) && fs.getFileStatus(metadataPath).isDirectory
      val hasDataFolder = fs.exists(dataPath) && fs.getFileStatus(dataPath).isDirectory
      val hasDeltaLogFolder = fs.exists(deltaLogPath) && fs.getFileStatus(deltaLogPath).isDirectory

      if (hasDeltaLogFolder) {
        log.info(s"Path '$pathStr' detected as Delta format.")
        Some(DataFormatKind.Delta)
      } else if (hasMetadataFolder && hasDataFolder) {
        log.info(s"Path '$pathStr' detected as Iceberg format.")
        Some(DataFormatKind.Iceberg)
      } else if (!hasMetadataFolder && !hasDataFolder) {
        log.info(s"Path '$pathStr' detected as Parquet format.")
        Some(DataFormatKind.Parquet)
      } else {
        log.info(s"The format of the data in '$pathStr' cannot be detected.")
        None
      }
    }
  }
}
