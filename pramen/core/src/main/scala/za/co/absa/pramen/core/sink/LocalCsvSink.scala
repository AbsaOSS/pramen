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

package za.co.absa.pramen.core.sink

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink}
import za.co.absa.pramen.core.sink.LocalCsvSink.OUTPUT_PATH_KEY
import za.co.absa.pramen.core.utils.FsUtils

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}

/**
  * This sink allows exporting data from the metastore to local CSV files.
  *
  * In order to use the sink you need to define sink parameters.
  * The only mandatory option is 'temp.hadoop.path'. It is the path intermediate CSV files will be stored
  * before copying to the final destination.
  *
  * Example sink definition:
  * {{{
  *  {
  *    name = "local_csv"
  *    factory.class = "za.co.absa.pramen.core.sink.LocalCsvSink"
  *
  *    temp.hadoop.path = "/tmp/csv_sink"
  *
  *    file.name.pattern = "FILE_@timestamp"
  *    file.name.timestamp.pattern = "yyyyMMdd_HHmmss"
  *
  *    column.name.transform = "make_upper"
  *
  *    date.format = "yyyy-MM-dd"
  *    timestamp.format = "yyyy-MM-dd HH:mm:ss Z"
  *
  *    option {
  *      sep = "|"
  *      quoteAll = "false"
  *      header = "true"
  *    }
  *  }
  * }}}
  *
  * Here is an example of a sink definition in a pipeline. As for any other operation you can specify
  * dependencies, transformations, filters and columns to select.
  *
  * {{{
  *  {
  *    name = "CSV sink"
  *    type = "sink"
  *    sink = "local_csv"
  *
  *    schedule.type = "daily"
  *
  *    dependencies = [
  *      {
  *        tables = [ dependent_table ]
  *        date.from = "@infoDate"
  *      }
  *    ]
  *
  *    tables = [
  *      {
  *        metastore.table = metastore_table
  *        output.path = "/local/csv/path"
  *
  *        # Date range to read the source table for. By default the job information date is used.
  *        # But you can define an arbitrary expression based on the information date.
  *        # More: see the section of documentation regarding date expressions, an the list of functions allowed.
  *        date {
  *          from = "@infoDate"
  *          to = "@infoDate"
  *        }
  *
  *        transformations = [
  *         { col = "col1", expr = "lower(some_string_column)" }
  *        ],
  *        filters = [
  *          "some_numeric_column > 100"
  *        ]
  *        columns = [ "col1", "col2", "col2", "some_numeric_column" ]
  *      }
  *    ]
  *  }
  * }}}
  *
  */
class LocalCsvSink(params: CsvConversionParams) extends Sink {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])
                   (implicit spark: SparkSession): Long = {

    if (!options.contains(OUTPUT_PATH_KEY)) {
      throw new IllegalArgumentException(s"Missing required parameter of LocalCsvSink: 'output.$OUTPUT_PATH_KEY'.")
    }

    val outputPath = options(OUTPUT_PATH_KEY)

    val count = df.count()

    if (count > 0) {
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, params.tempHadoopPath)
      val tempPath = getTempPath(fsUtils)
      val effectiveOptions = getEffectiveOptions(params.csvOptions)

      val transformedDf = applyColumnTransformations(df)

      convertDateTimeToString(transformedDf, params.dateFormat, params.timestampFormat)
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(effectiveOptions)
        .csv(tempPath.toString)

      val fileName = copyToLocal(tableName, infoDate, tempPath, outputPath, fsUtils)

      fsUtils.deleteDirectoryRecursively(tempPath)
      log.info(s"$count records saved to $fileName.")
      count
    } else {
      log.info(s"Notting to send to $outputPath.")
      0L
    }
  }

  private[core] def applyColumnTransformations(df: DataFrame): DataFrame = {
    params.columnNameTransform match {
      case ColumnNameTransform.NoChange  =>
        df
      case ColumnNameTransform.MakeUpper =>
        df.select(
          df.schema.fields.map(field => col(field.name).as(field.name.toUpperCase())): _*
        )
      case ColumnNameTransform.MakeLower =>
        df.select(
          df.schema.fields.map(field => col(field.name).as(field.name.toLowerCase())): _*
        )
    }
  }

  private[core] def convertDateTimeToString(df: DataFrame, datePattern: String, timestampPattern: String): DataFrame = {
    val columns = df.schema.map(field => {
      field.dataType match {
        case _: DateType      => date_format(col(field.name), datePattern).as(field.name)
        case _: TimestampType => date_format(col(field.name), timestampPattern).as(field.name)
        case _                => col(field.name)
      }
    })
    df.select(columns: _*)
  }

  private[core] def getEffectiveOptions(options: Map[String, String]): Map[String, String] = {
    val headerOptions = if (options.contains("header")) {
      None
    } else {
      Some("header" -> "true")
    }

    val quoteOptions = if (options.contains("quoteAll")) {
      None
    } else {
      Some("quoteAll" -> "true")
    }

    options ++ headerOptions ++ quoteOptions
  }

  private[core] def getTempPath(fsUtils: FsUtils): Path = {
    val tempBasePath = new Path(params.tempHadoopPath)
    fsUtils.getTempPath(tempBasePath)
  }

  private[core] def copyToLocal(tableName: String,
                                     infoDate: LocalDate,
                                     tempPathWithCSV: Path,
                                     localPath: String,
                                     fsUtils: FsUtils)(implicit spark: SparkSession): String = {
    val fileInHdfs = fsUtils.getFilesRecursive(tempPathWithCSV, "*.csv").head


    val fileBase = getFileName(params.fileNamePattern, params.fileNameTimestampPattern, tableName, infoDate)

    val finalFileName = Paths.get(localPath, s"$fileBase.csv").toString

    log.info(s"Copying $fileInHdfs to $finalFileName...")
    fsUtils.copyToLocal(fileInHdfs, new Path(finalFileName), overwrite = false)
    finalFileName
  }

  private[core] def getFileName(fileNamePattern: String, timestampPattern: String, tableName: String, infoDate: LocalDate): String = {
    val timestampFmt: DateTimeFormatter = DateTimeFormatter.ofPattern(timestampPattern)

    val ts = timestampFmt.format(ZonedDateTime.now())

    fileNamePattern
      .replace("@tableName", tableName)
      .replace("@infoDate", infoDate.toString)
      .replace("@timestamp", ts)
  }
}

object LocalCsvSink extends ExternalChannelFactory[LocalCsvSink] {
  val OUTPUT_PATH_KEY = "path"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): LocalCsvSink = {
    val params = CsvConversionParams.fromConfig(conf, parentPath)

    new LocalCsvSink(params)
  }
}
