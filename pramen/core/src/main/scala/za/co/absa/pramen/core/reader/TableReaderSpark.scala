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

package za.co.absa.pramen.core.reader

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, TableReader}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class TableReaderSpark(formatOpt: Option[String],
                       schemaOpt: Option[String],
                       hasInfoDateColumn: Boolean,
                       infoDateColumn: String,
                       infoDateFormat: String = "yyyy-MM-dd",
                       options: Map[String, String] = Map.empty[String, String]
                      )(implicit spark: SparkSession) extends TableReader {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    if (hasInfoDateColumn) {
      if (infoDateBegin.equals(infoDateEnd)) {
        log.info(s"Reading COUNT(*) FROM ${query.query} WHERE $infoDateColumn='${dateFormatter.format(infoDateBegin)}'")
        getDailyDataFrame(query, infoDateBegin).count()
      } else {
        log.info(s"Reading COUNT(*) FROM ${query.query} WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
        getFilteredDataFrame(query, infoDateBegin, infoDateEnd).count()
      }
    } else {
      getBaseDataFrame(query).count()
    }
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    if (hasInfoDateColumn) {
      if (infoDateBegin.equals(infoDateEnd)) {
        log.info(s"Reading * FROM ${query.query} WHERE $infoDateColumn='${dateFormatter.format(infoDateEnd)}'")
        getDailyDataFrame(query, infoDateEnd)
      } else {
        log.info(s"Reading * FROM ${query.query} WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
        getFilteredDataFrame(query, infoDateBegin, infoDateEnd)
      }
    } else {
      getBaseDataFrame(query)
    }
  }

  private[core] def getDailyDataFrame(query: Query, infoDate: LocalDate): DataFrame = {
    val dateStr = dateFormatter.format(infoDate)

    val readPartitionDirectly = query match {
      case Query.Path(path) => hasData(path, infoDate)
      case _ => false
    }

    if (readPartitionDirectly) {
      val path = query.asInstanceOf[Query.Path].path

      // If the partition folder exists, read directly from it
      val partitionPath = getPartitionPath(path, infoDate)

      val dfIn = if (hasData(path, infoDate)) {
        log.info(s"Partition column exists, reading from $partitionPath.")
        getBasePathReader(query).load(partitionPath.toUri.toString)
      } else {
        getFilteredDataFrame(query, infoDate, infoDate)
      }

      if (dfIn.schema.fields.exists(_.name == infoDateColumn)) {
        log.warn(s"Partition column $infoDateColumn is duplicated in data itself for $dateStr.")
        dfIn
      } else {
        // If the partition column does not exist in the schema (which is expected), add that column
        dfIn.withColumn(infoDateColumn, lit(dateStr))
      }
    } else {
      log.info(s"Reading data from ${query.query} filtered by $infoDateColumn = '$dateStr'.")
      getFilteredDataFrame(query, infoDate, infoDate)
    }
  }

  private[core] def getFilteredDataFrame(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    val infoDateBeginStr = dateFormatter.format(infoDateBegin)
    val infoDateEndStr = dateFormatter.format(infoDateEnd)

    if (infoDateBegin.equals(infoDateEnd)) {
      getBaseDataFrame(query)
        .filter(col(s"$infoDateColumn") === lit(infoDateBeginStr))
    } else {
      getBaseDataFrame(query)
        .filter(col(s"$infoDateColumn") >= lit(infoDateBeginStr) && col(s"$infoDateColumn") <= lit(infoDateEndStr))
    }
  }

  private[core] def hasData(basePath: String, infoDate: LocalDate): Boolean = {
    val path = getPartitionPath(basePath, infoDate)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val hasPartition = fs.exists(path)
    if (!hasPartition) {
      log.warn(s"No partition: $path")
    }
    hasPartition
  }

  private[core] def getBaseDataFrame(query: Query): DataFrame = {
    query match {
      case Query.Sql(sql)   =>
        spark.sql(sql)
      case Query.Path(path) =>
        schemaOpt match {
          case Some(schema) =>
            getFormattedReader(query)
              .schema(schema)
              .options(options)
              .load(path)
          case None         =>
            getFormattedReader(query)
              .options(options)
              .load(path)
        }
      case Query.Table(table) =>
        spark.table(table)
      case other            => throw new IllegalArgumentException(s"'${other.name}' is not supported by the Spark reader. Use 'path', 'table' or 'sql' instead.")
    }
  }

  private[core] def getBasePathReader(query: Query): DataFrameReader = {
    schemaOpt match {
      case Some(schema) =>
        getFormattedReader(query)
          .schema(schema)
          .options(options)
      case None         =>
        getFormattedReader(query)
          .options(options)
    }
  }

  private[core] def getFormattedReader(q: Query): DataFrameReader = {
    formatOpt match {
      case Some(format) => spark.read.format(format)
      case None => throw new IllegalArgumentException(s"Spark source input.${q.name} == '${q.query}' requires 'format' to be specified at the source.")
    }
  }

  private[core] def getPartitionPath(path: String, infoDate: LocalDate): Path = {
    val partition = s"$infoDateColumn=${dateFormatter.format(infoDate)}"
    new Path(path, partition)
  }
}
