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
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.PartitionScheme

import java.sql.Date
import java.time.LocalDate
import scala.collection.mutable

object MetastorePersistenceIcebergOps {
  private val log = LoggerFactory.getLogger(this.getClass)

  def createIcebergTable(df: DataFrame,
                         table: String,
                         infoDateColumn: String,
                         location: Option[String] = None,
                         description: String = "",
                         partitionScheme: PartitionScheme,
                         tableProperties: Map[String, String],
                         writerOptions: Map[String, String]): Unit = {
    implicit val spark: SparkSession = df.sparkSession

    val properties = new mutable.HashMap[String, String]
    properties ++= tableProperties

    location.foreach(path => properties += ("location" -> path))
    if (description.nonEmpty) properties += ("comment" -> description)

    val dfIn = partitionScheme match {
      case PartitionScheme.PartitionByDay | PartitionScheme.NotPartitioned | PartitionScheme.Overwrite =>
        df
      case _ =>
        df.filter(lit(false))
    }

    val writer = dfIn.writeTo(table)
      .using("iceberg")
      .tableProperty("format-version", "2")
      .options(writerOptions)

    val writerWithProperties = properties.foldLeft(writer) { (w, item) =>
      item match {
        case (k, v) =>
          log.info(s"Iceberg table property: $k = $v")
          w.tableProperty(k, v)
      }
    }

    val partitionedWriter = partitionScheme match {
      case PartitionScheme.PartitionByDay =>
        writerWithProperties.partitionedBy(col(infoDateColumn))
      case _ =>
        writerWithProperties
    }

    partitionScheme match {
      case PartitionScheme.Overwrite =>
        partitionedWriter.createOrReplace()
      case PartitionScheme.NotPartitioned =>
        partitionedWriter.create()
      case PartitionScheme.PartitionByDay =>
        partitionedWriter.create()
      case _ =>
        partitionedWriter.create()

        MetastorePersistenceIceberg.addGeneratedColumnPartition(table, infoDateColumn, partitionScheme)

        appendToTable(df, table, writerOptions)
    }
  }

  def overwriteDailyPartition(infoDate: LocalDate,
                              df: DataFrame,
                              table: String,
                              infoDateColumn: String,
                              writerOptions: Map[String, String]): Unit = {
    df.writeTo(table)
      .options(writerOptions)
      .overwrite(col(infoDateColumn) === Date.valueOf(infoDate))
  }

  def overwriteFullTable(df: DataFrame, table: String, writerOptions: Map[String, String]): Unit = {
    df.writeTo(table)
      .options(writerOptions)
  }

  def appendToTable(df: DataFrame,
                    table: String,
                    writerOptions: Map[String, String]): Unit = {
    df.writeTo(table)
      .options(writerOptions)
      .append()
  }
}
