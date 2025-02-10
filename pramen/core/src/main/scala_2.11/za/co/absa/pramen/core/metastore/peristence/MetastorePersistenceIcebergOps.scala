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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{CatalogTable, PartitionInfo, PartitionScheme}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object MetastorePersistenceIcebergOps {
  def createIcebergTable(df: DataFrame,
                         table: String,
                         infoDateColumn: String,
                         location: Option[String] = None,
                         description: String = "",
                         partitionScheme: PartitionScheme,
                         tableProperties: Map[String, String],
                         writerOptions: Map[String, String]): Unit = {
    throw new UnsupportedOperationException(s"Iceberg format is not supported in Scala 2.11")
  }

  def overwriteDailyPartition(infoDate: LocalDate,
                              df: DataFrame,
                              table: String,
                              infoDateColumn: String,
                              writerOptions: Map[String, String]): Unit = {
    throw new UnsupportedOperationException(s"Iceberg format is not supported in Scala 2.11")
  }

  def appendToTable(df: DataFrame,
                    table: String,
                    writerOptions: Map[String, String]): Unit = {
    throw new UnsupportedOperationException(s"Iceberg format is not supported in Scala 2.11")
  }
}
