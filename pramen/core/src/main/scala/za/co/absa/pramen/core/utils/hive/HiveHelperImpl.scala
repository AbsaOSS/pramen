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

package za.co.absa.pramen.core.utils.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class HiveHelperImpl(queryExecutor: QueryExecutor) extends HiveHelper {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def createOrUpdateHiveTable(parquetPath: String,
                                       partitionBy: Seq[String],
                                       databaseName: String,
                                       tableName: String)
                                      (implicit spark: SparkSession): Unit = {
    val fullTableName = getFullTable(databaseName, tableName)

    dropHiveTable(fullTableName)
    createHiveTable(fullTableName, parquetPath, partitionBy)
    if (partitionBy.nonEmpty) {
      repairHiveTable(fullTableName)
    }
  }

  override def repairHiveTable(databaseName: String,
                               tableName: String): Unit = {
    val fullTableName = getFullTable(databaseName, tableName)

    repairHiveTable(fullTableName)
  }

  override def getSchema(parquetPath: String)(implicit spark: SparkSession): StructType = {
    val df = spark.read.parquet(parquetPath)

    df.schema
  }

  private def getFullTable(databaseName: String,
                           tableName: String): String = {
    if (databaseName.isEmpty)
      tableName
    else
      s"$databaseName.$tableName"
  }

  private def dropHiveTable(fullTableName: String): Unit = {
    val sqlHiveDrop = s"DROP TABLE IF EXISTS $fullTableName"
    queryExecutor.execute(sqlHiveDrop)
  }

  private def createHiveTable(fullTableName: String,
                              parquetPath: String,
                              partitionBy: Seq[String]
                             )(implicit spark: SparkSession): Unit = {

    log.info(s"Creating Hive table: $fullTableName...")
    val schema = getSchema(parquetPath)

    val sqlHiveCreate =
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
         |$fullTableName (
         |${getTableDDL(schema, partitionBy)})
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
         |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
         |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         |${getPartitionDDL(schema, partitionBy)}
         |LOCATION '$parquetPath'""".stripMargin

    queryExecutor.execute(sqlHiveCreate)
  }


  private def repairHiveTable(fullTableName: String): Unit = {
    val sqlHiveRefresh = s"MSCK REPAIR TABLE $fullTableName"
    queryExecutor.execute(sqlHiveRefresh)
  }

  private def getTableDDL(schema: StructType, partitionBy: Seq[String]): String = {
    val partitionColsLower = partitionBy.map(_.toLowerCase())

    StructType(schema.filter(field => !partitionColsLower.contains(field.name.toLowerCase()))).toDDL
  }

  private def getPartitionDDL(schema: StructType, partitionBy: Seq[String]): String = {
    if (partitionBy.isEmpty) {
      ""
    } else {
      val partitionColsLower = partitionBy.map(_.toLowerCase())
      val cols = StructType(schema.filter(field => partitionColsLower.contains(field.name.toLowerCase())))
      s"PARTITIONED BY (${cols.toDDL})"
    }
  }
}
