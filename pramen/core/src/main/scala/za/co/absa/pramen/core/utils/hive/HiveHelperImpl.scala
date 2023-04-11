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

import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class HiveHelperImpl(queryExecutor: QueryExecutor, hiveConfig: HiveQueryTemplates) extends HiveHelper {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def createOrUpdateHiveTable(parquetPath: String,
                                       schema: StructType,
                                       partitionBy: Seq[String],
                                       databaseName: Option[String],
                                       tableName: String): Unit = {
    val fullTableName = getFullTable(databaseName, tableName)

    dropHiveTable(fullTableName)
    createHiveTable(fullTableName, parquetPath, schema, partitionBy)
    if (partitionBy.nonEmpty) {
      repairHiveTable(fullTableName)
    }
  }
  override def repairHiveTable(databaseName: Option[String],
                               tableName: String): Unit = {
    val fullTableName = getFullTable(databaseName, tableName)

    repairHiveTable(fullTableName)
  }

  private def dropHiveTable(fullTableName: String): Unit = {
    val sqlHiveDrop = applyTemplate(
      hiveConfig.dropTableTemplate,
      fullTableName
    )

    queryExecutor.execute(sqlHiveDrop)
  }

  private def createHiveTable(fullTableName: String,
                              parquetPath: String,
                              schema: StructType,
                              partitionBy: Seq[String]
                             ): Unit = {

    log.info(s"Creating Hive table: $fullTableName...")

    val sqlHiveCreate = applyTemplate(
      hiveConfig.createTableTemplate,
      fullTableName,
      parquetPath,
      getTableDDL(schema, partitionBy),
      getPartitionDDL(schema, partitionBy)
    )

    queryExecutor.execute(sqlHiveCreate)
  }


  private def repairHiveTable(fullTableName: String): Unit = {
    val sqlHiveRepair = applyTemplate(
      hiveConfig.repairTableTemplate,
      fullTableName
    )

    queryExecutor.execute(sqlHiveRepair)
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
      // Hive required not having nullability flags for partition columns
      val ddl = cols.toDDL.replace(" NOT NULL", "")
      s"PARTITIONED BY ($ddl)"
    }
  }

  private def applyTemplate(template: String,
                            fullTableName: String,
                            path: String = "",
                            schemaDDL: String = "",
                            partitionDDL: String = ""): String = {
    template.replace("@fullTableName", fullTableName)
      .replace("@path", path)
      .replace("@schema", schemaDDL)
      .replace("@partitionedBy", partitionDDL)
  }
}
