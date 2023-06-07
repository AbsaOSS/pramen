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

import scala.util.control.NonFatal

class HiveHelperSparkCatalog(spark: SparkSession) extends HiveHelper {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def createOrUpdateHiveTable(path: String,
                                       format: HiveFormat,
                                       schema: StructType,
                                       partitionBy: Seq[String],
                                       databaseName: Option[String],
                                       tableName: String): Unit = {
    val fullTableName = HiveHelper.getFullTable(databaseName, tableName)

    if (spark.catalog.tableExists(fullTableName)) {
      log.info(s"Table $fullTableName already exists. Dropping it...")
      dropCatalogTable(fullTableName)
    }

    createCatalogTable(fullTableName, path, format)

    if (partitionBy.nonEmpty) {
      repairCatalogTable(fullTableName)
    }
  }

  override def repairHiveTable(databaseName: Option[String],
                               tableName: String): Unit = {
    val fullTableName = HiveHelper.getFullTable(databaseName, tableName)

    repairCatalogTable(fullTableName)
  }

  private def dropCatalogTable(fullTableName: String): Unit = {
    spark.sql(s"DROP TABLE $fullTableName").collect()
  }

  override def doesTableExist(databaseName: Option[String], tableName: String): Boolean = spark.catalog.tableExists(HiveHelper.getFullTable(databaseName, tableName))

  private def createCatalogTable(fullTableName: String,
                              path: String,
                              format: HiveFormat
                             ): Unit = {

    log.info(s"Creating Spark Catalog table: $fullTableName...")

    spark.catalog.createTable(fullTableName, path, format.name).collect()
  }


  private def repairCatalogTable(fullTableName: String): Unit = {
    try {
      spark.catalog.recoverPartitions(fullTableName)
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Failed to repair table $fullTableName", ex)
    }
  }
}
