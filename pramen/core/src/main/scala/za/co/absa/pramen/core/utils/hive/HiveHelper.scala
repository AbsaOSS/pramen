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
import za.co.absa.pramen.core.metastore.model.{HiveApi, HiveConfig}
import za.co.absa.pramen.core.reader.JdbcUrlSelector

abstract class HiveHelper {
  def createOrUpdateHiveTable(path: String,
                              format: HiveFormat,
                              schema: StructType,
                              partitionBy: Seq[String],
                              databaseName: Option[String],
                              tableName: String): Unit

  def repairHiveTable(databaseName: Option[String],
                      tableName: String,
                      format: HiveFormat): Unit

  def addPartition(databaseName: Option[String],
                   tableName: String,
                   partitionBy: Seq[String],
                   partitionValues: Seq[String],
                   location: String): Unit

  def doesTableExist(databaseName: Option[String],
                     tableName: String): Boolean

  def dropTable(databaseName: Option[String],
                tableName: String): Unit
}

object HiveHelper {
  private val log = LoggerFactory.getLogger(this.getClass)

  def fromHiveConfig(hiveConfig: HiveConfig)
                    (implicit spark: SparkSession): HiveHelper = {
    hiveConfig.hiveApi match {
      case HiveApi.Sql          =>
        val queryExecutor = hiveConfig.jdbcConfig match {
          case Some(jdbcConfig) =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via JDBC.")
            new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), hiveConfig.optimizeExistQuery)
          case None             =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via Spark.")
            new QueryExecutorSpark()
        }
        new HiveHelperSql(queryExecutor, hiveConfig.templates, hiveConfig.alwaysEscapeColumnNames)
      case HiveApi.SparkCatalog =>
        log.info(s"Using Hive via Spark Catalog API and configuration.")
        new HiveHelperSparkCatalog(spark)
    }
  }

  def fromQueryExecutor(api: HiveApi,
                        templates: HiveQueryTemplates,
                        queryExecutor: QueryExecutor)
                       (implicit spark: SparkSession): HiveHelper = {
    api match {
      case HiveApi.Sql => new HiveHelperSql(queryExecutor, templates, true)
      case _ => new HiveHelperSparkCatalog(spark)
    }
  }

  def getFullTable(databaseName: Option[String],
                   tableName: String): String = {

    databaseName match {
      case Some(dbName) => s"`$dbName`.`$tableName`"
      case None         => tableName
    }
  }
}
