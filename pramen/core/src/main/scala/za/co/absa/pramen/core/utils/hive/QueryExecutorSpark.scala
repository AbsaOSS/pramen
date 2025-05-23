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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.CatalogTable
import za.co.absa.pramen.core.utils.SparkUtils

class QueryExecutorSpark(implicit spark: SparkSession)  extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = {
    val catalogTable = CatalogTable.fromComponents(None, dbName, tableName)
    val exists = SparkUtils.doesCatalogTableExist(catalogTable)(spark)

    if (exists)
      log.info(s"Table ${catalogTable.getFullTableName} exists.")
    else
      log.info(s"Table ${catalogTable.getFullTableName} does not exist.")

    exists
  }

  @throws[AnalysisException]
  override def execute(query: String): Unit = {
    log.info(s"Executing SQL: $query")
    spark.sql(query).take(100)
  }

  override def close(): Unit = { }

  /** Ensures that the database name is passed as database, and not embedded into the table name itself. */
  private [core] def splitTableDatabase(dbName: Option[String], tableName: String): (Option[String], String) = {
    dbName match {
      case Some(db) =>
        (Some(db), tableName)
      case None     =>
        if (tableName.contains('.')) {
          val split = tableName.split('.')
          (Option(split.head), split.tail.mkString("."))
        } else
          (None, tableName)
    }
  }
}

object QueryExecutorSpark {
  def apply(sparkSession: SparkSession): QueryExecutorSpark = new QueryExecutorSpark()(sparkSession)
}
