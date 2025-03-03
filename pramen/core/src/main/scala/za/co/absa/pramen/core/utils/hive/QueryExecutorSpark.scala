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

class QueryExecutorSpark(implicit spark: SparkSession)  extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = {
    val fullTableName = HiveHelper.getFullTable(dbName, tableName)
    val (database, table) = splitTableDatabase(dbName, tableName)

    val exists = try {
      database match {
        case Some(db) =>
          if (spark.catalog.databaseExists(db)) {
            spark.catalog.tableExists(db, table)
          } else {
            throw new IllegalArgumentException(s"Database '$db' not found")
          }
        case None =>
          spark.catalog.tableExists(tableName)
      }
    } catch {
      case _: AnalysisException =>
        // Workaround for Iceberg tables stored in Glue
        // The error is:
        //   Caused by org.apache.spark.sql.AnalysisException: org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table my_test_table
        // Don't forget that Iceberg requires lowercase names as well.
        try {
          spark.read.table(fullTableName)
          true
        } catch {
          // This is a common error
          case ex: AnalysisException if ex.getMessage().contains("Table or view not found") => false
          // This is the exception, needs to be re-thrown.
          case ex: AnalysisException if ex.getMessage().contains("TableType cannot be null for table:") => throw ex
          // If the exception is not AnalysisException, something is wrong so the original exception is thrown.
          //case _: AnalysisException => false
        }
    }

    if (exists)
      log.info(s"Table $fullTableName exists.")
    else
      log.info(s"Table $fullTableName does not exist.")

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
