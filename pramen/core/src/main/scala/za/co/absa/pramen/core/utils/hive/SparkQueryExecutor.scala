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

class SparkQueryExecutor(implicit spark: SparkSession)  extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def doesTableExist(dbName: String, tableName: String): Boolean = {
    if (spark.catalog.databaseExists(dbName)) {
      spark.catalog.tableExists(dbName, tableName)
    } else {
      throw new IllegalArgumentException(s"Database '$dbName' not found")
    }
  }

  override def execute(query: String): Unit = {
    log.info(s"Executing SQL: $query")
    spark.sql(query).take(100)
  }

  override def getSchema(parquetPath: String): StructType = {
    val df = spark.read.parquet(parquetPath)

    df.schema
  }
}
