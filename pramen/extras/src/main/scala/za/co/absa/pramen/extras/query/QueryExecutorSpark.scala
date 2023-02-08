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

package za.co.absa.pramen.extras.query

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class QueryExecutorSpark(spark: SparkSession) extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def execute(sql: String): Unit = {
    log.info(s"Executing SQL: $sql")
    spark.sql(sql).take(100)
  }
  override def query(sql: String): DataFrame = {
    log.info(s"Executing SQL: $sql")
    spark.sql(sql)
  }
}
