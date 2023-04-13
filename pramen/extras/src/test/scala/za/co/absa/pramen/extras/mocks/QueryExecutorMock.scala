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

package za.co.absa.pramen.extras.mocks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import scala.collection.mutable.ListBuffer

class QueryExecutorMock(tableExists: Boolean,
                        executeFunction: () => Unit = QueryExecutorMock.doNothing)(implicit spark: SparkSession) extends QueryExecutor {
  val queries = new ListBuffer[String]
  var closeCalled = 0

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = tableExists

  override def execute(query: String): Unit = {
    queries += query
    executeFunction()
  }

  def getSchema(parquetPath: String): StructType = {
    spark.read.parquet(parquetPath).schema
  }

  override def close(): Unit = closeCalled += 1
}

object QueryExecutorMock {
  def doNothing(): Unit = {}
}
