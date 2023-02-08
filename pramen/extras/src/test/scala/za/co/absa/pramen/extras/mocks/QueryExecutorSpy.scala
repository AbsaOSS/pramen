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

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.extras.query.QueryExecutor

import scala.collection.mutable.ListBuffer

class QueryExecutorSpy(dfToReturn: Option[DataFrame] = None,
                       throwException: Option[Throwable] = None)(implicit spark: SparkSession) extends QueryExecutor {

  import spark.implicits._

  val executed = new ListBuffer[String]
  val queried = new ListBuffer[String]

  override def execute(sql: String): Unit = {
    executed += sql
    throwException.foreach(e => throw e)
  }

  override def query(sql: String): DataFrame = {
    queried += sql
    throwException.foreach(e => throw e)
    dfToReturn match {
      case Some(df) => df
      case None     => List(("A", 1), ("B", 2), ("C", 3)).toDF("A", "B")
    }
  }
}
