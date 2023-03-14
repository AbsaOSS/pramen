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

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

abstract class HiveHelper {
  def createOrUpdateHiveTable(parquetPath: String,
                              schema: StructType,
                              partitionBy: Seq[String],
                              databaseName: Option[String],
                              tableName: String): Unit

  def repairHiveTable(databaseName: Option[String],
                      tableName: String): Unit

  def getFullTable(databaseName: Option[String],
                   tableName: String): String = {

    databaseName match {
      case Some(dbName) => s"$dbName.$tableName"
      case None         => tableName
    }
  }
}

object HiveHelper {
  def apply(conf: Config)(implicit spark: SparkSession): HiveHelper = {
    val queryExecutor = new QueryExecutorSpark()
    val hiveConfig = HiveConfig.fromConfig(conf)
    new HiveHelperImpl(queryExecutor, hiveConfig)
  }
}
