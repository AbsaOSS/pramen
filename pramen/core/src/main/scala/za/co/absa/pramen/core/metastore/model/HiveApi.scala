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

package za.co.absa.pramen.core.metastore.model

sealed trait HiveApi

object HiveApi {
  case object Sql extends HiveApi
  case object SparkCatalog extends HiveApi

  def fromString(s: String): HiveApi = s match {
    case "sql" => Sql
    case "spark_catalog" => SparkCatalog
    case _ => throw new IllegalArgumentException(s"Unknown Hive API config: '$s'. Only 'sql' and 'spark_catalog' are supported.")
  }
}
