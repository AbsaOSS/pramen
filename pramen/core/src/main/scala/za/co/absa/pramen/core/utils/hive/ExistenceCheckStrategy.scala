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

sealed trait ExistenceCheckStrategy

object ExistenceCheckStrategy {
  case object MetadataQuery extends ExistenceCheckStrategy

  case object DescribeTable extends ExistenceCheckStrategy

  case object MetadataAndDescribeQuery extends ExistenceCheckStrategy

  case object SelectQuery extends ExistenceCheckStrategy

  def fromString(s: String): ExistenceCheckStrategy = {
    s.toLowerCase match {
      case "metadata_query"              => MetadataQuery
      case "describe_table"              => DescribeTable
      case "metadata_and_describe_query" => MetadataAndDescribeQuery
      case "select_query"                => SelectQuery
      case other                         => throw new IllegalArgumentException(s"Unknown existence check strategy: '$other'. " +
        s"Valid values are: 'metadata_query', 'describe_table', 'metadata_and_describe_query', 'select_query'.")
    }
  }
}
