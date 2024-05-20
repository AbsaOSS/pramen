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

package za.co.absa.pramen.core.sink

import org.apache.hadoop.fs.Path

sealed trait SparkSinkFormat

object SparkSinkFormat {
  case class PathFormat(path: Path) extends SparkSinkFormat {
    override def toString = s"path: $path"
  }
  case class TableFormat(table: String) extends SparkSinkFormat {
    override def toString = s"table: $table"
  }

  case object ConnectionFormat extends SparkSinkFormat {
    override def toString = "the connection"
  }
}
