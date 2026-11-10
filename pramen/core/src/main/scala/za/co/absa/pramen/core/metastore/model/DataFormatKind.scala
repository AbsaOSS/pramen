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

sealed trait DataFormatKind {
  def name: String
}

object DataFormatKind {
  case object Parquet extends DataFormatKind {
    override val name: String = "parquet"
  }

  case object Iceberg extends DataFormatKind {
    override val name: String = "iceberg"
  }

  case object Delta extends DataFormatKind {
    override val name: String = "delta"
  }

  val values: Seq[DataFormatKind] = Seq(Parquet, Iceberg, Delta)

  def fromString(s: String): Option[DataFormatKind] = {
    values.find(_.name.equalsIgnoreCase(s.trim))
  }
}
