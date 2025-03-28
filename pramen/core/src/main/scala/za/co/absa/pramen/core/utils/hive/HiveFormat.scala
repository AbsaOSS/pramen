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

trait HiveFormat {
  def name: String

  def repairPartitionsRequired: Boolean
}

object HiveFormat {
  case object Parquet extends HiveFormat {
    override val name: String = "parquet"

    override val repairPartitionsRequired: Boolean = true
  }

  case object Delta extends HiveFormat {
    override val name: String = "delta"

    override val repairPartitionsRequired: Boolean = false
  }

  case object Iceberg extends HiveFormat {
    override val name: String = "iceberg"

    override val repairPartitionsRequired: Boolean = false
  }

  def fromString(format: String): HiveFormat = {
    format.toLowerCase match {
      case Parquet.name => Parquet
      case Delta.name => Delta
      case Iceberg.name => Iceberg
      case _ => throw new IllegalArgumentException(s"Unsupported Hive format: $format")
    }
  }
}
