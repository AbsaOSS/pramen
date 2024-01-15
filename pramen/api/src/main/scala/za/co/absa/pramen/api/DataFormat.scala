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

package za.co.absa.pramen.api

/** Storage formats supported by the metastore. */
sealed trait DataFormat {
  def name: String

  def isTransient: Boolean
}

object DataFormat {
  case class Parquet(path: String, recordsPerPartition: Option[Long]) extends DataFormat {
    override def name: String = "parquet"

    val isTransient: Boolean = false
  }

  case class Delta(query: Query, recordsPerPartition: Option[Long]) extends DataFormat {
    override def name: String = "delta"

    val isTransient: Boolean = false
  }

  // This format is used for metatables which are just files and can only be used for further sourcing
  case class Raw(path: String) extends DataFormat {
    override def name: String = "raw"

    val isTransient: Boolean = false
  }

  // This format is used for tables that exist only for the duration of the process, and is not persisted
  case class Transient(cachePolicy: CachePolicy) extends DataFormat {
    override def name: String = "transient"

    val isTransient: Boolean = true
  }

  // This format is used for tables are calculated only if requested, and is not persisted
  case class OnDemand(cachePolicy: CachePolicy) extends DataFormat {
    override def name: String = "on_demand"

    val isTransient: Boolean = true
  }

  // This format is used for metatables which do not support persistence, e.g. for sink or transfer jobs
  case class Null() extends DataFormat {
    override def name: String = "null"

    val isTransient: Boolean = false
  }
}
