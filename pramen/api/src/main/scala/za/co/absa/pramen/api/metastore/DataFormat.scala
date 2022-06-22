/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.api.metastore

import za.co.absa.pramen.api.v2.Query

/** Storage formats supported by the metastore. */
sealed trait DataFormat {
  def name: String
}

object DataFormat {
  case class Parquet(path: String, recordsPerPartition: Option[Long]) extends DataFormat {
    override def name: String = "parquet"
  }

  case class Delta(query: Query, recordsPerPartition: Option[Long]) extends DataFormat {
    override def name: String = "delta"
  }
}