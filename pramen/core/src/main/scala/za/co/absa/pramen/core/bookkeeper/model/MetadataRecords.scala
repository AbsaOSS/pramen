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

package za.co.absa.pramen.core.bookkeeper.model

import slick.jdbc.H2Profile.api._

class MetadataRecords(tag: Tag) extends Table[MetadataRecord](tag, "metadata") {
  def pramenTableName = column[String]("table_name", O.Length(128))
  def infoDate = column[String]("info_date", O.Length(20))
  def key = column[String]("key", O.Length(255))
  def value = column[String]("value")
  def lastUpdated = column[Long]("last_updated")
  def * = (pramenTableName, infoDate, key, value, lastUpdated) <> (MetadataRecord.tupled, MetadataRecord.unapply)
  def idx1 = index("meta_idx_1", (pramenTableName, infoDate, key), unique = true)
  def idx2 = index("meta_idx_2", (pramenTableName, infoDate), unique = false)
}

object MetadataRecords {
  lazy val records = TableQuery[MetadataRecords]
}
