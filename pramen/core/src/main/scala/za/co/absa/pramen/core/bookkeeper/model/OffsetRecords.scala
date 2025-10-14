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

class OffsetRecords(tag: Tag) extends Table[OffsetRecord](tag, "offsets") {
  def pramenTableName = column[String]("table_name", O.Length(256))
  def infoDate = column[String]("info_date", O.Length(20))
  def dataType = column[String]("data_type", O.Length(20))
  def minOffset = column[String]("min_offset")
  def maxOffset = column[String]("max_offset")
  def batchId = column[Long]("batch_id")
  def createdAt = column[Long]("created_at")
  def committedAt = column[Option[Long]]("committed_at")
  def * = (pramenTableName, infoDate, dataType, minOffset, maxOffset, batchId, createdAt, committedAt) <> (OffsetRecord.tupled, OffsetRecord.unapply)
  def idx1 = index("offset_idx_1", (pramenTableName, infoDate, createdAt), unique = true)
  def idx2 = index("offset_idx_2", (pramenTableName, committedAt), unique = false)
}

object OffsetRecords {
  lazy val records = TableQuery[OffsetRecords]
}
