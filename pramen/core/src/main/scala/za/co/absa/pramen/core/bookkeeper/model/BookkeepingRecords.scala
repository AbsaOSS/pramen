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
import slick.lifted.TableQuery

class BookkeepingRecords(tag: Tag) extends Table[BookkeepingRecord](tag, "bookkeeping") {
  def pramenTableName = column[String]("watcher_table_name", O.Length(128))
  def infoDate = column[String]("info_date", O.Length(20))
  def infoDateBegin = column[String]("info_date_begin", O.Length(20))
  def infoDateEnd = column[String]("info_date_end", O.Length(20))
  def inputRecordCount = column[Long]("input_record_count")
  def outputRecordCount = column[Long]("output_record_count")
  def jobStarted = column[Long]("job_started")
  def jobFinished = column[Long]("job_finished")
  def * = (pramenTableName, infoDate, infoDateBegin, infoDateEnd,
    inputRecordCount, outputRecordCount,
    jobStarted, jobFinished) <> (BookkeepingRecord.tupled, BookkeepingRecord.unapply)
  def idx1 = index("bk_idx_1", (pramenTableName, infoDate), unique = false)
}

object BookkeepingRecords {
  lazy val records = TableQuery[BookkeepingRecords]
}
