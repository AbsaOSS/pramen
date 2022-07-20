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

package za.co.absa.pramen.framework.journal.model

import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery

class JournalTasks(tag: Tag) extends Table[JournalTask](tag, "journal") {
  def jobName = column[String]("job_name", O.Length(200))
  def pramenTableName = column[String]("pramen_table_name", O.Length(100))
  def periodBegin = column[String]("period_begin", O.Length(20))
  def periodEnd = column[String]("period_end", O.Length(20))
  def informationDate = column[String]("information_date", O.Length(20))
  def inputRecordCount = column[Long]("input_record_count")
  def inputRecordCountOld = column[Long]("input_record_count_old")
  def outputRecordCount = column[Option[Long]]("output_record_count")
  def outputRecordCountOld = column[Option[Long]]("output_record_count_old")
  def outputSize = column[Option[Long]]("output_size")
  def startedAt = column[Long]("started_at")
  def finishedAt = column[Long]("finished_at")
  def status = column[String]("status", O.Length(50))
  def failureReason = column[Option[String]]("failure_reason")
  def * = (jobName, pramenTableName, periodBegin, periodEnd,
    informationDate, inputRecordCount, inputRecordCountOld, outputRecordCount,
    outputRecordCountOld, outputSize, startedAt, finishedAt, status, failureReason) <> (JournalTask.tupled, JournalTask.unapply)
  def idx1 = index("idx_started_at", startedAt, unique = false)
  def idx2 = index("idx_finished_at", finishedAt, unique = false)
}

object JournalTasks {
  lazy val journalTasks = TableQuery[JournalTasks]
}
