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

package za.co.absa.pramen.bulkload.model

import slick.jdbc.JdbcProfile

trait BulkLoadStateTable {
  val profile: JdbcProfile
  import profile.api._

  class BulkLoadStateRecords(tag: Tag) extends Table[BulkLoadStateSerialized](tag, "bulk_load_state") {
    def pramenTableName = column[String]("table_name", O.Length(600))
    def infoDateColumn = column[String]("info_date_column", O.Length(255))
    def outputInfoDate = column[String]("output_info_date", O.Length(20))
    def dataDateFrom = column[String]("data_date_from", O.Length(20))
    def dataDateTo = column[String]("data_date_to", O.Length(20))
    def phase = column[String]("phase", O.Length(20))
    def * = (pramenTableName, infoDateColumn, outputInfoDate, dataDateFrom, dataDateTo, phase) <> (BulkLoadStateSerialized.tupled, BulkLoadStateSerialized.unapply)
    def idx1 = index("bulkload_offset_idx_1", (pramenTableName, outputInfoDate), unique = true)
  }

  lazy val records = TableQuery[BulkLoadStateRecords]
}
