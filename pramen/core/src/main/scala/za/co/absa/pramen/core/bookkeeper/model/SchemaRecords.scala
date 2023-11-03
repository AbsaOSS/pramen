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

class SchemaRecords(tag: Tag) extends Table[SchemaRecord](tag, "schemas") {
  def pramenTableName = column[String]("watcher_table_name", O.Length(128))
  def infoDate = column[String]("info_date", O.Length(20))
  def schemaJson = column[String]("schema_json")
  def * = (pramenTableName, infoDate, schemaJson) <> (SchemaRecord.tupled, SchemaRecord.unapply)
  def idx1 = index("sch_idx_1", (pramenTableName, infoDate), unique = true)
}

object SchemaRecords {
  lazy val records = TableQuery[SchemaRecords]
}

