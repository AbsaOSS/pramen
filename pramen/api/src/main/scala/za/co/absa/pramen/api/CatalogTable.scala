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

case class CatalogTable(
                         catalog: Option[String],
                         database: Option[String],
                         table: String
                       ) {

  import CatalogTable._

  def getFullTableName: String = {
    val catalogPart = catalog.map(c => s"$escapeCharacter$c$escapeCharacter.").getOrElse("")
    val databasePart = database.map(d => s"$escapeCharacter$d$escapeCharacter.").getOrElse("")
    s"$catalogPart$databasePart$escapeCharacter$table$escapeCharacter"
  }

  def getShortUnescapedTableName: String = {
    val databasePart = database.map(d => s"$d.").getOrElse("")
    s"$databasePart$table"
  }
}

object CatalogTable {
  val escapeCharacter = "`"

  def fromFullTableName(fullTableName: String): CatalogTable = {
    val parts = fullTableName.split("\\.")
    parts.length match {
      case 1 => CatalogTable(None, None, parts(0).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case 2 => CatalogTable(None, Some(parts(0).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), parts(1).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case 3 => CatalogTable(Some(parts(0).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), Some(parts(1).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), parts(2).stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case _ => throw new IllegalArgumentException(s"Too many components of the table name: '$fullTableName'. It should be 'catalog.database.table'.")
    }
  }

  def fromComponents(catalog: Option[String], database: Option[String], table: String): CatalogTable = {
    (catalog, database) match {
      case (Some(c), Some(d)) => CatalogTable(Some(c.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), Some(d.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), table.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case (Some(c), None) => CatalogTable(Some(c.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), None, table.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case (None, Some(d)) => CatalogTable(None, Some(d.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter)), table.stripPrefix(escapeCharacter).stripSuffix(escapeCharacter))
      case (None, None) => CatalogTable(None, None, table)
    }
  }
}