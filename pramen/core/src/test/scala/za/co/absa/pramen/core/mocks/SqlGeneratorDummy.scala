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

package za.co.absa.pramen.core.mocks

import za.co.absa.pramen.api.sql.{SqlConfig, SqlGenerator}

import java.time.LocalDate

class SqlGeneratorDummy(sqlConfig: SqlConfig) extends SqlGenerator {
  def getSqlConfig: SqlConfig = sqlConfig

  override def getDtable(sql: String): String = null

  override def getCountQuery(tableName: String): String = null

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = null

  override def getCountQueryForSql(filteredSql: String): String = null

  override def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = null

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = null

  override def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = null

  override def getAliasExpression(expression: String, alias: String): String = null

  override def getDateLiteral(date: LocalDate): String = null

  override def escape(identifier: String): String = null

  override def quote(identifier: String): String = null

  override def unquote(identifier: String): String = null
}
