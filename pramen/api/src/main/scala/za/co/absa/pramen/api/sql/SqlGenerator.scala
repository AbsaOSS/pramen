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

package za.co.absa.pramen.api.sql

import java.sql.Connection
import java.time.LocalDate

trait SqlGenerator {
  /**
    * Returns wrapped query that can be passed as .option("dtable", here) to the Spark JDBC reader.
    * For example, given "SELECT * FROM abc", returns "(SELECT * FROM abc) tbl"
    */
  def getDtable(sql: String): String

  /**
    * Generates a query that returns the record count of a table that does not have the information date field.
    */
  def getCountQuery(tableName: String): String

  /**
    * Generates a query that returns the record count of a table for the given period when the table does have the information date field.
    */
  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String

  /**
    * Generates a query that returns the record count of an SQL query that is already formed.
    */
  def getCountQueryForSql(filteredSql: String): String

  /**
    * Generates a query that returns data of a table that does not have the information date field.
    */
  def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String

  /**
    * Generates a query that returns the data of a table for the given period when the table does have the information date field.
    */
  def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String

  /**
    * Returns WHERE condition for table that has the information date field given the time period.
    */
  def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String

  /**
    * Returns an aliased expression for a give expression and alias.
    * Alias is automatically escaped, the expression is never escaped. If the expression points just to column name,
    * it needs to be explicitly escaped when needed via escape(name).
    *
    * The method handles differences between aliasing rules of various SQL dialects.
    * For example:
    * Expression: "d", alias: "InfoDate", possible outputs:
    * "d AS InfoDate"
    * "d InfoDate"
    */
  def getAliasExpression(expression: String, alias: String): String

  /** Returns the date literal for the dialect of the SQL. */
  def getDateLiteral(date: LocalDate): String

  /**
    * Validates and escapes an identifier (table or column name) if needed.
    * Escaping happens according to the quoting policy.
    */
  def escape(identifier: String): String

  /**
    * Quotes an identifier name with characters specific to SQL dialects.
    * If the identifier is already wrapped, it won't be double wrapped.
    * It supports partially quoted identifiers. E.g. '"my_catalog".my table' will be quoted as '"my_catalog"."my table"'.
    */
  def quote(identifier: String): String

  /**
    * Unquotes an identifier name with characters specific to SQL dialects.
    * If the identifier is already not quoted, nothing will be done.
    * It supports partially quoted identifiers. E.g. '"my_catalog".my table' will be quoted as 'my_catalog.my table'.
    */
  def unquote(identifier: String): String

  /**
    * Returns true if the SQL generator can only work if it has an active connection.
    * This can be for database engines that does not support "SELECT * FROM table" and require explicit list of columns.
    * The connection can be used to query the list of columns first.
    */
  def requiresConnection: Boolean = false

  /**
    * Sets the connection for the the SQL generator can only work if it has an active connection.
    */
  def setConnection(connection: Connection): Unit = {}
}
