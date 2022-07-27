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

package za.co.absa.pramen.core.rdb

trait Rdb {
  /**
    * Returns the version of the database. Version of a database determines the schema used for writes.
    */
  def getVersion(): Int

  /**
    * Sets the version of the database. When a version is set it implies a migration to that version is completed
    * successfully and the database can be used with the specified version of the schema.
    */
  def setVersion(version: Int): Unit

  /**
    * Returns true if the specified table exists in the database.
    */
  def doesTableExists(tableName: String): Boolean

  /**
    * Executes a DDL/SQL statement that does not expect any result set.
    */
  def executeDDL(ddl: String): Unit

}
