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

import java.time.LocalDate

trait MetadataManager {
  /**
    * Get metadata value for a given table, date and key.
    *
    * @param tableName The name of the Pramen metastore table.
    * @param infoDate  The information date of the data.
    * @param key       The key metadata key.
    * @return The metadata value if found.
    */
  def getMetadata(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue]

  /**
    * Get all metadata for a given table and date.
    *
    * @param tableName The name of the Pramen metastore table.
    * @param infoDate  The information date of the data.
    * @return A map of metadata key-value pairs for the specified table and date.
    */
  def getMetadata(tableName: String, infoDate: LocalDate): Map[String, MetadataValue]

  /**
    * Set metadata value for a given table, date and key.
    *
    * @param tableName The name of the Pramen metastore table.
    * @param infoDate  The information date of the data.
    * @param key       The key metadata key.
    * @param value     The value of the key set for the specified table and date.
    */
  def setMetadata(tableName: String, infoDate: LocalDate, key: String, value: String): Unit

  /**
    * Delete metadata value for a given table, date and key.
    *
    * @param tableName The name of the Pramen metastore table.
    * @param infoDate  The information date of the data.
    * @param key       The key metadata key.
    */
  def deleteMetadata(tableName: String, infoDate: LocalDate, key: String): Unit

  /**
    * Delete all metadata for a given table and date.
    *
    * @param tableName The name of the Pramen metastore table.
    * @param infoDate  The information date of the data.
    */
  def deleteMetadata(tableName: String, infoDate: LocalDate): Unit

  /**
    * Returns true if metadata is persistent in a database.
    * Returns false if metadata is available only for the duration of the session.
    */
  def isPersistent: Boolean
}
