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

package za.co.absa.pramen.core.metadata

import za.co.absa.pramen.api.MetadataValue

import java.time.LocalDate

class MetadataManagerNull(isPersistenceEnabled: Boolean) extends MetadataManagerBase(isPersistenceEnabled) {
  private val errorMessage = "The current bookkeeping persistent layer does not support metadata management."

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    throw new UnsupportedOperationException(errorMessage)
  }

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    throw new UnsupportedOperationException(errorMessage)
  }

  def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, value: MetadataValue): Unit = {
    throw new UnsupportedOperationException(errorMessage)
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit = {
    throw new UnsupportedOperationException(errorMessage)
  }

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit = {
    throw new UnsupportedOperationException(errorMessage)
  }
}
