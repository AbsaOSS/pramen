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

import java.time.LocalDate
import scala.collection.mutable

abstract class MetadataManagerBase(isBookkeepingEnabled: Boolean) extends MetadataManager {
  private val metadataLocalStore = new mutable.HashMap[MetadataTableKey, mutable.HashMap[String, String]]()

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[String]

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, String]

  def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, value: String): Unit

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit

  final override def getMetadata(tableName: String, infoDate: LocalDate, key: String): Option[String] = {
    if (isPersistent) {
      getMetadataFromStorage(tableName, infoDate, key)
    } else {
      metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).get(key)
    }
  }

  final override def getMetadata(tableName: String, infoDate: LocalDate): Map[String, String] = {
    if (isPersistent) {
      getMetadataFromStorage(tableName, infoDate)
    } else {
      metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).toMap
    }
  }

  final override def setMetadata(tableName: String, infoDate: LocalDate, key: String, value: String): Unit = {
    if (isPersistent) {
      setMetadataToStorage(tableName, infoDate, key, value)
    } else {
      this.synchronized {
        metadataLocalStore.getOrElseUpdate(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).put(key, value)
      }
    }
  }

  final override def deleteMetadata(tableName: String, infoDate: LocalDate, key: String): Unit = {
    if (isPersistent) {
      deleteMetadataFromStorage(tableName, infoDate, key)
    } else {
      this.synchronized {
        metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).remove(key)
      }
    }
  }

  final override def deleteMetadata(tableName: String, infoDate: LocalDate): Unit = {
    if (isPersistent) {
      deleteMetadataFromStorage(tableName, infoDate)
    } else {
      this.synchronized {
        metadataLocalStore.remove(MetadataTableKey(tableName, infoDate))
      }
    }
  }

  final override def isPersistent: Boolean = isBookkeepingEnabled
}
