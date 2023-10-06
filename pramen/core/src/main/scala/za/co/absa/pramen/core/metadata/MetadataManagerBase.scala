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

import za.co.absa.pramen.api.{MetadataManager, MetadataValue}

import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.util.Try

abstract class MetadataManagerBase(isPersistenceEnabled: Boolean) extends MetadataManager {
  private val metadataLocalStore = new mutable.HashMap[MetadataTableKey, mutable.HashMap[String, MetadataValue]]()

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue]

  def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue]

  def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, value: MetadataValue): Unit

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit

  def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit

  final override def getMetadata(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    val tableLowerCase = tableName.toLowerCase
    if (isPersistent) {
      getMetadataFromStorage(tableLowerCase, infoDate, key)
    } else {
      metadataLocalStore.getOrElse(MetadataTableKey(tableLowerCase, infoDate), mutable.HashMap.empty).get(key)
    }
  }

  final override def getMetadata(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    val tableLowerCase = tableName.toLowerCase
    if (isPersistent) {
      getMetadataFromStorage(tableLowerCase, infoDate)
    } else {
      metadataLocalStore.getOrElse(MetadataTableKey(tableLowerCase, infoDate), mutable.HashMap.empty).toMap
    }
  }

  final override def setMetadata(tableName: String, infoDate: LocalDate, key: String, value: String): Unit = {
    val metadataValue = MetadataValue(value, Instant.now())
    val tableLowerCase = tableName.toLowerCase
    if (isPersistent) {
      setMetadataToStorage(tableLowerCase, infoDate, key, metadataValue)
    } else {
      this.synchronized {
        metadataLocalStore.getOrElseUpdate(MetadataTableKey(tableLowerCase, infoDate), mutable.HashMap.empty)
          .put(key, metadataValue)
      }
    }
  }

  final override def deleteMetadata(tableName: String, infoDate: LocalDate, key: String): Unit = {
    val tableLowerCase = tableName.toLowerCase
    if (isPersistent) {
      deleteMetadataFromStorage(tableLowerCase, infoDate, key)
    } else {
      this.synchronized {
        metadataLocalStore.getOrElse(MetadataTableKey(tableLowerCase, infoDate), mutable.HashMap.empty).remove(key)
      }
    }
  }

  final override def deleteMetadata(tableName: String, infoDate: LocalDate): Unit = {
    val tableLowerCase = tableName.toLowerCase
    if (isPersistent) {
      deleteMetadataFromStorage(tableLowerCase, infoDate)
    } else {
      this.synchronized {
        metadataLocalStore.remove(MetadataTableKey(tableLowerCase, infoDate))
      }
    }
  }

  final override def isPersistent: Boolean = isPersistenceEnabled
}
