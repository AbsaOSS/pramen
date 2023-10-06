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

package za.co.absa.pramen.core.mocks.metadata

import za.co.absa.pramen.api.MetadataValue
import za.co.absa.pramen.core.metadata.{MetadataManagerBase, MetadataTableKey}

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MetadataManagerSpy(isPersistent: Boolean) extends MetadataManagerBase(isPersistent){
  private val metadataLocalStore = new mutable.HashMap[MetadataTableKey, mutable.HashMap[String, MetadataValue]]()

  val getMetadataFromStorageCalls1 = new ListBuffer[(String, LocalDate, String, Option[MetadataValue])]()
  val getMetadataFromStorageCalls2 = new ListBuffer[(String, LocalDate, Map[String, MetadataValue])]()
  val setMetadataFromStorageCalls = new ListBuffer[(String, LocalDate, String, MetadataValue)]()
  val deleteMetadataFromStorageCalls1 = new ListBuffer[(String, LocalDate, String)]()
  val deleteMetadataFromStorageCalls2 = new ListBuffer[(String, LocalDate)]()

  override def getMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Option[MetadataValue] = {
    val result = metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).get(key)

    getMetadataFromStorageCalls1 += ((tableName, infoDate, key, result))

    result
  }

  override def getMetadataFromStorage(tableName: String, infoDate: LocalDate): Map[String, MetadataValue] = {
    val result = metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).toMap

    getMetadataFromStorageCalls2 += ((tableName, infoDate, result))

    result
  }

  override def setMetadataToStorage(tableName: String, infoDate: LocalDate, key: String, value: MetadataValue): Unit = {
    setMetadataFromStorageCalls += ((tableName, infoDate, key, value))

    metadataLocalStore.getOrElseUpdate(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).put(key, value)
  }

  override def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate, key: String): Unit = {
    deleteMetadataFromStorageCalls1 += ((tableName, infoDate, key))

    metadataLocalStore.getOrElse(MetadataTableKey(tableName, infoDate), mutable.HashMap.empty).remove(key)
  }

  override def deleteMetadataFromStorage(tableName: String, infoDate: LocalDate): Unit = {
    deleteMetadataFromStorageCalls2 += ((tableName, infoDate))

    metadataLocalStore.remove(MetadataTableKey(tableName, infoDate))
  }
}
