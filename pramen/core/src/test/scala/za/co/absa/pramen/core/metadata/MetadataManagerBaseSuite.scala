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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.mocks.metadata.MetadataManagerSpy

import java.time.LocalDate

class MetadataManagerBaseSuite extends AnyWordSpec {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "getMetadata" should {
    "return metadata from the persistent layer" in {
      val metadata = new MetadataManagerSpy(true)

      metadata.setMetadata("table1", infoDate, "key1", "Value1")

      assert(metadata.getMetadata("TABLE1", infoDate, "key1").exists(_.value == "Value1"))
      assert(metadata.getMetadata("TABLE1", infoDate, "key2").isEmpty)
      assert(metadata.getMetadata("TABLE1", infoDate.plusDays(1), "key1").isEmpty)
      assert(metadata.getMetadata("table2", infoDate, "key1").isEmpty)

      assert(metadata.getMetadataFromStorageCalls1.length == 4)
      assert(metadata.getMetadataFromStorageCalls1.head._1 == "table1")
      assert(metadata.getMetadataFromStorageCalls1.head._2 == infoDate)
      assert(metadata.getMetadataFromStorageCalls1.head._3 == "key1")
      assert(metadata.getMetadataFromStorageCalls1.head._4.exists(_.value == "Value1"))
    }

    "return metadata from the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      metadata.setMetadata("table1",infoDate, "key1", "value1")

      assert(metadata.getMetadata("TABLE1",infoDate, "key1").exists(_.value == "value1"))
      assert(metadata.getMetadataFromStorageCalls1.isEmpty)
    }

    "return none from the persistent layer when no such key found" in {
      val metadata = new MetadataManagerSpy(true)

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.getMetadataFromStorageCalls1.length == 1)
      assert(metadata.getMetadataFromStorageCalls1.head._1 == "table1")
      assert(metadata.getMetadataFromStorageCalls1.head._2 ==infoDate)
      assert(metadata.getMetadataFromStorageCalls1.head._3 == "key1")
      assert(metadata.getMetadataFromStorageCalls1.head._4.isEmpty)
    }

    "return none from the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.getMetadataFromStorageCalls1.isEmpty)
    }

    "return metadata map from the persistent layer" in {
      val metadata = new MetadataManagerSpy(true)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.setMetadata("table1",infoDate, "key2", "value2")

      val result = metadata.getMetadata("TABLE1",infoDate)

      assert(result.contains("key1"))
      assert(result.contains("key2"))
      assert(result("key1").value == "value1")
      assert(result("key2").value == "value2")
      assert(result.size == 2)

      assert(metadata.getMetadataFromStorageCalls2.length == 1)
      assert(metadata.getMetadataFromStorageCalls2.head._1 == "table1")
      assert(metadata.getMetadataFromStorageCalls2.head._2 ==infoDate)
    }

    "return metadata map from the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.setMetadata("table1",infoDate, "key2", "value2")

      val result = metadata.getMetadata("TABLE1",infoDate)

      assert(result.contains("key1"))
      assert(result.contains("key2"))
      assert(result("key1").value == "value1")
      assert(result("key2").value == "value2")
      assert(result.size == 2)

      assert(metadata.getMetadataFromStorageCalls1.isEmpty)
    }

    "return empty map from the persistent layer when no such key found" in {
      val metadata = new MetadataManagerSpy(true)

      val result = metadata.getMetadata("table1",infoDate)

      assert(result.isEmpty)
      assert(metadata.getMetadataFromStorageCalls2.length == 1)
      assert(metadata.getMetadataFromStorageCalls2.head._1 == "table1")
      assert(metadata.getMetadataFromStorageCalls2.head._2 ==infoDate)
      assert(metadata.getMetadataFromStorageCalls2.head._3.isEmpty)
    }

    "return empty map from the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      val result = metadata.getMetadata("table1",infoDate)

      assert(result.isEmpty)
      assert(metadata.getMetadataFromStorageCalls1.isEmpty)
    }
  }

  "setMetadata" should {
    "set metadata in the persistent layer" in {
      val metadata = new MetadataManagerSpy(true)

      metadata.setMetadata("table1",infoDate, "key1", "value1")

      assert(metadata.getMetadata("TABLE1",infoDate, "key1").exists(_.value == "value1"))
    }

    "set metadata in the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      metadata.setMetadata("table1",infoDate, "key1", "value1")

      assert(metadata.getMetadata("table1",infoDate, "key1").exists(_.value == "value1"))
    }
  }

  "deleteMetadata" should {
    "delete metadata previously added to the persistent layer" in {
      val metadata = new MetadataManagerSpy(true)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.deleteMetadata("TABLE1",infoDate, "key1")

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.deleteMetadataFromStorageCalls1.length == 1)
      assert(metadata.deleteMetadataFromStorageCalls1.head._1 == "table1")
      assert(metadata.deleteMetadataFromStorageCalls1.head._2 ==infoDate)
      assert(metadata.deleteMetadataFromStorageCalls1.head._3 == "key1")
    }

    "delete metadata previously added to the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.deleteMetadata("TABLE1",infoDate, "key1")

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.deleteMetadataFromStorageCalls1.isEmpty)
    }

    "delete all metadata previously added to the persistent layer" in {
      val metadata = new MetadataManagerSpy(true)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.setMetadata("table1",infoDate, "key2", "value2")
      metadata.deleteMetadata("TABLE1",infoDate)

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.getMetadata("table1",infoDate, "key2").isEmpty)
      assert(metadata.deleteMetadataFromStorageCalls2.length == 1)
      assert(metadata.deleteMetadataFromStorageCalls2.head._1 == "table1")
      assert(metadata.deleteMetadataFromStorageCalls2.head._2 ==infoDate)
    }

    "delete all metadata previously added to the local in memory map" in {
      val metadata = new MetadataManagerSpy(false)

      metadata.setMetadata("table1",infoDate, "key1", "value1")
      metadata.setMetadata("table1",infoDate, "key2", "value2")
      metadata.deleteMetadata("TABLE1",infoDate)

      assert(metadata.getMetadata("table1",infoDate, "key1").isEmpty)
      assert(metadata.getMetadata("table1",infoDate, "key2").isEmpty)
      assert(metadata.deleteMetadataFromStorageCalls1.isEmpty)
    }
  }

  "isPersistent" should {
    "return true for persisted metadata" in {
      val metadata = new MetadataManagerSpy(true)

      assert(metadata.isPersistent)
    }

    "return false for non-persisted metadata" in {
      val metadata = new MetadataManagerSpy(false)

      assert(!metadata.isPersistent)
    }
  }
}
