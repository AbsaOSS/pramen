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
import za.co.absa.pramen.api.MetadataManager

import java.time.LocalDate

class MetadataManagerNullSuite extends AnyWordSpec {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "getMetadataFromStorage" should {
    "throw an exception when querying for a key" in {
      val metadata: MetadataManager = new MetadataManagerNull(true)
      assertThrows[UnsupportedOperationException](metadata.getMetadata("table1", infoDate, "key1"))
    }

    "not throw when persistence is disabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(false)
      metadata.getMetadata("table1", infoDate, "key1")
    }

    "throw an exception when querying for a map" in {
      val metadata: MetadataManager = new MetadataManagerNull(true)
      assertThrows[UnsupportedOperationException](metadata.getMetadata("table1", infoDate))
    }

    "not throw an exception when querying for a map and persistence is disabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(false)
      metadata.getMetadata("table1", infoDate)
    }
  }

  "setMetadataFromStorage" should {
    "throw an exception when persistence is enabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(true)
      assertThrows[UnsupportedOperationException](metadata.setMetadata("table1", infoDate, "key1", "value1"))
    }

    "not throw an exception when persistence is disabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(false)
      metadata.setMetadata("table1", infoDate, "key1", "value1")
    }
  }

  "deleteMetadataFromStorage" should {
    "throw an exception when deleting a key" in {
      val metadata: MetadataManager = new MetadataManagerNull(true)
      assertThrows[UnsupportedOperationException](metadata.deleteMetadata("table1", infoDate, "key1"))
    }

    "not throw an exception when deleting a key and persistence is disabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(false)
      metadata.deleteMetadata("table1", infoDate, "key1")
    }

    "throw an exception when deleting all metadata for a table and day" in {
      val metadata: MetadataManager = new MetadataManagerNull(true)
      assertThrows[UnsupportedOperationException](metadata.deleteMetadata("table1", infoDate))
    }

    "not throw an exception when deleting all metadata for a table and day and persistence is disabled" in {
      val metadata: MetadataManager = new MetadataManagerNull(false)
      metadata.deleteMetadata("table1", infoDate)
    }
  }
}
