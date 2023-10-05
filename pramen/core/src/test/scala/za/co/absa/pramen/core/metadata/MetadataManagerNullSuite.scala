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

class MetadataManagerNullSuite extends AnyWordSpec {
  "getMetadataFromStorage" should {
    "throw an exception when querying for a key" in {
      val metadata = new MetadataManagerNull()
      assertThrows[UnsupportedOperationException](metadata.getMetadataFromStorage(null, null, null))
    }

    "throw an exception when querying for a map" in {
      val metadata = new MetadataManagerNull()
      assertThrows[UnsupportedOperationException](metadata.getMetadataFromStorage(null, null))
    }
  }

  "setMetadataFromStorage" should {
    "throw an exception" in {
      val metadata = new MetadataManagerNull()
      assertThrows[UnsupportedOperationException](metadata.setMetadataToStorage(null, null, null, null))
    }
  }

  "deleteMetadataFromStorage" should {
    "throw an exception when deleting a key" in {
      val metadata = new MetadataManagerNull()
      assertThrows[UnsupportedOperationException](metadata.deleteMetadataFromStorage(null, null, null))
    }

    "throw an exception when deleting all metadata for a table and day" in {
      val metadata = new MetadataManagerNull()
      assertThrows[UnsupportedOperationException](metadata.deleteMetadataFromStorage(null, null))
    }
  }
}
