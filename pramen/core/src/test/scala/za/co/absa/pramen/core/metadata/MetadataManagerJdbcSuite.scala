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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.api.MetadataValue
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.time.{LocalDate, ZoneOffset}

class MetadataManagerJdbcSuite extends AnyWordSpec with RelationalDbFixture with BeforeAndAfter with BeforeAndAfterAll {
  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)
  private val infoDate = LocalDate.of(2021, 2, 18)
  private val exampleInstant = infoDate.atStartOfDay().toInstant(ZoneOffset.UTC)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  "getMetadataFromStorage" should {
    "fetch metadata by key" in {
      val metadata = getMetadataManager
      val v = MetadataValue("value1", exampleInstant)

      metadata.setMetadataToStorage("table1", infoDate, "key1", v)

      assert(metadata.getMetadataFromStorage("table1", infoDate, "key1").exists(_.value == "value1"))
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key1").exists(_.lastUpdated == exampleInstant))
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key2").isEmpty)
      assert(metadata.getMetadataFromStorage("table1", infoDate.plusDays(1), "key1").isEmpty)
    }

    "fetch all metadata for a table and info date" in {
      val metadata = getMetadataManager
      val v1 = MetadataValue("value1", exampleInstant)
      val v2 = MetadataValue("value2", exampleInstant)

      metadata.setMetadataToStorage("table1", infoDate, "key1", v1)
      metadata.setMetadataToStorage("table1", infoDate, "key2", v2)

      val result = metadata.getMetadataFromStorage("table1", infoDate)

      assert(result.size == 2)
      assert(result("key1") == v1)
      assert(result("key2") == v2)

      assert(metadata.getMetadataFromStorage("table1", infoDate.plusDays(1)).isEmpty)
    }

    "throw an exception on connection errors when querying a key" in {
      val metadata = new MetadataManagerJdbc(null)

      val ex = intercept[RuntimeException] {
        metadata.getMetadataFromStorage("table1", infoDate, "key1")
      }

      assert(ex.getMessage.contains("Unable to read from the metadata table."))
    }

    "throw an exception on connection errors when querying a table" in {
      val metadata = new MetadataManagerJdbc(null)

      val ex = intercept[RuntimeException] {
        metadata.getMetadataFromStorage("table1", infoDate)
      }

      assert(ex.getMessage.contains("Unable to read from the metadata table."))
    }
  }

  "setMetadataToStorage" should {
    "overwrite previous value" in {
      val metadata = getMetadataManager
      val v1 = MetadataValue("value1", exampleInstant)
      val v2 = MetadataValue("value2", exampleInstant)

      metadata.setMetadataToStorage("table1", infoDate, "key1", v1)
      metadata.setMetadataToStorage("table1", infoDate, "key1", v2)

      assert(metadata.getMetadataFromStorage("table1", infoDate, "key1").exists(_.value == "value2"))
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key2").isEmpty)
      assert(metadata.getMetadataFromStorage("table1", infoDate.plusDays(1), "key1").isEmpty)
    }

    "throw an exception on connection errors" in {
      val metadata = new MetadataManagerJdbc(null)
      val v = MetadataValue("value1", exampleInstant)

      val ex = intercept[RuntimeException] {
        metadata.setMetadataToStorage("table1", infoDate, "key1", v)
      }

      assert(ex.getMessage.contains("Unable to write to the metadata table."))
    }
  }

  "deleteMetadataFromStorage" should {
    "delete metadata by key" in {
      val metadata = getMetadataManager
      val v1 = MetadataValue("value1", exampleInstant)
      val v2 = MetadataValue("value2", exampleInstant)

      metadata.setMetadataToStorage("table1", infoDate, "key1", v1)
      metadata.setMetadataToStorage("table1", infoDate, "key2", v2)

      metadata.deleteMetadataFromStorage("table1", infoDate, "key1")
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key1").isEmpty)
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key2").exists(_.value == "value2"))
      assert(metadata.getMetadataFromStorage("table1", infoDate.plusDays(1), "key1").isEmpty)
    }

    "not throw an exception when deleting a key that does not exist" in {
      val metadata = getMetadataManager

      metadata.deleteMetadataFromStorage("table1", infoDate, "key1")
    }

    "delete all metadata for a table and info date" in {
      val metadata = getMetadataManager
      val v1 = MetadataValue("value1", exampleInstant)
      val v2 = MetadataValue("value2", exampleInstant)

      metadata.setMetadataToStorage("table1", infoDate, "key1", v1)
      metadata.setMetadataToStorage("table1", infoDate, "key2", v2)

      metadata.deleteMetadataFromStorage("table1", infoDate)

      assert(metadata.getMetadataFromStorage("table1", infoDate, "key1").isEmpty)
      assert(metadata.getMetadataFromStorage("table1", infoDate, "key2").isEmpty)
      assert(metadata.getMetadataFromStorage("table1", infoDate.plusDays(1), "key1").isEmpty)
    }

    "throw an exception on connection errors when deleting a key" in {
      val metadata = new MetadataManagerJdbc(null)

      val ex = intercept[RuntimeException] {
        metadata.deleteMetadataFromStorage("table1", infoDate, "key1")
      }

      assert(ex.getMessage.contains("Unable to delete from the metadata table."))
    }

    "throw an exception on connection errors when deleting metadata from a partision" in {
      val metadata = new MetadataManagerJdbc(null)

      val ex = intercept[RuntimeException] {
        metadata.deleteMetadataFromStorage("table1", infoDate)
      }

      assert(ex.getMessage.contains("Unable to delete from the metadata table."))
    }
  }

  def getMetadataManager: MetadataManagerJdbc = {
    new MetadataManagerJdbc(pramenDb.slickDb)
  }
}
