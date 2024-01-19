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

package za.co.absa.pramen.core.metastore.persistence

import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.metastore.peristence.{MetastorePersistenceTransientEager, TransientTableManager}

import java.time.LocalDate

class MetastorePersistenceTransientEagerSuiteEager extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private var tempDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    tempDir = createTempDir("transient_persist")
  }

  override def afterAll(): Unit = {
    deleteDir(tempDir)

    super.afterAll()
  }

  "loadTable" should {
    "return data for a date when it is available" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table1", CachePolicy.NoCache)

      persistor.saveTable(infoDate, exampleDf, Some(10))

      val df = persistor.loadTable(Some(infoDate), Some(infoDate))

      assert(df.count() == 3)

      TransientTableManager.reset()
    }

    "return an empty dataframe if data is not available but schema is available" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table2", CachePolicy.Cache)

      persistor.saveTable(infoDate, exampleDf, Some(10))

      val df = persistor.loadTable(Some(infoDate.plusDays(1)), Some(infoDate.plusDays(1)))

      assert(df.isEmpty)

      TransientTableManager.reset()
    }

    "throw an exception if the data and schema are not available" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table2", CachePolicy.Cache)

      val ex = intercept[IllegalStateException] {
        persistor.loadTable(Some(infoDate.plusDays(1)), Some(infoDate.plusDays(1)))
      }

      assert(ex.getMessage.contains("No data for transient table 'table2' for '2022-02-19'"))

      TransientTableManager.reset()
    }

    "throw an exception on range queries" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table2", CachePolicy.Cache)

      persistor.saveTable(infoDate, exampleDf, None)

      val ex = intercept[IllegalArgumentException] {
        persistor.loadTable(Some(infoDate.minusDays(1)), Some(infoDate.plusDays(1)))
      }

      assert(ex.getMessage.contains("Metastore 'transient' format does not support ranged queries"))

      TransientTableManager.reset()
    }

    "throw an exception if info date is not provided" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table2", CachePolicy.Cache)

      val ex = intercept[IllegalArgumentException] {
        persistor.loadTable(None, None)
      }

      assert(ex.getMessage.contains("Metastore 'transient' format requires info date for querying its contents"))

      TransientTableManager.reset()
    }
  }

  "saveTable" should {
    "work with non-cached data frames" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table_not_cached", CachePolicy.NoCache)

      val saveResult = persistor.saveTable(infoDate, exampleDf, Some(10))

      assert(saveResult.recordCount == 10)
      assert(saveResult.dataSizeBytes.isEmpty)

      TransientTableManager.reset()
    }

    "work with cached data frames" in {
      val persistor = new MetastorePersistenceTransientEager(None, "table_cached", CachePolicy.Cache)

      val saveResult = persistor.saveTable(infoDate, exampleDf, None)

      assert(saveResult.recordCount == 3)
      assert(saveResult.dataSizeBytes.isEmpty)

      TransientTableManager.reset()
    }

    "work with persisted data frames" in {
      val persistor = new MetastorePersistenceTransientEager(Some(tempDir), "table_persisted", CachePolicy.Persist)

      val saveResult = persistor.saveTable(infoDate, exampleDf, None)

      assert(saveResult.recordCount == 3)
      assert(saveResult.dataSizeBytes.isDefined)
      assert(saveResult.dataSizeBytes.exists(_ > 100))

      TransientTableManager.reset()
    }
  }

  "getStats" should {
    "not be supported" in {
      val persistor = new MetastorePersistenceTransientEager(null, null, null)

      assertThrows[UnsupportedOperationException] {
        persistor.getStats(null)
      }
    }
  }

  "createOrUpdateHiveTable" should {
    "not be supported" in {
      val persistor = new MetastorePersistenceTransientEager(null, null, null)

      assertThrows[UnsupportedOperationException] {
        persistor.createOrUpdateHiveTable(null, null, null, null)
      }
    }
  }

  "repairHiveTable" should {
    "not be supported" in {
      val persistor = new MetastorePersistenceTransientEager(null, null, null)

      assertThrows[UnsupportedOperationException] {
        persistor.repairHiveTable(null, null, null)
      }
    }
  }
}
