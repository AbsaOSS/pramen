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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.metastore.peristence.TransientTableManager

import java.time.LocalDate

class TransientTableManagerSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture {
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

  "reset should do nothing if spark session is not available" in {
    TransientTableManager.reset()
  }

  "addRawDataFrame" should {
    "return dataframe only" in {
      val (df, size) = TransientTableManager.addRawDataFrame("table_not_cached2", infoDate, exampleDf)

      assert(df.schema.treeString == exampleDf.schema.treeString)
      assert(df.count() == 3)
      assert(size.isEmpty)

      TransientTableManager.reset()
    }
  }

  "cachedDataframes" should {
    "return dataframe only" in {
      val (df, size) = TransientTableManager.addCachedDataframe("table_cached2", infoDate, exampleDf)

      assert(df.schema.treeString == exampleDf.schema.treeString)
      assert(df.count() == 3)
      assert(size.isEmpty)

      TransientTableManager.reset()
    }
  }

  "addPersistedLocation" should {
    "return dataframe and size" in {
      val (df, size) = TransientTableManager.addPersistedDataFrame("table_persist2", infoDate, exampleDf, tempDir)

      assert(df.count() == 3)
      assert(size.isDefined)
      assert(size.exists(_ > 100))

      TransientTableManager.reset()
    }
  }

  "getDataForTheDate" should {
    "work for non-cached dataframes" in {
      TransientTableManager.addRawDataFrame("table_not_cached3", infoDate, exampleDf)

      assert(!TransientTableManager.getDataForTheDate("table_not_cached3", infoDate).isEmpty)

      TransientTableManager.reset()
    }

    "work for cached dataframes" in {
      TransientTableManager.addCachedDataframe("table_cached3", infoDate, exampleDf)

      assert(!TransientTableManager.getDataForTheDate("table_cached3", infoDate).isEmpty)

      TransientTableManager.reset()
    }

    "work for persisted dataframes" in {
      TransientTableManager.addPersistedDataFrame("table_persist3", infoDate, exampleDf, tempDir)

      assert(!TransientTableManager.getDataForTheDate("table_persist3", infoDate).isEmpty)

      TransientTableManager.reset()
    }

    "return an empty dataframe if data not found but schema found" in {
      TransientTableManager.addCachedDataframe("table_cache", infoDate, exampleDf)

      val df = TransientTableManager.getDataForTheDate("table_cache", infoDate.plusDays(1))

      assert(df.isEmpty)
      assert(df.schema.sameElements(exampleDf.schema))

      TransientTableManager.reset()
    }

    "throw an exception if data nor schema not found" in {
      assertThrows[IllegalStateException] {
        TransientTableManager.getDataForTheDate("table_cache", infoDate.plusDays(1))
      }

      TransientTableManager.reset()
    }
  }

  "cleanup" should {
    "remove all types of transient tables from the internal state" in {
      TransientTableManager.addRawDataFrame("table_not_cached4", infoDate, exampleDf)
      TransientTableManager.addCachedDataframe("table_cached4", infoDate, exampleDf)
      TransientTableManager.addPersistedDataFrame("table_persist4", infoDate, exampleDf, tempDir)

      assert(!TransientTableManager.getDataForTheDate("table_not_cached4", infoDate).isEmpty)
      assert(!TransientTableManager.getDataForTheDate("table_cached4", infoDate).isEmpty)
      assert(!TransientTableManager.getDataForTheDate("table_persist4", infoDate).isEmpty)

      TransientTableManager.reset()

      assertThrows[IllegalStateException] {
        TransientTableManager.getDataForTheDate("table_not_cached4", infoDate)
      }

      assertThrows[IllegalStateException] {
        TransientTableManager.getDataForTheDate("table_cached4", infoDate)
      }

      assertThrows[IllegalStateException] {
        TransientTableManager.getDataForTheDate("table_persist4", infoDate)
      }
    }
  }

  "getTempDirectory" should {
    val conf = ConfigFactory.parseString("""pramen.temporary.directory = "/a/b/c"""")

    "return None for non-persistent policies" in {
      assert(TransientTableManager.getTempDirectory(CachePolicy.NoCache, conf).isEmpty)
      assert(TransientTableManager.getTempDirectory(CachePolicy.Cache, conf).isEmpty)
    }

    "return the directory for the persist policy" in {
      assert(TransientTableManager.getTempDirectory(CachePolicy.Persist, conf).contains("/a/b/c/cache"))
    }
  }
}
