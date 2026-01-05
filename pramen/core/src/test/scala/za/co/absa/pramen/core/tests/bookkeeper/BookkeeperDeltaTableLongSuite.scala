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

package za.co.absa.pramen.core.tests.bookkeeper

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.BookkeeperDeltaTable

import java.io.File
import scala.util.Random

class BookkeeperDeltaTableLongSuite extends BookkeeperCommonSuite with SparkTestBase with BeforeAndAfter with BeforeAndAfterAll {
  val bookkeepingTablePrefix: String = getNewTablePrefix

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanUpWarehouse()
  }

  override protected def afterAll(): Unit = {
    cleanUpWarehouse()
    super.afterAll()
  }

  before {
    if (!spark.version.startsWith("2.")) {
      val fullRecordsTableName = BookkeeperDeltaTable.getFullTableName(None, bookkeepingTablePrefix, BookkeeperDeltaTable.recordsTable)
      val fullSchemasTableName = BookkeeperDeltaTable.getFullTableName(None, bookkeepingTablePrefix, BookkeeperDeltaTable.schemasTable)

      if (spark.catalog.tableExists(fullRecordsTableName)) {
        spark.sql(s"DELETE FROM $fullRecordsTableName WHERE true")
      }

      if (spark.catalog.tableExists(fullSchemasTableName)) {
        spark.sql(s"DELETE FROM $fullSchemasTableName WHERE true")
      }
    }
  }

  def getBookkeeper(prefix: String, batchId: Long): BookkeeperDeltaTable = {
    new BookkeeperDeltaTable(None, prefix, batchId)
  }

  "BookkeeperHadoopDeltaTable" when {
    testBookKeeper { batchId =>
      if (spark.version.startsWith("2.")) {
        getBookkeeper(getNewTablePrefix, batchId)
      } else {
        getBookkeeper(bookkeepingTablePrefix, batchId)
      }
    }

    "test tables are created properly" in {
      val prefix = getNewTablePrefix
      getBookkeeper(prefix, 123L)

      assert(spark.catalog.tableExists(s"${prefix}bookkeeping"))
      assert(spark.catalog.tableExists(s"${prefix}schemas"))
    }
  }

  private def getNewTablePrefix: String = {
    val rndInt = Math.abs(Random.nextInt())
    s"tbl${rndInt}_"
  }

  private def cleanUpWarehouse(): Unit = {
    val warehouseDir = new File("spark-warehouse")
    if (warehouseDir.exists()) {
      warehouseDir.listFiles().foreach(f => {
        if (f.isDirectory) {
          f.listFiles().foreach(ff => ff.delete())
        }
        f.delete()
      })
      warehouseDir.delete()
    }
  }
}
