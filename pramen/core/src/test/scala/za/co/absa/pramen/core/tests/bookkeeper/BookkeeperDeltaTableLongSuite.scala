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

import org.scalatest.BeforeAndAfterAll
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.BookkeeperDeltaTable

import java.io.File
import scala.util.Random

class BookkeeperDeltaTableLongSuite extends BookkeeperCommonSuite with SparkTestBase with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanUpWarehouse()
  }

  override protected def afterAll(): Unit = {
    cleanUpWarehouse()
    super.afterAll()
  }

  def getBookkeeper(prefix: String): BookkeeperDeltaTable = {
    new BookkeeperDeltaTable(None, prefix)
  }

  "BookkeeperHadoopDeltaTable" when {
    testBookKeeper { () =>
      val rndInt = Math.abs(Random.nextInt())
      getBookkeeper(s"tbl${rndInt}_")
    }

    "test tables are created properly" in {
      getBookkeeper(s"tbl0000_")

      assert(spark.catalog.tableExists("tbl0000_bookkeeping"))
      assert(spark.catalog.tableExists("tbl0000_schemas"))
    }
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
