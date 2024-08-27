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

package za.co.absa.pramen.core.tests.journal

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.journal.{Journal, JournalHadoopDeltaTable}

import java.io.File

class JournalHadoopDeltaTableSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase {

  import TestCases._

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanUpWarehouse()
  }

  override def afterAll(): Unit = {
    cleanUpWarehouse()
    super.afterAll()
  }

  "Journal" should {
    "Make sure the journal works even with empty path" in {
      val journal = getJournal("tbl1_")

      assert(journal.getEntries(instant1, instant3).isEmpty)
    }

    "addEntry()" should {
      "return Nil if there are no entries" in {
        val journal = getJournal("tbl2_")

        assert(journal.getEntries(instant1, instant3).isEmpty)
      }

      "return entries if there are entries" in {
        if (spark.version.split('.').head.toInt >= 3) {
          val journal = getJournal("tbl3_")

          journal.addEntry(task1)
          journal.addEntry(task2)
          journal.addEntry(task3)


          val entries = journal.getEntries(instant2, instant3).sortBy(_.informationDate.toString)

          assert(entries.nonEmpty)
          assert(entries == task2 :: task3 :: Nil)
        }
      }
    }
  }

  private def getJournal(prefix: String): Journal = {
    new JournalHadoopDeltaTable(None, prefix)
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
