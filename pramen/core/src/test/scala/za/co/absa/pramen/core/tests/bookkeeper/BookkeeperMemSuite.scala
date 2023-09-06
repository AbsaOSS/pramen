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

import java.time.LocalDate

import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.model.DataChunk

class BookkeeperMemSuite extends AnyWordSpec with BeforeAndAfter {

  var bk: Bookkeeper = new SyncBookkeeperMock

  before {
    bk.asInstanceOf[SyncBookkeeperMock].clear()
  }

  "Bookkeeper" when {
    val infoDate1 = LocalDate.of(2020, 8, 11)
    val infoDate2 = LocalDate.of(2020, 8, 12)
    val infoDate3 = LocalDate.of(2020, 8, 13)

    "getLatestProcessedDate()" should {
      "return None if there are no entries" in {
        assert(bk.getLatestProcessedDate("table").isEmpty)
      }

      "return a date when there is an entry" in {
        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)

        val dateOpt = bk.getLatestProcessedDate("table")

        assert(dateOpt.nonEmpty)
        assert(dateOpt.get.equals(infoDate2))
      }

      "return the latest date when there are several dates" in {
        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)
        bk.setRecordCount("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318830, 1597318835, isTableTransient = false)
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318830, 1597318835, isTableTransient = false)

        val dateOpt = bk.getLatestProcessedDate("table")

        assert(dateOpt.nonEmpty)
        assert(dateOpt.get.equals(infoDate3))
      }
    }

    "getLatestDataChunk()" should {
      "return None if there are no entries" in {
        assert(bk.getLatestDataChunk("table", infoDate1, infoDate1).isEmpty)
      }

      "return the latest date from the specified periods" in {
        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318831, 1597318835, isTableTransient = false)
        bk.setRecordCount("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318832, 1597318836, isTableTransient = false)
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318833, 1597318837, isTableTransient = false)

        val chunkOpt = bk.getLatestDataChunk("table", infoDate2, infoDate3)
        val infoDate3Str = infoDate3.format(DataChunk.dateFormatter)

        assert(chunkOpt.nonEmpty)

        val chunk = chunkOpt.get
        assert(chunk.jobStarted == 1597318832)
        assert(chunk.jobFinished == 1597318836)
        assert(chunk.infoDate == infoDate3Str)
        assert(chunk.inputRecordCount == 200)
        assert(chunk.outputRecordCount == 20)
      }

    }

    "getDataChunks()" should {
      "return Nil if there are no entries" in {
        assert(bk.getDataChunks("table", infoDate1, infoDate1).isEmpty)
      }

      "return entries if there are entries" in {
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318833, 1597318837, isTableTransient = false)
        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318831, 1597318835, isTableTransient = false)
        bk.setRecordCount("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318832, 1597318836, isTableTransient = false)

        val chunks = bk.getDataChunks("table", infoDate1, infoDate2).sortBy(_.infoDate)

        assert(chunks.size == 2)

        val chunk1 = chunks.head
        val chunk2 = chunks(1)

        assert(chunk1.infoDate == "2020-08-11")
        assert(chunk2.infoDate == "2020-08-12")
      }

    }

    "setRecordCount()" should {
      "overwrite the previous entry" in {
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 100, 10, 1597318833, 1597318837, isTableTransient = false)
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 200, 20, 1597318838, 1597318839, isTableTransient = false)

        val chunks = bk.getDataChunks("table", infoDate1, infoDate1)

        assert(chunks.size == 1)

        assert(chunks.head.infoDate == "2020-08-11")
        assert(chunks.head.jobFinished == 1597318839)
      }

    }
  }
}
