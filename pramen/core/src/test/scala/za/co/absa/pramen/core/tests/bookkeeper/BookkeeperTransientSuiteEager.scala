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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.bookkeeper.{BookkeeperBase, BookkeeperNull}

import java.time.LocalDate

class BookkeeperTransientSuiteEager extends AnyWordSpec {
  private val infoDate1 = LocalDate.of(2020, 8, 11)
  private val infoDate2 = LocalDate.of(2020, 8, 12)
  private val infoDate3 = LocalDate.of(2020, 8, 13)

  "getLatestProcessedDate()" should {
    "return information according to data available at the session" in {
      val bk = getBookkeeper

      val dateOpt = bk.getLatestProcessedDate("table", Some(infoDate2))

      assert(dateOpt.isDefined)
      assert(dateOpt.contains(infoDate2))
    }

    "return None if bookkeeping records not found" in {
      val bk = getBookkeeper

      val dateOpt = bk.getLatestProcessedDate("table", Some(infoDate1.minusDays(1)))

      assert(dateOpt.isEmpty)
    }
  }

  "getLatestDataChunk()" should {
    "return information according to data available at the session" in {
      val bk = getBookkeeper

      val chunkOpt = bk.getLatestDataChunk("table", infoDate2, infoDate3)

      assert(chunkOpt.isDefined)
      assert(chunkOpt.get.infoDate == infoDate3.toString)
    }
  }

  "getDataChunks()" should {
    "return chunks from the current session" in {
      val bk = getBookkeeper

      val chunks = bk.getDataChunks("table", infoDate1, infoDate2)

      assert(chunks.nonEmpty)
      assert(chunks.length == 2)
      assert(chunks.head.infoDate == infoDate2.toString)
      assert(chunks(1).infoDate == infoDate1.toString)
    }
  }

  "getDataChunksCount()" should {
    "return the number of chunks from the current session" in {
      val bk = getBookkeeper

      val chunksCount = bk.getDataChunksCount("table", Option(infoDate1), Option(infoDate2))

      assert(chunksCount == 2)
    }
  }

  "setRecordCount()" should {
    "return the newly added record" in {
      val bk = getBookkeeper
      bk.setRecordCount("table1", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = true)

      val chunks = bk.getDataChunks("table1", infoDate2, infoDate2)

      assert(chunks.length == 1)
      assert(chunks.head.infoDate == infoDate2.toString)
    }
  }

  def getBookkeeper: BookkeeperBase = {
    val bk = new BookkeeperNull
    bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = true)
    bk.setRecordCount("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318830, 1597318836, isTableTransient = true)
    bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318830, 1597318837, isTableTransient = true)
    bk
  }
}
