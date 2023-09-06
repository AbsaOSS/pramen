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

class BookkeeperNullSuite extends AnyWordSpec {
  private val infoDate1 = LocalDate.of(2020, 8, 11)
  private val infoDate2 = LocalDate.of(2020, 8, 12)
  private val infoDate3 = LocalDate.of(2020, 8, 13)

  "getLatestProcessedDate()" should {
    "return nothing" in {
      val bk = getBookkeeper

      val dateOpt = bk.getLatestProcessedDateFromStorage("table", Some(infoDate2))

      assert(dateOpt.isEmpty)
    }
  }

  "getLatestDataChunk()" should {
    "return nothing" in {
      val bk = getBookkeeper

      val chunkOpt = bk.getLatestDataChunkFromStorage("table", infoDate2, infoDate3)

      assert(chunkOpt.isEmpty)
    }
  }

  "getDataChunks()" should {
    "return nothing" in {
      val bk = getBookkeeper

      val chunks = bk.getDataChunksFromStorage("table", infoDate1, infoDate2)

      assert(chunks.isEmpty)
    }
  }

  "setRecordCount()" should {
    "do nothing" in {
      val bk = getBookkeeper
      bk.setRecordCount("table1", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)

      val chunks = bk.getDataChunksFromStorage("table", infoDate1, infoDate2)

      assert(chunks.isEmpty)
    }
  }

  "getLatestSchema" should {
    "return nothing" in {
      val bk = getBookkeeper

      val schemaOpt = bk.getLatestSchema(null, null)

      assert(schemaOpt.isEmpty)
    }
  }

  "saveSchema" should {
    "do nothing" in {
      val bk = getBookkeeper
      bk.saveSchema("table", infoDate2, null)

      val schemaOpt = bk.getLatestSchema("table", infoDate2)

      assert(schemaOpt.isEmpty)
    }
  }

  def getBookkeeper: BookkeeperBase = {
    val bk = new BookkeeperNull
    bk.saveRecordCountToStorage("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835)
    bk.saveRecordCountToStorage("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318830, 1597318835)
    bk.saveRecordCountToStorage("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318830, 1597318835)
    bk
  }
}
