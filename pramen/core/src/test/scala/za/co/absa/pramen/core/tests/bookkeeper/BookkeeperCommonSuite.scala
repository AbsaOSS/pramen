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

import org.apache.spark.sql.types._
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, BookkeeperDeltaTable}
import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate

class BookkeeperCommonSuite extends AnyWordSpec {

  def testBookKeeper(getBookkeeper: () => Bookkeeper): Unit = {
    val infoDate1 = LocalDate.of(2020, 8, 11)
    val infoDate2 = LocalDate.of(2020, 8, 12)
    val infoDate3 = LocalDate.of(2020, 8, 13)

    val schema1 = StructType(Seq(
      StructField("a", StringType),
      StructField("b", LongType)
    ))

    val schema2 = StructType(Seq(
      StructField("a", LongType),
      StructField("c", IntegerType)
    ))

    "getLatestProcessedDate()" should {
      "return None if there are no entries" in {
        val bk = getBookkeeper()

        assert(bk.getLatestProcessedDate("table").isEmpty)
      }

      "return a date when there is an entry" in {
        val bk = getBookkeeper()

        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)

        val dateOpt = bk.getLatestProcessedDate("table")

        assert(dateOpt.nonEmpty)
        assert(dateOpt.get.equals(infoDate2))
      }

      "return None if the passed date is too old" in {
        val bk = getBookkeeper()

        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)

        val dateOpt = bk.getLatestProcessedDate("table", Some(infoDate1))

        assert(dateOpt.isEmpty)
      }

      "return a date when there is an entry and until date is passed" in {
        val bk = getBookkeeper()

        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318830, 1597318835, isTableTransient = false)

        val dateOpt = bk.getLatestProcessedDate("table", Some(infoDate2))

        assert(dateOpt.nonEmpty)
        assert(dateOpt.get.equals(infoDate2))
      }

      "return the latest date when there are several dates" in {
        val bk = getBookkeeper()

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
        val bk = getBookkeeper()

        assert(bk.getLatestDataChunk("table", infoDate1, infoDate1).isEmpty)
      }

      "return the latest date from the specified periods" in {
        val bk = getBookkeeper()

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
        val bk = getBookkeeper()

        assert(bk.getDataChunks("table", infoDate1, infoDate1).isEmpty)
      }

      "return entries if there are entries" in {
        val bk = getBookkeeper()

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

    "getDataChunksCount()" should {
      "return 0 if there are no entries" in {
        val bk = getBookkeeper()

        assert(bk.getDataChunksCount("table", Option(infoDate1), Option(infoDate1)) == 0)
      }

      "return the number of entries if there are entries" in {
        val bk = getBookkeeper()

        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 400, 40, 1597318833, 1597318837, isTableTransient = false)
        bk.setRecordCount("table", infoDate2, infoDate2, infoDate2, 100, 10, 1597318831, 1597318835, isTableTransient = false)
        bk.setRecordCount("table", infoDate3, infoDate3, infoDate3, 200, 20, 1597318832, 1597318836, isTableTransient = false)

        val chunksCount = bk.getDataChunksCount("table", Option(infoDate1), Option(infoDate2))

        assert(chunksCount == 2)
      }
    }

    "setRecordCount()" should {
      "overwrite the previous entry" in {
        val bk = getBookkeeper()

        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 100, 10, 1597318833, 1597318837, isTableTransient = false)
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 200, 20, 1597318838, 1597318839, isTableTransient = false)

        val chunks = bk.getDataChunks("table", infoDate1, infoDate1)

        assert(chunks.size == 1)

        assert(chunks.head.infoDate == "2020-08-11")
        assert(chunks.head.jobFinished == 1597318839)
      }

    }

    "saveSchema" should {
      "overwrite a schema entry" in {
        val bk = getBookkeeper()

        bk.saveSchema("table", infoDate2, schema1)
        Thread.sleep(10)
        bk.saveSchema("table", infoDate2, schema2)

        val actualSchema = bk.getLatestSchema("table", infoDate2)

        assert(actualSchema.isDefined)
        assert(actualSchema.get._1 == schema2)
      }
    }

    "getLatestSchema" should {
      "return the latest schema" in {
        val bk = getBookkeeper()

        bk.saveSchema("table", infoDate1, schema1)
        bk.saveSchema("table", infoDate2, schema2)

        val actualSchema1 = bk.getLatestSchema("table", infoDate1)
        val actualSchema2 = bk.getLatestSchema("table", infoDate2)
        val actualSchema3 = bk.getLatestSchema("table", infoDate3)

        assert(actualSchema1.isDefined)
        assert(actualSchema2.isDefined)
        assert(actualSchema3.isDefined)

        assert(actualSchema1.get._1 == schema1)
        assert(actualSchema2.get._1 == schema2)
        assert(actualSchema3.get._1 == schema2)
      }

      "return None is schema is not available" in {
        val bk = getBookkeeper()

        bk.saveSchema("table", infoDate2, schema1)

        val actualSchema1 = bk.getLatestSchema("table", infoDate1)
        val actualSchema2 = bk.getLatestSchema("table2", infoDate2)

        assert(actualSchema1.isEmpty)
        assert(actualSchema2.isEmpty)
      }
    }

    "Multiple bookkeepers" should {
      "share the state" in {
        val bk = getBookkeeper()

        // A workaround for BookkeeperDeltaTable which outputs to different tables in unit tests
        val bk2 = if (bk.isInstanceOf[BookkeeperDeltaTable]) bk else getBookkeeper()
        val bk3 = if (bk.isInstanceOf[BookkeeperDeltaTable]) bk else getBookkeeper()

        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 100, 10, 1597318833, 1597318837, isTableTransient = false)
        bk.setRecordCount("table", infoDate1, infoDate1, infoDate1, 200, 20, 1597318838, 1597318839, isTableTransient = false)

        bk2.setRecordCount("table", infoDate2, infoDate2, infoDate2, 101, 10, 1597318833, 1597318837, isTableTransient = false)
        bk2.setRecordCount("table", infoDate3, infoDate3, infoDate3, 201, 20, 1597318838, 1597318839, isTableTransient = false)

        bk3.setRecordCount("table", infoDate3, infoDate3, infoDate3, 102, 10, 1597318833, 1597318837, isTableTransient = false)
        bk3.setRecordCount("table", infoDate2, infoDate2, infoDate2, 202, 20, 1597318838, 1597318839, isTableTransient = false)

        val chunks = bk.getDataChunks("table", infoDate1, infoDate3).sortBy(_.infoDate)
        val chunks2 = bk2.getDataChunks("table", infoDate1, infoDate3).sortBy(_.infoDate)
        val chunks3 = bk3.getDataChunks("table", infoDate1, infoDate3).sortBy(_.infoDate)

        assert(chunks.size == 3)
        assert(chunks.head.infoDate == "2020-08-11")
        assert(chunks.head.inputRecordCount == 200)

        assert(chunks(1).infoDate == "2020-08-12")
        assert(chunks(1).inputRecordCount == 202)

        assert(chunks(2).infoDate == "2020-08-13")

        // Both are correct, depending on the notion of 'latest' used.
        assert(chunks(2).inputRecordCount == 201 || chunks(2).inputRecordCount == 102)

        assert(chunks == chunks2)
        assert(chunks == chunks3)
      }
    }
  }

}
