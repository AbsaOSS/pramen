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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.bookkeeper.model.OffsetValue
import za.co.absa.pramen.core.bookkeeper.{OffsetManager, OffsetManagerJdbc}
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.time.{Instant, LocalDate}

class OffsetManagerJdbcSuite extends AnyWordSpec with RelationalDbFixture with BeforeAndAfter with BeforeAndAfterAll {
  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  private val infoDate = LocalDate.of(2023, 8, 25)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  def getOffsetManager: OffsetManager = {
    new OffsetManagerJdbc(pramenDb.slickDb)
  }

  "getOffsets" should {
    "should return an empty array if nothing to return " in {
      val om = getOffsetManager

      val actual = om.getOffsets("table1", infoDate)

      assert(actual.isEmpty)
    }

    "return uncommitted offsets" in {
      val now = Instant.now().toEpochMilli
      val nextHour = now + 3600000
      val om = getOffsetManager

      om.startWriteOffsets("table1", infoDate, OffsetValue.getMinimumForType("long"))

      val actualNonEmpty = om.getOffsets("table1", infoDate)
      val actualEmpty1 = om.getOffsets("table1", infoDate.plusDays(1))
      val actualEmpty2 = om.getOffsets("table1", infoDate.minusDays(1))

      assert(actualEmpty1.isEmpty)
      assert(actualEmpty2.isEmpty)

      assert(actualNonEmpty.nonEmpty)
      assert(actualNonEmpty.length == 1)

      val offset = actualNonEmpty.head

      assert(offset.infoDate == infoDate)
      assert(offset.createdAt >= now)
      assert(offset.createdAt < nextHour)
      assert(offset.committedAt.isEmpty)
      assert(offset.minOffset == OffsetValue.getMinimumForType("long"))
      assert(offset.maxOffset.isEmpty)
    }

    "return committed offsets" in {
      val now = Instant.now().toEpochMilli
      val nextHour = now + 3600000
      val om = getOffsetManager

      val transactionReference = om.startWriteOffsets("table1", infoDate, OffsetValue.getMinimumForType("long"))
      Thread.sleep(10)
      om.commitOffsets(transactionReference, OffsetValue.LongType(100))

      val actualNonEmpty = om.getOffsets("table1", infoDate)
      val actualEmpty1 = om.getOffsets("table1", infoDate.plusDays(1))
      val actualEmpty2 = om.getOffsets("table1", infoDate.minusDays(1))

      assert(actualEmpty1.isEmpty)
      assert(actualEmpty2.isEmpty)

      assert(actualNonEmpty.nonEmpty)
      assert(actualNonEmpty.length == 1)

      val offset = actualNonEmpty.head

      assert(offset.infoDate == infoDate)
      assert(offset.createdAt >= now)
      assert(offset.createdAt < nextHour)
      assert(offset.committedAt.get >= now)
      assert(offset.committedAt.get < nextHour)
      assert(offset.committedAt.get > actualNonEmpty.head.createdAt)
      assert(offset.minOffset == OffsetValue.getMinimumForType("long"))
      assert(offset.maxOffset.get == OffsetValue.fromString("long", "100"))
    }
  }

  "getMaximumDateAndOffset" should {
    "return None when no offsets have loaded yet" in {
      val om = getOffsetManager

      val actual = om.getMaxInfoDateAndOffset("table1", None)

      assert(actual.isEmpty)
    }

    "return None when no offsets have loaded yet for the info date" in {
      val om = getOffsetManager

      val actual = om.getMaxInfoDateAndOffset("table1", Some(infoDate))

      assert(actual.isEmpty)
    }

    "return the maximum info date and offsets " in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetValue.getMinimumForType("long"))
      om.commitOffsets(t1, OffsetValue.LongType(100))

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(100))
      om.commitOffsets(t2, OffsetValue.LongType(200))

      val t3 = om.startWriteOffsets("table1", infoDate.plusDays(1), OffsetValue.LongType(200))
      om.commitOffsets(t3, OffsetValue.LongType(300))

      om.getOffsets("table1", infoDate.plusDays(1)).foreach(println)

      val summaryMultiDay = om.getMaxInfoDateAndOffset("table1", None)
      val summarySingleDay = om.getMaxInfoDateAndOffset("table1", Some(infoDate))

      assert(summaryMultiDay.isDefined)
      assert(summarySingleDay.isDefined)

      val actualM = summaryMultiDay.get
      val actual1 = summarySingleDay.get

      assert(actualM.tableName == "table1")
      assert(actualM.maximumInfoDate == infoDate.plusDays(1))
      assert(actualM.minimumOffset == OffsetValue.LongType(200))
      assert(actualM.maximumOffset == OffsetValue.LongType(300))
      assert(actualM.offsetsForTheDay.length == 1)

      assert(actual1.tableName == "table1")
      assert(actual1.maximumInfoDate == infoDate)
      assert(actual1.minimumOffset == OffsetValue.getMinimumForType("long"))
      assert(actual1.maximumOffset == OffsetValue.LongType(200))
      assert(actual1.offsetsForTheDay.length == 2)
    }
  }

  "committing offsets" should {
    "work for empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(0))
      om.commitOffsets(t1, OffsetValue.LongType(100))

      val offsets = om.getOffsets("table1", infoDate)

      assert(offsets.length == 1)

      val offset = offsets.head
      assert(offset.tableName == "table1")
      assert(offset.infoDate == infoDate)
      assert(offset.minOffset == OffsetValue.LongType(0))
      assert(offset.maxOffset.get == OffsetValue.LongType(100))
    }

    "work for non-empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(0))
      om.commitOffsets(t1, OffsetValue.LongType(100))

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(100))
      om.commitOffsets(t2, OffsetValue.LongType(200))

      val offsets = om.getOffsets("table1", infoDate).sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 2)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset == OffsetValue.LongType(0))
      assert(offset1.maxOffset.get == OffsetValue.LongType(100))

      val offset2 = offsets(1)
      assert(offset2.tableName == "table1")
      assert(offset2.infoDate == infoDate)
      assert(offset2.minOffset == OffsetValue.LongType(100))
      assert(offset2.maxOffset.get == OffsetValue.LongType(200))
    }
  }

  "rollback offsets" should {
    "work for empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(0))
      om.rollbackOffsets(t1)

      val offsets = om.getOffsets("table1", infoDate)

      assert(offsets.isEmpty)
    }

    "work for non-empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(0))
      om.commitOffsets(t1, OffsetValue.LongType(100))

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetValue.LongType(100))
      om.rollbackOffsets(t2)

      val offsets = om.getOffsets("table1", infoDate).sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset == OffsetValue.LongType(0))
      assert(offset1.maxOffset.get == OffsetValue.LongType(100))
    }
  }

}
