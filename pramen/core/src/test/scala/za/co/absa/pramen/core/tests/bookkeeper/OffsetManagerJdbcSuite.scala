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
import za.co.absa.pramen.api.offset.DataOffset.{CommittedOffset, UncommittedOffset}
import za.co.absa.pramen.api.offset.{OffsetType, OffsetValue}
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
    new OffsetManagerJdbc(pramenDb.slickDb, 123L)
  }

  "getOffsets" should {
    "should return an empty array if nothing to return " in {
      val om = getOffsetManager

      val actual = om.getOffsets("table1", infoDate)

      assert(actual.isEmpty)
    }

    "return uncommitted offsets" in {
      val now = Instant.now()
      val nextHour = now.plusSeconds(3600)
      val om = getOffsetManager

      om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

      val actualNonEmpty = om.getOffsets("table1", infoDate)
      val actualEmpty1 = om.getOffsets("table1", infoDate.plusDays(1))
      val actualEmpty2 = om.getOffsets("table1", infoDate.minusDays(1))

      assert(actualEmpty1.isEmpty)
      assert(actualEmpty2.isEmpty)

      assert(actualNonEmpty.nonEmpty)
      assert(actualNonEmpty.length == 1)

      val offset = actualNonEmpty.head.asInstanceOf[UncommittedOffset]

      assert(offset.infoDate == infoDate)
      assert(!offset.createdAt.isBefore(now))
      assert(offset.createdAt.isBefore(nextHour))
    }

    "return committed offsets" in {
      val now = Instant.now()
      val nextHour = now.plusSeconds(3600)
      val om = getOffsetManager

      val transactionReference = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      Thread.sleep(10)
      om.commitOffsets(transactionReference, OffsetValue.IntegralValue(1), OffsetValue.IntegralValue(100))

      val actualNonEmpty = om.getOffsets("table1", infoDate)
      val actualEmpty1 = om.getOffsets("table1", infoDate.plusDays(1))
      val actualEmpty2 = om.getOffsets("table1", infoDate.minusDays(1))

      assert(actualEmpty1.isEmpty)
      assert(actualEmpty2.isEmpty)

      assert(actualNonEmpty.nonEmpty)
      assert(actualNonEmpty.length == 1)

      val offset = actualNonEmpty.head.asInstanceOf[CommittedOffset]

      assert(offset.infoDate == infoDate)
      assert(!offset.createdAt.isAfter(now))
      assert(offset.createdAt.isBefore(nextHour))
      assert(!offset.committedAt.isBefore(now))
      assert(offset.committedAt.isBefore(nextHour))
      assert(offset.committedAt.isAfter(actualNonEmpty.head.createdAt))
      assert(offset.minOffset == OffsetValue.IntegralValue(1))
      assert(offset.maxOffset == OffsetValue.IntegralValue(100))
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

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(1), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(101), OffsetValue.IntegralValue(200))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate.plusDays(1), OffsetType.IntegralType)
      om.commitOffsets(t3, OffsetValue.IntegralValue(201), OffsetValue.IntegralValue(300))

      val summaryMultiDay = om.getMaxInfoDateAndOffset("table1", None)
      val summarySingleDay = om.getMaxInfoDateAndOffset("table1", Some(infoDate))

      assert(summaryMultiDay.isDefined)
      assert(summarySingleDay.isDefined)

      val actualM = summaryMultiDay.get
      val actual1 = summarySingleDay.get

      assert(actualM.tableName == "table1")
      assert(actualM.maximumInfoDate == infoDate.plusDays(1))
      assert(actualM.minimumOffset == OffsetValue.IntegralValue(201))
      assert(actualM.maximumOffset == OffsetValue.IntegralValue(300))
      assert(actualM.offsetsForTheDay.length == 1)

      assert(actual1.tableName == "table1")
      assert(actual1.maximumInfoDate == infoDate)
      assert(actual1.minimumOffset == OffsetValue.IntegralValue(1))
      assert(actual1.maximumOffset == OffsetValue.IntegralValue(200))
      assert(actual1.offsetsForTheDay.length == 2)
    }
  }

  "committing offsets" should {
    "work for empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(10), OffsetValue.IntegralValue(100))

      val offsets = om.getOffsets("table1", infoDate)

      assert(offsets.length == 1)

      val offset = offsets.head.asInstanceOf[CommittedOffset]
      assert(offset.tableName == "table1")
      assert(offset.infoDate == infoDate)
      assert(offset.minOffset == OffsetValue.IntegralValue(10))
      assert(offset.maxOffset == OffsetValue.IntegralValue(100))
    }

    "work for non-empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(10), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(101), OffsetValue.IntegralValue(200))

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 2)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset == OffsetValue.IntegralValue(10))
      assert(offset1.maxOffset == OffsetValue.IntegralValue(100))

      val offset2 = offsets(1)
      assert(offset2.tableName == "table1")
      assert(offset2.infoDate == infoDate)
      assert(offset2.minOffset == OffsetValue.IntegralValue(101))
      assert(offset2.maxOffset == OffsetValue.IntegralValue(200))
    }
  }

  "committing reruns" should {
    "work with reruns that do not change offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(101), OffsetValue.IntegralValue(200))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitRerun(t3, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(200))

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset.valueString == "0")
      assert(offset1.maxOffset.valueString == "200")
    }

    "work with reruns that do change offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate.minusDays(1), OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(101), OffsetValue.IntegralValue(200))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitRerun(t3, OffsetValue.IntegralValue(201), OffsetValue.IntegralValue(300))

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset.valueString == "201")
      assert(offset1.maxOffset.valueString == "300")
    }

    "work with reruns that deletes all data and no previous offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(101), OffsetValue.IntegralValue(200))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitRerun(t3, OffsetValue.IntegralValue(-1), OffsetValue.IntegralValue(-1))

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset.valueString.toLong == -1)
      assert(offset1.maxOffset.valueString.toLong == -1)
    }

    "work with reruns that deletes all data and there are previous offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate.minusDays(1), OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(Long.MinValue), OffsetValue.IntegralValue(0))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(1), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitRerun(t3, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(0))

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset.valueString.toLong == 0)
      assert(offset1.maxOffset.valueString.toLong == 0)
    }

    "throw an exception when offsets are incorrect" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(Long.MinValue), OffsetValue.IntegralValue(0))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t2, OffsetValue.IntegralValue(1), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t3 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      val ex = intercept[IllegalArgumentException] {
        om.commitRerun(t3, OffsetValue.IntegralValue(1), OffsetValue.IntegralValue(-1))
      }

      assert(ex.getMessage == "minOffset is greater than maxOffset: 1 > -1")

      val offsets0 = om.getOffsets("table1", infoDate)
        .filter(_.isCommitted)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)
      val offsets1 = om.getOffsets("table1", infoDate)
        .filter(!_.isCommitted)
        .map(_.asInstanceOf[UncommittedOffset])

      assert(offsets0.length == 2)
      assert(offsets1.length == 1)

      val offset1 = offsets0.head
      val offset2 = offsets0(1)
      val offset3 = offsets1.head

      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset.valueString.toLong == Long.MinValue)
      assert(offset1.maxOffset.valueString.toLong == 0)

      assert(offset2.tableName == "table1")
      assert(offset2.infoDate == infoDate)
      assert(offset2.minOffset.valueString.toLong == 1)
      assert(offset2.maxOffset.valueString.toLong == 100)

      assert(offset3.tableName == "table1")
      assert(offset3.infoDate == infoDate)
    }
  }

  "rollback offsets" should {
    "work for empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.rollbackOffsets(t1)

      val offsets = om.getOffsets("table1", infoDate)

      assert(offsets.isEmpty)
    }

    "work for non-empty offsets" in {
      val om = getOffsetManager

      val t1 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.commitOffsets(t1, OffsetValue.IntegralValue(0), OffsetValue.IntegralValue(100))
      Thread.sleep(10)

      val t2 = om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)
      om.rollbackOffsets(t2)

      val offsets = om.getOffsets("table1", infoDate)
        .map(_.asInstanceOf[CommittedOffset])
        .sortBy(_.minOffset.valueString.toLong)

      assert(offsets.length == 1)

      val offset1 = offsets.head
      assert(offset1.tableName == "table1")
      assert(offset1.infoDate == infoDate)
      assert(offset1.minOffset == OffsetValue.IntegralValue(0))
      assert(offset1.maxOffset == OffsetValue.IntegralValue(100))
    }
  }

}
