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

package za.co.absa.pramen.core.tests.sql

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType, SqlGenerator, SqlGeneratorBase}
import za.co.absa.pramen.core.mocks.DummySqlConfigFactory
import za.co.absa.pramen.core.sql.SqlGeneratorDb2._
import za.co.absa.pramen.core.tests.sql.SqlGeneratorGenericSuite.testDefaultTz

import java.time.{Instant, LocalDate}

class SqlGeneratorDb2Suite extends AnyWordSpec {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  val driver = "com.ibm.db2.jcc.DB2Driver"

  val genDate: SqlGenerator = getSqlGenerator(driver, sqlConfigDate)
  val genDateTime: SqlGenerator = getSqlGenerator(driver, sqlConfigDateTime)
  val genStr: SqlGenerator = getSqlGenerator(driver, sqlConfigString)
  val genNum: SqlGenerator = getSqlGenerator(driver, sqlConfigNumber)
  val genEscaped: SqlGenerator = getSqlGenerator(driver, sqlConfigEscape)

  "generate count queries without date ranges" in {
    assert(genDate.getCountQuery("A") == "SELECT COUNT(*) AS CNT FROM A")
  }

  "generate data queries without date ranges" in {
    assert(genDate.getDataQuery("A", Nil, None) == "SELECT * FROM A")
  }

  "generate data queries when list of columns is specified" in {
    assert(genEscaped.getDataQuery("A", columns, None) == "SELECT \"A\", \"D\", \"Column with spaces\" FROM \"A\"")
  }

  "generate data queries with limit clause date ranges" in {
    assert(genDate.getDataQuery("A", Nil, Some(100)) == "SELECT * FROM A LIMIT 100")
  }

  "generate ranged count queries" when {
    "date is in DATE format" in {
      assert(genDate.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D = DATE '2020-08-17'")
      assert(genDate.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D >= DATE '2020-08-17' AND D <= DATE '2020-08-30'")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) = DATE '2020-08-17'")
      assert(genDateTime.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) >= DATE '2020-08-17' AND CAST(D AS DATE) <= DATE '2020-08-30'")
    }

    "date is in STRING format" in {
      assert(genStr.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D = '2020-08-17'")
      assert(genStr.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
    }

    "date is in NUMBER format" in {
      assert(genNum.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D = 20200817")
      assert(genNum.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D >= 20200817 AND D <= 20200830")
    }

    "the table name and column name need to be escaped" in {
      assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
        "SELECT COUNT(*) AS \"CNT\" FROM \"Input Table\" WHERE \"Info date\" = DATE '2020-08-17'")
      assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT(*) AS \"CNT\" FROM \"Input Table\" WHERE \"Info date\" >= DATE '2020-08-17' AND \"Info date\" <= DATE '2020-08-30'")
    }
  }

  "generate ranged data queries" when {
    "date is in DATE format" in {
      assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = DATE '2020-08-17'")
      assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= DATE '2020-08-17' AND D <= DATE '2020-08-30'")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) = DATE '2020-08-17'")
      assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) >= DATE '2020-08-17' AND CAST(D AS DATE) <= DATE '2020-08-30'")
    }

    "date is in STRING format" in {
      assert(genStr.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = '2020-08-17'")
      assert(genStr.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
    }

    "date is in NUMBER format" in {
      assert(genNum.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = 20200817")
      assert(genNum.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
    }

    "with limit records" in {
      assert(genDate.getDataQuery("A", date1, date1, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D = DATE '2020-08-17' LIMIT 100")
      assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D >= DATE '2020-08-17' AND D <= DATE '2020-08-30' LIMIT 100")
    }
  }

  "getCountQueryForSql" should {
    "generate count queries for an SQL subquery" in {
      assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) AS query")
    }
  }
  
  //   /**
  //    * Converts a DB2 timezone offset value into a Java `ZoneId`.
  //    *
  //    *  DB2 returns timezone as ±hhmmss
  //    *  Examples:
  //    *    20000  -> +02:00:00
  //    *   -53000  -> -05:30:00
  //    *
  //    * @param db2Offset the timezone offset as returned by DB2, encoded as a signed integer in the `hhmmss` format
  //    * @return a `ZoneOffset` (as a `ZoneId`) representing the same offset from UTC
  //    * @throws java.time.DateTimeException if the decoded hours, minutes or seconds are outside the range supported by `ZoneOffset`
  //    */
  //  def convertDb2TzOffsets(db2Offset: Int): ZoneId = {
  //    val sign = if (db2Offset < 0) -1 else 1
  //    val abs = math.abs(db2Offset)
  //
  //    val hours = abs / 10000
  //    val minutes = (abs % 10000) / 100
  //    val seconds = abs % 100
  //
  //    ZoneOffset.ofHoursMinutesSeconds(
  //      sign * hours,
  //      sign * minutes,
  //      sign * seconds
  //    )
  //  }
  "convertDb2TzOffsets" should {
    "convert zero offset to UTC" in {
      assert(convertDb2TzOffsets(0) == java.time.ZoneOffset.UTC)
    }

    "convert positive whole hour offsets" in {
      assert(convertDb2TzOffsets(20000) == java.time.ZoneOffset.ofHours(2))
      assert(convertDb2TzOffsets(10000) == java.time.ZoneOffset.ofHours(1))
    }

    "convert negative whole hour offsets" in {
      assert(convertDb2TzOffsets(-50000) == java.time.ZoneOffset.ofHours(-5))
      assert(convertDb2TzOffsets(-120000) == java.time.ZoneOffset.ofHours(-12))
    }

    "convert positive offsets with minutes" in {
      assert(convertDb2TzOffsets(53000) == java.time.ZoneOffset.ofHoursMinutes(5, 30))
      assert(convertDb2TzOffsets(54500) == java.time.ZoneOffset.ofHoursMinutes(5, 45))
    }

    "convert negative offsets with minutes" in {
      assert(convertDb2TzOffsets(-53000) == java.time.ZoneOffset.ofHoursMinutes(-5, -30))
      assert(convertDb2TzOffsets(-33000) == java.time.ZoneOffset.ofHoursMinutes(-3, -30))
    }

    "convert offsets with seconds" in {
      assert(convertDb2TzOffsets(53045) == java.time.ZoneOffset.ofHoursMinutesSeconds(5, 30, 45))
      assert(convertDb2TzOffsets(-53045) == java.time.ZoneOffset.ofHoursMinutesSeconds(-5, -30, -45))
    }

    "convert offsets smaller than one hour" in {
      assert(convertDb2TzOffsets(3000) == java.time.ZoneOffset.ofHoursMinutes(0, 30))
      assert(convertDb2TzOffsets(-3000) == java.time.ZoneOffset.ofHoursMinutes(0, -30))
      assert(convertDb2TzOffsets(15) == java.time.ZoneOffset.ofTotalSeconds(15))
      assert(convertDb2TzOffsets(-15) == java.time.ZoneOffset.ofTotalSeconds(-15))
    }

    "convert boundary offsets" in {
      assert(convertDb2TzOffsets(180000) == java.time.ZoneOffset.ofHours(18))
      assert(convertDb2TzOffsets(-180000) == java.time.ZoneOffset.ofHours(-18))
    }

    "return a zone id with the expected id string" in {
      assert(convertDb2TzOffsets(20000).getId == "+02:00")
      assert(convertDb2TzOffsets(-53000).getId == "-05:30")
      assert(convertDb2TzOffsets(53045).getId == "+05:30:45")
    }

    "throw an exception when hours are out of range" in {
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(190000)
      }
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(-190000)
      }
    }

    "throw an exception when minutes are out of range" in {
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(6000)
      }
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(-16000)
      }
    }

    "throw an exception when seconds are out of range" in {
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(60)
      }
      intercept[java.time.DateTimeException] {
        convertDb2TzOffsets(-10099)
      }
    }
  }

  "getDtable" should {
    "return the original table when a table is provided" in {
      assert(genDate.getDtable("A") == "A")
    }

    "wrapped query without alias for SQL queries " in {
      assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS T")
    }
  }

  "quote" should {
    "quote each subfields separately" in {
      val actual = genDate.quote("System User.\"Table Name\"")

      assert(actual == "\"System User\".\"Table Name\"")
    }
  }

  "unquote" should {
    "quote each subfields separately" in {
      val actual = genDate.unquote("System User.\"Table Name\"")

      assert(actual == "System User.Table Name")
    }
  }
  "getOffsetWhereCondition" should {
    "return the correct condition for integral offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", "<", OffsetValue.IntegralValue(1), None)

      assert(actual == "offset < 1")
    }

    "return the correct condition for datetime offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">", OffsetValue.DateTimeValue(Instant.ofEpochMilli(1727761000)), Some(testDefaultTz))

      assert(actual == "offset > TIMESTAMP('1970-01-21 01:56:01.000')")
    }

    "return the correct condition for string offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">=", OffsetValue.StringValue("AAA"), None)

      assert(actual == "offset >= 'AAA'")
    }
  }
}
