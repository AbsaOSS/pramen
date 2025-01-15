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

import java.time.{Instant, LocalDate}

class SqlGeneratorDatabricksSuite extends AnyWordSpec {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  val driver = "com.databricks.client.jdbc.Driver"

  val gen: SqlGenerator = getSqlGenerator(driver, sqlConfigDate)
  val genStr: SqlGenerator = getSqlGenerator(driver, sqlConfigString)
  val genNum: SqlGenerator = getSqlGenerator(driver, sqlConfigNumber)
  val genDateTime: SqlGenerator = getSqlGenerator(driver, sqlConfigDateTime)
  val genEscaped: SqlGenerator = getSqlGenerator(driver, sqlConfigEscape)
  val genEscaped2: SqlGenerator = getSqlGenerator(driver, DummySqlConfigFactory.getDummyConfig(infoDateColumn = "`Info date`", identifierQuotingPolicy = QuotingPolicy.Auto))

  "generate count queries without date ranges" in {
    assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
  }

  "generate data queries without date ranges" in {
    assert(gen.getDataQuery("A", Nil, None) == "SELECT * FROM A")
  }

  "generate data queries when list of columns is specified" in {
    assert(genEscaped.getDataQuery("A", columns, None) == "SELECT `A`, `D`, `Column with spaces` FROM `A`")
  }

  "generate data queries with limit clause date ranges" in {
    assert(gen.getDataQuery("A", Nil, Some(100)) == "SELECT * FROM A LIMIT 100")
  }

  "generate ranged count queries" when {
    "date is in DATE format" in {
      assert(gen.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE D = to_date('2020-08-17')")
      assert(gen.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30')")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) = to_date('2020-08-17')")
      assert(genDateTime.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) >= to_date('2020-08-17') AND CAST(D AS DATE) <= to_date('2020-08-30')")
    }

    "date is in STRING format" in {
      assert(genStr.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE D = '2020-08-17'")
      assert(genStr.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
    }

    "date is in NUMBER format" in {
      assert(genNum.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE D = 20200817")
      assert(genNum.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE D >= 20200817 AND D <= 20200830")
    }

    "the table name and column name need to be escaped" in {
      assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
        "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` = to_date('2020-08-17')")
      assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` >= to_date('2020-08-17') AND `Info date` <= to_date('2020-08-30')")
    }

    "the table name and column name already escaped" in {
      assert(genEscaped2.getCountQuery("Input Table", date1, date1) ==
        "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` = to_date('2020-08-17')")
      assert(genEscaped2.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` >= to_date('2020-08-17') AND `Info date` <= to_date('2020-08-30')")
    }
  }

  "generate ranged data queries" when {
    "date is in DATE format" in {
      assert(gen.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = to_date('2020-08-17')")
      assert(gen.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30')")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) = to_date('2020-08-17')")
      assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) >= to_date('2020-08-17') AND CAST(D AS DATE) <= to_date('2020-08-30')")
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
      assert(gen.getDataQuery("A", date1, date1, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D = to_date('2020-08-17') LIMIT 100")
      assert(gen.getDataQuery("A", date1, date2, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30') LIMIT 100")
    }
  }

  "getCountQueryForSql" should {
    "generate count queries for an SQL subquery" in {
      assert(gen.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) AS query")
    }
  }

  "getDtable" should {
    "return the original table when a table is provided" in {
      assert(gen.getDtable("A") == "(A) tbl")
    }

    "wrapped query without alias for SQL queries " in {
      assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B) tbl")
    }
  }

  "quote" should {
    "escape each subfields separately" in {
      val actual = gen.quote("System User.`Table Name`")

      assert(actual == "`System User`.`Table Name`")
    }
  }

  "getOffsetWhereCondition" should {
    "return the correct condition for integral offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", "<", OffsetValue.IntegralValue(1))

      assert(actual == "offset < 1")
    }

    "return the correct condition for datetime offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">", OffsetValue.DateTimeValue(Instant.ofEpochMilli(1727761000)))

      assert(actual == "offset > '1970-01-21 01:56:01.000'")
    }

    "return the correct condition for string offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">=", OffsetValue.StringValue("AAA"))

      assert(actual == "offset >= 'AAA'")
    }
  }
}
