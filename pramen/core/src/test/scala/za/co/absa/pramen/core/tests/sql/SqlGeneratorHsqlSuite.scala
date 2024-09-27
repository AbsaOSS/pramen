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
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType, SqlGenerator}
import za.co.absa.pramen.core.mocks.DummySqlConfigFactory

import java.time.LocalDate

class SqlGeneratorHsqlSuite extends AnyWordSpec {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  val driver = "org.hsqldb.jdbc.JDBCDriver"

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
        "SELECT COUNT(*) AS CNT FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
      assert(genDate.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
      assert(genDateTime.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
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
      assert(genEscaped.getCountQuery("SELECT", date1, date1) ==
        "SELECT COUNT(*) AS \"CNT\" FROM \"SELECT\" WHERE \"Info date\" = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
      assert(genEscaped.getCountQuery("SELECT", date1, date2) ==
        "SELECT COUNT(*) AS \"CNT\" FROM \"SELECT\" WHERE \"Info date\" >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND \"Info date\" <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
    }
  }

  "generate ranged data queries" when {
    "date is in DATE format" in {
      assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
      assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
      assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
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
        "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD') LIMIT 100")
      assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD') LIMIT 100")
    }
  }

  "getCountQueryForSql" should {
    "generate count queries for an SQL subquery" in {
      assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B)")
    }
  }

  "getDtable" should {
    "return the original table when a table is provided" in {
      assert(genDate.getDtable("A") == "A")
    }

    "wrapped query without alias for SQL queries " in {
      assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) t")
    }
  }

  "quote" should {
    "escape each subfields separately" in {
      val actual = genDate.quote("System User.\"Table Name\"")

      assert(actual == "\"System User\".\"Table Name\"")
    }

    "throw an exception if a column contains a single quote" in {
      assertThrows[IllegalArgumentException] {
        genDate.quote("ABC ' DEF")
      }
    }

    "throw an exception if a column contains a semicolon" in {
      assertThrows[IllegalArgumentException] {
        genDate.quote("ABC ; DEF")
      }
    }
  }
}