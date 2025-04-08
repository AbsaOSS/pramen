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
import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetType, OffsetValue}
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType, SqlGenerator}
import za.co.absa.pramen.core.mocks.DummySqlConfigFactory
import za.co.absa.pramen.core.sql.SqlGeneratorMicrosoft

import java.time.{Instant, LocalDate}

class SqlGeneratorMicrosoftSuite extends AnyWordSpec {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D", offsetInfo = Some(OffsetInfo("offset", OffsetType.IntegralType)))
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D", offsetInfo = Some(OffsetInfo("offset", OffsetType.DateTimeType)))
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  val driver = "net.sourceforge.jtds.jdbc.Driver"

  val genDate: SqlGenerator = getSqlGenerator(driver, sqlConfigDate)
  val genDateTime: SqlGenerator = getSqlGenerator(driver, sqlConfigDateTime)
  val genStr: SqlGenerator = getSqlGenerator(driver, sqlConfigString)
  val genStr2: SqlGenerator = getSqlGenerator(driver, DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, dateFormatApp = "yyyyMMdd",  infoDateColumn = "D"))
  val genNum: SqlGenerator = getSqlGenerator(driver, sqlConfigNumber)
  val genEscaped: SqlGenerator = getSqlGenerator(driver, sqlConfigEscape)
  val genEscaped2: SqlGenerator = getSqlGenerator(driver,  DummySqlConfigFactory.getDummyConfig(infoDateColumn = "[Info date]", identifierQuotingPolicy = QuotingPolicy.Auto))

  "generate schema query without list of columns specified" in {
    assert(genDate.getSchemaQuery("A", Seq.empty) == "SELECT * FROM A WITH (NOLOCK) WHERE 0=1")
  }

  "generate schema queries when list of columns is specified" in {
    assert(genEscaped.getSchemaQuery("A", columns) == "SELECT [A], [D], [Column with spaces] FROM [A] WITH (NOLOCK) WHERE 0=1")
  }

  "generate count queries without date ranges" in {
    assert(genDate.getCountQuery("A") == "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK)")
  }

  "generate data queries without date ranges" in {
    assert(genDate.getDataQuery("A", Nil, None) == "SELECT * FROM A WITH (NOLOCK)")
  }

  "generate data queries when list of columns is specified" in {
    assert(genEscaped.getDataQuery("A", columns, None) == "SELECT [A], [D], [Column with spaces] FROM [A] WITH (NOLOCK)")
  }

  "generate data queries with limit clause date ranges" in {
    assert(genDate.getDataQuery("A", Nil, Some(100)) == "SELECT TOP 100 * FROM A WITH (NOLOCK)")
  }

  "generate data queries with limit clause date ranges when table is quoted" in {
    assert(genDate.getDataQuery("\"A\"", Nil, Some(100)) == "SELECT TOP 100 * FROM \"A\" WITH (NOLOCK)")
  }

  "generate data queries with limit clause date ranges when table is escaped in brackets" in {
    assert(genDate.getDataQuery("[A]", Nil, Some(100)) == "SELECT TOP 100 * FROM [A] WITH (NOLOCK)")
  }

  "generate ranged count queries" when {
    "date is in DATE format" in {
      assert(genDate.getCountQuery("A", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23)")
      assert(genDate.getCountQuery("A", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17', 23) AND D <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getCountQuery("A", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
      assert(genDateTime.getCountQuery("A", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) AND CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in STRING ISO format" in {
      assert(genStr.getCountQuery("A", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
      assert(genStr.getCountQuery("A", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) " +
          "AND TRY_CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in STRING non ISO format" in {
      assert(genStr2.getCountQuery("A", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D = '20200817'")
      assert(genStr2.getCountQuery("A", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= '20200817' AND D <= '20200830'")
    }

    "date is in NUMBER format" in {
      assert(genNum.getCountQuery("A", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D = 20200817")
      assert(genNum.getCountQuery("A", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= 20200817 AND D <= 20200830")
    }

    "the table name and column name need to be escaped" in {
      assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
        "SELECT COUNT_BIG(*) AS [CNT] FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17', 23)")
      assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT_BIG(*) AS [CNT] FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17', 23) AND [Info date] <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "the table name and column name already escaped" in {
      assert(genEscaped2.getCountQuery("Input Table", date1, date1) ==
        "SELECT COUNT_BIG(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17', 23)")
      assert(genEscaped2.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT_BIG(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17', 23) AND [Info date] <= CONVERT(DATE, '2020-08-30', 23)")
    }
  }

  "generate ranged data queries" when {
    "date is in DATE format" in {
      assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23)")
      assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17', 23) AND D <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
      assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) AND CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in STRING ISO format" in {
      assert(genStr.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
      assert(genStr.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) " +
          "AND TRY_CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
    }

    "date is in STRING non-ISO format" in {
      assert(genStr2.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D = '20200817'")
      assert(genStr2.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D >= '20200817' AND D <= '20200830'")
    }

    "date is in NUMBER format" in {
      assert(genNum.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D = 20200817")
      assert(genNum.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WITH (NOLOCK) WHERE D >= 20200817 AND D <= 20200830")
    }

    "with limit records" in {
      assert(genDate.getDataQuery("A", date1, date1, Nil, Some(100)) ==
        "SELECT TOP 100 * FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23)")
      assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
        "SELECT TOP 100 * FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17', 23) AND D <= CONVERT(DATE, '2020-08-30', 23)")
    }
  }

  "getCountQueryForSql" should {
    "generate count queries for an SQL subquery" in {
      assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT_BIG(*) FROM (SELECT A FROM B) AS query")
    }
  }

  "getDtable" should {
    "return the original table when a table is provided" in {
      assert(genDate.getDtable("A") == "A")
    }

    "wrapped query with alias for SQL queries " in {
      assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS tbl")
    }
  }

  "quote" should {
    "escape each subfields separately" in {
      val actual = genDate.quote("System User.[Table Name]")

      assert(actual == "[System User].[Table Name]")
    }
  }

  "unquote" should {
    "quote each subfields separately using quotes" in {
      val actual = genDate.unquote("System User.\"Table Name\"")

      assert(actual == "System User.Table Name")
    }

    "quote each subfields separately using brackets" in {
      val actual = genDate.unquote("[System User].[Table Name]")

      assert(actual == "System User.Table Name")
    }
  }

  "splitComplexIdentifier" should {
    "throw on an empty identifier" in {
      assertThrows[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier(" ")
      }
    }

    "keep original column name as is" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User")

      assert(actual == Seq("System User"))
    }

    "split a complex column with brackets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User.[Table Name]")

      assert(actual == Seq("System User", "[Table Name]"))
    }

    "split a complex column with quotes 1" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("\"System User\".Table Name")

      assert(actual == Seq("\"System User\"", "Table Name"))
    }

    "split a complex column with quotes 2" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("[System User].\"Table Name\"")

      assert(actual == Seq("[System User]", "\"Table Name\""))
    }

    "handle escaped dots with brackets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("[System User.Table Name]")

      assert(actual == Seq("[System User.Table Name]"))
    }

    "handle escaped dots with quotes" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("\"System User.Table Name\"")

      assert(actual == Seq("\"System User.Table Name\""))
    }

    "throw an exception if brackets are found inside the identifier" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System Use[r.T]able Name")
      }

      assert(ex.getMessage.contains("Invalid character '[' in the identifier 'System Use[r.T]able Name', position 10."))
    }

    "throw an exception if quotes are found inside the identifier" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System Use\"r.T\"able Name")
      }

      assert(ex.getMessage.contains("Invalid character '\"' in the identifier 'System Use\"r.T\"able Name', position 10."))
    }

    "throw on unmatched open bracket" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User.[Table Name")
      }

      assert(ex.getMessage.contains("Found not matching '[' in the identifier 'System User.[Table Name'"))
    }

    "throw on unmatched open double quote" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User.\"Table Name")
      }

      assert(ex.getMessage.contains("Found not matching '\"' in the identifier 'System User.\"Table Name'"))
    }

    "throw on unmatched closing bracket" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User.Table Name]")
      }

      assert(ex.getMessage.contains("Found not matching ']' in the identifier 'System User.Table Name]'"))
    }

    "throw on unmatched double quote at the and" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("System User.Table Name\"")
      }

      assert(ex.getMessage.contains("Found not matching '\"' in the identifier 'System User.Table Name\"'"))
    }

    "throw on invalid open double-quote" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("\"System User.\"Table Name")
      }

      assert(ex.getMessage.contains("Invalid character '\"' in the identifier '\"System User.\"Table Name'"))
    }

    "throw on invalid close double-quote" in {
      val ex = intercept[IllegalArgumentException] {
        genDate.asInstanceOf[SqlGeneratorMicrosoft].splitComplexIdentifier("\"System Use\"r.Table Name")
      }

      assert(ex.getMessage.contains("Invalid character '\"' in the identifier '\"System Use\"r.Table Name'"))
    }
  }

  "getDataQueryIncremental" should {
    "integral offset" when {
      "info date is absent" when {
        "work without offsets" in {
          val sql = genDate.getDataQueryIncremental("table1", None, None, None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK)")
        }

        "work with only from offset" in {
          val sql = genDate.getDataQueryIncremental("table1", None, Some(OffsetValue.IntegralValue(1)), None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset > 1")
        }

        "work with only to offset" in {
          val sql = genDate.getDataQueryIncremental("table1", None, None, Some(OffsetValue.IntegralValue(1)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset <= 1")
        }

        "work with from and to offsets" in {
          val sql = genDate.getDataQueryIncremental("table1", None, Some(OffsetValue.IntegralValue(1)), Some(OffsetValue.IntegralValue(2)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset >= 1 AND offset <= 2")
        }

        "work with from and to offsets, column projection" in {
          val sql = genDate.getDataQueryIncremental("table1", None, Some(OffsetValue.IntegralValue(1)), Some(OffsetValue.IntegralValue(2)), columns)

          assert(sql == "SELECT A, D, [Column with spaces] FROM table1 WITH (NOLOCK) WHERE offset >= 1 AND offset <= 2")
        }
      }
      "info date is present" when {
        "work without offsets" in {
          val sql = genDate.getDataQueryIncremental("table1", Some(date1), None, None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23)")
        }

        "work with only from offset" in {
          val sql = genDate.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.IntegralValue(1)), None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23) AND offset > 1")
        }

        "work with only to offset" in {
          val sql = genDate.getDataQueryIncremental("table1", Some(date1), None, Some(OffsetValue.IntegralValue(1)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23) AND offset <= 1")
        }

        "work with from and to offsets" in {
          val sql = genDate.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.IntegralValue(1)), Some(OffsetValue.IntegralValue(2)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23) AND offset >= 1 AND offset <= 2")
        }

        "work with from and to offsets, column projection" in {
          val sql = genDate.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.IntegralValue(1)), Some(OffsetValue.IntegralValue(2)), columns)

          assert(sql == "SELECT A, D, [Column with spaces] FROM table1 WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23) AND offset >= 1 AND offset <= 2")
        }
      }
    }
    "datetime offset" when {
      val offset1 = Instant.ofEpochMilli(1727761000)
      val offset2 = Instant.ofEpochMilli(1727762000)
      "info date is absent" when {
        "work without offsets" in {
          val sql = genDateTime.getDataQueryIncremental("table1", None, None, None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK)")
        }

        "work with only from offset" in {
          val sql = genDateTime.getDataQueryIncremental("table1", None, Some(OffsetValue.DateTimeValue(offset1)), None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset > '1970-01-21 01:56:01.000'")
        }

        "work with only to offset" in {
          val sql = genDateTime.getDataQueryIncremental("table1", None, None, Some(OffsetValue.DateTimeValue(offset1)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset <= '1970-01-21 01:56:01.000'")
        }

        "work with from and to offsets" in {
          val sql = genDateTime.getDataQueryIncremental("table1", None, Some(OffsetValue.DateTimeValue(offset1)), Some(OffsetValue.DateTimeValue(offset2)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE offset >= '1970-01-21 01:56:01.000' AND offset <= '1970-01-21 01:56:02.000'")
        }

        "work with from and to offsets, column projection" in {
          val sql = genDateTime.getDataQueryIncremental("table1", None, Some(OffsetValue.DateTimeValue(offset1)), Some(OffsetValue.DateTimeValue(offset2)), columns)

          assert(sql == "SELECT A, D, [Column with spaces] FROM table1 WITH (NOLOCK) WHERE offset >= '1970-01-21 01:56:01.000' AND offset <= '1970-01-21 01:56:02.000'")
        }
      }
      "info date is present" when {
        "work without offsets" in {
          val sql = genDateTime.getDataQueryIncremental("table1", Some(date1), None, None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
        }

        "work with only from offset" in {
          val sql = genDateTime.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.DateTimeValue(offset1)), None, Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23) AND offset > '1970-01-21 01:56:01.000'")
        }

        "work with only to offset" in {
          val sql = genDateTime.getDataQueryIncremental("table1", Some(date1), None, Some(OffsetValue.DateTimeValue(offset1)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23) AND offset <= '1970-01-21 01:56:01.000'")
        }

        "work with from and to offsets" in {
          val sql = genDateTime.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.DateTimeValue(offset1)), Some(OffsetValue.DateTimeValue(offset2)), Seq.empty)

          assert(sql == "SELECT * FROM table1 WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23) AND offset >= '1970-01-21 01:56:01.000' AND offset <= '1970-01-21 01:56:02.000'")
        }

        "work with from and to offsets, column projection" in {
          val sql = genDateTime.getDataQueryIncremental("table1", Some(date1), Some(OffsetValue.DateTimeValue(offset1)), Some(OffsetValue.DateTimeValue(offset2)), columns)

          assert(sql == "SELECT A, D, [Column with spaces] FROM table1 WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23) AND offset >= '1970-01-21 01:56:01.000' AND offset <= '1970-01-21 01:56:02.000'")
        }
      }
    }
  }

  "getOffsetWhereCondition" should {
    "return the correct condition for integral offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft]
        .getOffsetWhereCondition("offset", "<", OffsetValue.IntegralValue(1))

      assert(actual == "offset < 1")
    }

    "return the correct condition for datetime offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft]
        .getOffsetWhereCondition("offset", ">", OffsetValue.DateTimeValue(Instant.ofEpochMilli(1727761000)))

      assert(actual == "offset > '1970-01-21 01:56:01.000'")
    }

    "return the correct condition for string offsets" in {
      val actual = genDate.asInstanceOf[SqlGeneratorMicrosoft]
        .getOffsetWhereCondition("offset", ">=", OffsetValue.StringValue("AAA"))

      assert(actual == "offset >= 'AAA'")
    }
  }
}
