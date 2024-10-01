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

class SqlGeneratorDenodoSuite extends AnyWordSpec {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  val driver = "com.denodo.vdp.jdbc.Driver"

  val gen: SqlGenerator = getSqlGenerator(driver, sqlConfigDate)
  val genStr: SqlGenerator = getSqlGenerator(driver, sqlConfigString)
  val genNum: SqlGenerator = getSqlGenerator(driver, sqlConfigNumber)
  val genDateTime: SqlGenerator = getSqlGenerator(driver, sqlConfigDateTime)
  val genEscaped: SqlGenerator = getSqlGenerator(driver, sqlConfigEscape)
  val genEscapedAuto: SqlGenerator = getSqlGenerator(driver, sqlConfigEscape.copy(identifierQuotingPolicy = QuotingPolicy.Auto))

  "generate count queries without date ranges" in {
    assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
  }

  "generate data queries without date ranges" in {
    assert(gen.getDataQuery("A", Nil, None) == "SELECT * FROM A")
  }

  "generate data queries when list of columns is specified with escaping" in {
    assert(genEscaped.getDataQuery("A", columns, None) == "SELECT \"A\", \"D\", \"Column with spaces\" FROM \"A\"")
  }

  "generate data queries when list of columns is specified with auto escaping" in {
    assert(genEscapedAuto.getDataQuery("A", columns, None) == "SELECT A, D, \"Column with spaces\" FROM A")
  }

  "generate data queries with limit clause date ranges" in {
    assert(gen.getDataQuery("A", Nil, Some(100)) == "SELECT * FROM A")
  }

  "generate ranged count queries" when {
    "date is in DATE format" in {
      assert(gen.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE D = date'2020-08-17'")
      assert(gen.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getCountQuery("A", date1, date1) ==
        "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) = date'2020-08-17'")
      assert(genDateTime.getCountQuery("A", date1, date2) ==
        "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) >= date'2020-08-17' AND CAST(D AS DATE) <= date'2020-08-30'")
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
        "SELECT COUNT(*) FROM \"Input Table\" WHERE \"Info date\" = date'2020-08-17'")
      assert(genEscapedAuto.getCountQuery("Input Table", date1, date2) ==
        "SELECT COUNT(*) FROM \"Input Table\" WHERE \"Info date\" >= date'2020-08-17' AND \"Info date\" <= date'2020-08-30'")
    }
  }

  "generate ranged data queries" when {
    "date is in DATE format" in {
      assert(gen.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE D = date'2020-08-17'")
      assert(gen.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
    }

    "date is in DATETIME format" in {
      assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) = date'2020-08-17'")
      assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
        "SELECT * FROM A WHERE CAST(D AS DATE) >= date'2020-08-17' AND CAST(D AS DATE) <= date'2020-08-30'")
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
        "SELECT * FROM A WHERE D = date'2020-08-17'")
      assert(gen.getDataQuery("A", date1, date2, Nil, Some(100)) ==
        "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
    }
  }

  "getCountQueryForSql" should {
    "generate count queries for an SQL subquery" in {
      assert(gen.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) AS query")
    }
  }

  "getDtable" should {
    "return the original table when a table is provided" in {
      assert(gen.getDtable("A") == "A")
    }

    "wrapped query without alias for SQL queries " in {
      assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B) tbl")
    }
  }

  "quote" should {
    "quote each subfields separately" in {
      val actual = gen.quote("System User.\"Table Name\"")

      assert(actual == "\"System User\".\"Table Name\"")
    }
  }

  "unquote" should {
    "quote each subfields separately" in {
      val actual = gen.unquote("System User.\"Table Name\"")

      assert(actual == "System User.Table Name")
    }
  }

  "splitComplexIdentifier" should {
    "throw on an empty identifier" in {
      assertThrows[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier(" ")
      }
    }

    "keep original column name as is" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("System User")

      assert(actual == Seq("System User"))
    }

    "split a complex column" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("System User.\"Table Name\"")

      assert(actual == Seq("System User", "\"Table Name\""))
    }

    "handle escaped dots" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("\"System User.Table Name\"")

      assert(actual == Seq("\"System User.Table Name\""))
    }

    "handle first identifier escaped" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("\"System User\".Table Name")

      assert(actual == Seq("\"System User\"", "Table Name"))
    }

    "handle multiple level of identifiers" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("My Catalog.My Schema.\"My Table\".My column")

      assert(actual == Seq("My Catalog", "My Schema", "\"My Table\"", "My column"))
    }

    "handle escaped 2 quotes (maybe support this eventually)" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("\"System \"\"User\"\".Table Name\"")
      }

      assert(ex.getMessage == "Invalid character '\"' in the identifier '\"System \"\"User\"\".Table Name\"', position 8.")
    }

    "handle escaped 3 quotes (this is never supported)" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("\"System \"\"\"User\"\"\".Table Name\"")
      }

      assert(ex.getMessage == "Invalid character '\"' in the identifier '\"System \"\"\"User\"\"\".Table Name\"', position 8.")
    }

    "throw an exception if quotes are found inside an identifier" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("System Use\"r.T\"able Name")
      }

      assert(ex.getMessage.contains("Invalid character '\"' in the identifier 'System Use\"r.T\"able Name'"))
    }

    "handle multiple level of identifiers of multiple levels" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("My Catalog.My Schema.\"My Tab\"le.My column")
      }

      assert(ex.getMessage == "Invalid character '\"' in the identifier 'My Catalog.My Schema.\"My Tab\"le.My column', position 28.")
    }

    "throw on unmatched open bracket" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("System User.\"Table Name")
      }

      assert(ex.getMessage.contains("Found not matching '\"' in the identifier 'System User.\"Table Name'."))
    }

    "throw on unmatched closing bracket" in {
      val ex = intercept[IllegalArgumentException] {
        gen.asInstanceOf[SqlGeneratorBase].splitComplexIdentifier("System User.Table Name\"")
      }

      assert(ex.getMessage.contains("Found not matching '\"' in the identifier 'System User.Table Name\"'."))
    }
  }
  "getOffsetWhereCondition" should {
    "return the correct condition for integral offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", "<", OffsetValue.IntegralType(1))

      assert(actual == "offset < 1")
    }

    "return the correct condition for datetime offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">", OffsetValue.DateTimeType(Instant.ofEpochMilli(1727761000)))

      assert(actual == "offset > TIMESTAMP '1970-01-21 01:56:01.000'")
    }

    "return the correct condition for string offsets" in {
      val actual = gen.asInstanceOf[SqlGeneratorBase]
        .getOffsetWhereCondition("offset", ">=", OffsetValue.StringType("AAA"))

      assert(actual == "offset >= 'AAA'")
    }
  }
}
