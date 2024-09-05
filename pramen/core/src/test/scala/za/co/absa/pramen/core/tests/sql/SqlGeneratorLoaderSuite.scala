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
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType, SqlGeneratorBase}
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.mocks.{DummySqlConfigFactory, SqlGeneratorDummy}
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql._

import java.time.LocalDate

class SqlGeneratorLoaderSuite extends AnyWordSpec with RelationalDbFixture {

  import za.co.absa.pramen.core.sql.SqlGeneratorLoader._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date", identifierQuotingPolicy = QuotingPolicy.Always)
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("A", "D", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "loadSqlGenerator" should {
    "return an Oracle SQL generator" in {
      assert(getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigDate).isInstanceOf[SqlGeneratorOracle])
    }

    "return a Microsoft SQL generator" in {
      assert(getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigDate).isInstanceOf[SqlGeneratorMicrosoft])
      assert(getSqlGenerator("com.microsoft.sqlserver.jdbc.SQLServerDriver", sqlConfigDate).isInstanceOf[SqlGeneratorMicrosoft])
    }

    "return a generic SQL generator" in {
      assert(getSqlGenerator("unknown", sqlConfigDate).isInstanceOf[SqlGeneratorGeneric])
    }

    "return from a class" in {
      val sqlConfig = sqlConfigDate.copy(sqlGeneratorClass = Some("za.co.absa.pramen.core.mocks.SqlGeneratorDummy"))
      val generator = getSqlGenerator("unknown", sqlConfig)

      assert(generator.isInstanceOf[SqlGeneratorDummy])
      assert(generator.asInstanceOf[SqlGeneratorDummy].getSqlConfig == sqlConfig)
    }
  }

  "Oracle SQL generator" should {
    val gen = getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigDate)
    val genStr = getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigString)
    val genNum = getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigNumber)
    val genDateTime = getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigDateTime)
    val genEscaped = getSqlGenerator("oracle.jdbc.OracleDriver", sqlConfigEscape)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("A", Nil, None) == "SELECT * FROM A")
    }

   "generate data queries when list of columns is specified" in {
      assert(gen.getDataQuery("A", columns, None) == "SELECT A, D, \"Column with spaces\" FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("A", Nil, Some(100)) == "SELECT * FROM A WHERE ROWNUM <= 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE D = date'2020-08-17'")
        assert(gen.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
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
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
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

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
          "SELECT * FROM A WHERE TO_DATE(D, 'YYYY-MM-DD') = date'2020-08-17'")
        assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
          "SELECT * FROM A WHERE TO_DATE(D, 'YYYY-MM-DD') >= date'2020-08-17' AND TO_DATE(D, 'YYYY-MM-DD') <= date'2020-08-30'")
      }

      "with limit records" in {
        assert(gen.getDataQuery("A", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D = date'2020-08-17' AND ROWNUM <= 100")
        assert(gen.getDataQuery("A", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' AND ROWNUM <= 100")
      }
    }

    "getCountQueryForSql" should {
      "generate count queries for an SQL subquery" in {
        assert(gen.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) query")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(gen.getDtable("A") == "A")
      }

      "wrapped query without alias for SQL queries " in {
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B)")
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
  }

  "Microsoft SQL generator" should {
    val genDate = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigDate)
    val genDateTime = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigDateTime)
    val genStr = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigString)
    val genStr2 = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, dateFormatApp = "yyyyMMdd",  infoDateColumn = "D"))
    val genNum = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigNumber)
    val genEscaped = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver", sqlConfigEscape)
    val genEscaped2 = getSqlGenerator("net.sourceforge.jtds.jdbc.Driver",  DummySqlConfigFactory.getDummyConfig(infoDateColumn = "[Info date]", identifierQuotingPolicy = QuotingPolicy.Auto))

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK)")
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
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17', 23)")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17', 23) AND D <= CONVERT(DATE, '2020-08-30', 23)")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) AND CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
    }

      "date is in STRING ISO format" in {
        assert(genStr.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) = CONVERT(DATE, '2020-08-17', 23)")
        assert(genStr.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE TRY_CONVERT(DATE, D, 23) >= CONVERT(DATE, '2020-08-17', 23) " +
            "AND TRY_CONVERT(DATE, D, 23) <= CONVERT(DATE, '2020-08-30', 23)")
      }

      "date is in STRING non ISO format" in {
        assert(genStr2.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = '20200817'")
        assert(genStr2.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= '20200817' AND D <= '20200830'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = 20200817")
        assert(genNum.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= 20200817 AND D <= 20200830")
      }

      "the table name and column name need to be escaped" in {
        assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS [CNT] FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17', 23)")
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS [CNT] FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17', 23) AND [Info date] <= CONVERT(DATE, '2020-08-30', 23)")
      }

      "the table name and column name already escaped" in {
        assert(genEscaped2.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17', 23)")
        assert(genEscaped2.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17', 23) AND [Info date] <= CONVERT(DATE, '2020-08-30', 23)")
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
        assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) AS query")
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
  }

  "Denodo SQL generator" should {
    val gen = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigDate)
    val genStr = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigString)
    val genNum = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigNumber)
    val genDateTime = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigDateTime)
    val genEscaped = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigEscape)
    val genEscapedAuto = getSqlGenerator("com.denodo.vdp.jdbc.Driver", sqlConfigEscape.copy(identifierQuotingPolicy = QuotingPolicy.Auto))

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
  }

  "SAS SQL generator" should {
    val connection = getConnection

    val gen = getSqlGenerator("com.sas.rio.MVADriver", sqlConfigDate)
    val genStr = getSqlGenerator("com.sas.rio.MVADriver", sqlConfigString)
    val genNum = getSqlGenerator("com.sas.rio.MVADriver", sqlConfigNumber)
    val genDateTime = getSqlGenerator("com.sas.rio.MVADriver", sqlConfigDateTime)
    val genEscaped = getSqlGenerator("com.sas.rio.MVADriver", sqlConfigEscape)

    gen.setConnection(connection)
    genStr.setConnection(connection)
    genNum.setConnection(connection)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) AS cnt 'cnt' FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("company", Nil, None) == "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company")
    }

    "generate data queries when list of columns is specified" in {
      assert(genEscaped.getDataQuery("company", columns, None) == "SELECT 'A'n, 'D'n, 'Column with spaces'n FROM 'company'n")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("company", Nil, Some(100)) == "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D = date'2020-08-17'")
        assert(gen.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D = '2020-08-17'")
        assert(genStr.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D = 20200817")
        assert(genNum.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "date is in DATETIME format" in {
        assertThrows[UnsupportedOperationException] {
          genDateTime.getDataQuery("A", date1, date1, Nil, None)
        }
      }

      "the table name and column name need to be escaped" in {
        assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM 'Input Table'n WHERE 'Info date'n = date'2020-08-17'")
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM 'Input Table'n WHERE 'Info date'n >= date'2020-08-17' AND 'Info date'n <= date'2020-08-30'")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(gen.getDataQuery("company", date1, date1, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = date'2020-08-17'")
        assert(gen.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("company", date1, date1, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("company", date1, date1, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = 20200817")
        assert(genNum.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("company", date1, date1, Nil, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = date'2020-08-17' LIMIT 100")
        assert(gen.getDataQuery("company", date1, date2, Nil, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' LIMIT 100")
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
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B)")
      }
    }
  }

  "Hive SQL generator" should {
    val gen = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigDate)
    val genStr = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigString)
    val genNum = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigNumber)
    val genDateTime = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigDateTime)
    val genEscaped = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigEscape)
    val genEscaped2 = getSqlGenerator("com.cloudera.hive.jdbc41.HS2Driver", DummySqlConfigFactory.getDummyConfig(infoDateColumn = "`Info date`", identifierQuotingPolicy = QuotingPolicy.Auto))

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
  }

  "PostgreSQL SQL generator" should {
    val genDate = getSqlGenerator("org.postgresql.Driver", sqlConfigDate)
    val genDateTime = getSqlGenerator("org.postgresql.Driver", sqlConfigDateTime)
    val genStr = getSqlGenerator("org.postgresql.Driver", sqlConfigString)
    val genNum = getSqlGenerator("org.postgresql.Driver", sqlConfigNumber)
    val genEscaped = getSqlGenerator("org.postgresql.Driver", sqlConfigEscape)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) FROM A")
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
          "SELECT COUNT(*) FROM A WHERE D = date'2020-08-17'")
        assert(genDate.getCountQuery("A", date1, date2) ==
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
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) FROM \"Input Table\" WHERE \"Info date\" >= date'2020-08-17' AND \"Info date\" <= date'2020-08-30'")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
          "SELECT * FROM A WHERE D = date'2020-08-17'")
        assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
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
        assert(genDate.getDataQuery("A", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D = date'2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' LIMIT 100")
      }
    }

    "getCountQueryForSql" should {
      "generate count queries for an SQL subquery" in {
        assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) query")
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
  }

  "MySQL SQL generator" should {
    val genDate = getSqlGenerator("com.mysql.cj.jdbc.Driver", sqlConfigDate)
    val genDateTime = getSqlGenerator("com.mysql.cj.jdbc.Driver", sqlConfigDateTime)
    val genStr = getSqlGenerator("com.mysql.cj.jdbc.Driver", sqlConfigString)
    val genNum = getSqlGenerator("com.mysql.cj.jdbc.Driver", sqlConfigNumber)
    val genEscaped = getSqlGenerator("com.mysql.cj.jdbc.Driver", sqlConfigEscape)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("A", Nil, None) == "SELECT * FROM A")
    }

    "generate data queries when list of columns is specified" in {
      assert(genEscaped.getDataQuery("A", columns, None) == "SELECT `A`, `D`, `Column with spaces` FROM `A`")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("A", Nil, Some(100)) == "SELECT * FROM A LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE D = '2020-08-17'")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE DATE(D) = '2020-08-17'")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE DATE(D) >= '2020-08-17' AND DATE(D) <= '2020-08-30'")
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
          "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` = '2020-08-17'")
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) FROM `Input Table` WHERE `Info date` >= '2020-08-17' AND `Info date` <= '2020-08-30'")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("A", date1, date1, Nil, None) ==
          "SELECT * FROM A WHERE DATE(D) = '2020-08-17'")
        assert(genDateTime.getDataQuery("A", date1, date2, Nil, None) ==
          "SELECT * FROM A WHERE DATE(D) >= '2020-08-17' AND DATE(D) <= '2020-08-30'")
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
          "SELECT * FROM A WHERE D = '2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30' LIMIT 100")
      }
    }

    "getCountQueryForSql" should {
      "generate count queries for an SQL subquery" in {
        assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) query")
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
      "quote each subfields separately" in {
        val actual = genDate.quote("System User.`Table Name`")

        assert(actual == "`System User`.`Table Name`")
      }
    }

    "unquote" should {
      "quote each subfields separately" in {
        val actual = genDate.unquote("System User.`Table Name`")

        assert(actual == "System User.Table Name")
      }
    }
  }

  "DB2 SQL generator" should {
    val genDate = getSqlGenerator("com.ibm.db2.jcc.DB2Driver", sqlConfigDate)
    val genDateTime = getSqlGenerator("com.ibm.db2.jcc.DB2Driver", sqlConfigDateTime)
    val genStr = getSqlGenerator("com.ibm.db2.jcc.DB2Driver", sqlConfigString)
    val genNum = getSqlGenerator("com.ibm.db2.jcc.DB2Driver", sqlConfigNumber)
    val genEscaped = getSqlGenerator("com.ibm.db2.jcc.DB2Driver", sqlConfigEscape)

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
  }

  "HSQL generator" should {
    val genDate = getSqlGenerator("org.hsqldb.jdbc.JDBCDriver", sqlConfigDate)
    val genDateTime = getSqlGenerator("org.hsqldb.jdbc.JDBCDriver", sqlConfigDateTime)
    val genStr = getSqlGenerator("org.hsqldb.jdbc.JDBCDriver", sqlConfigString)
    val genNum = getSqlGenerator("org.hsqldb.jdbc.JDBCDriver", sqlConfigNumber)
    val genEscaped = getSqlGenerator("org.hsqldb.jdbc.JDBCDriver", sqlConfigEscape)

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

  "Generic SQL generator" should {
    val genDate = getSqlGenerator("generic", sqlConfigDate)
    val genDateTime = getSqlGenerator("generic", sqlConfigDateTime)
    val genStr = getSqlGenerator("generic", sqlConfigString)
    val genNum = getSqlGenerator("generic", sqlConfigNumber)
    val genEscaped = getSqlGenerator("generic", sqlConfigEscape)

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
          "SELECT COUNT(*) AS CNT FROM A WHERE D = date'2020-08-17'")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) = date'2020-08-17'")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WHERE CAST(D AS DATE) >= date'2020-08-17' AND CAST(D AS DATE) <= date'2020-08-30'")
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
          "SELECT COUNT(*) AS CNT FROM \"SELECT\" WHERE \"Info date\" = date'2020-08-17'")
        assert(genEscaped.getCountQuery("SELECT", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM \"SELECT\" WHERE \"Info date\" >= date'2020-08-17' AND \"Info date\" <= date'2020-08-30'")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, Nil, None) ==
          "SELECT * FROM A WHERE D = date'2020-08-17'")
        assert(genDate.getDataQuery("A", date1, date2, Nil, None) ==
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
        assert(genDate.getDataQuery("A", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D = date'2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("A", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' LIMIT 100")
      }
    }

    "getCountQueryForSql" should {
      "generate count queries for an SQL subquery" in {
        assert(genDate.getCountQueryForSql("SELECT A FROM B") == "SELECT COUNT(*) FROM (SELECT A FROM B) AS query")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("A") == "A")
      }

      "wrapped query without alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS t")
      }
    }

    "quote" should {
      "quote each subfields separately" in {
        val actual = genDate.quote("System User.\"Table Name\"")

        assert(actual == "\"System User\".\"Table Name\"")
      }

      "escape should not escape if turned off by config" in {
        val sql = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D", identifierQuotingPolicy = QuotingPolicy.Never)
        val genDate = getSqlGenerator("generic", sql)

        val actual = genDate.asInstanceOf[SqlGeneratorBase].escape("System User.\"Table Name\"")

        assert(actual == "System User.\"Table Name\"")
      }

      "throw an exception if a column contains a single quote" in {
        assertThrows[IllegalArgumentException] {
          genDate.quote("ABC ' DEF")
        }
      }

      "throw an exception if a column contains a semicolon" in {
        val ex = intercept[IllegalArgumentException] {
          genDate.quote("ABC ; DEF")
        }

        assert(ex.getMessage == "The character ';' (0x3B) cannot be used as part of column name in 'ABC ; DEF'.")
      }

      "throw an exception if a column contains a back slash" in {
        assertThrows[IllegalArgumentException] {
          genDate.quote("ABC \\n DEF")
        }
      }

      "throw an exception if a column contains a new line" in {
        assertThrows[IllegalArgumentException] {
          genDate.quote("ABC \n DEF")
        }
      }
    }

    "unquote" should {
      "quote each subfields separately" in {
        val actual = genDate.unquote("System User.\"Table Name\"")

        assert(actual == "System User.Table Name")
      }
    }
  }

}
