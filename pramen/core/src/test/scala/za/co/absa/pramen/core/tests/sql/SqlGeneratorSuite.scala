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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.mocks.DummySqlConfigFactory
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql.{SqlColumnType, SqlGeneratorGeneric, SqlGeneratorMicrosoft, SqlGeneratorOracle}

import java.time.LocalDate

class SqlGeneratorSuite extends AnyWordSpec with RelationalDbFixture {

  import za.co.absa.pramen.core.sql.SqlGenerator._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "DD")
  private val sqlConfigEscape = DummySqlConfigFactory.getDummyConfig(infoDateColumn = "Info date")
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "DD")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "DD")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "DD", dateFormatApp = "yyyyMMdd")
  private val columns = Seq("AA", "DD", "Column with spaces")

  private val date1 = LocalDate.of(2020, 8, 17)
  private val date2 = LocalDate.of(2020, 8, 30)

  private val config = ConfigFactory.empty()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "fromDriverName" should {
    "return an Oracle SQL generator" in {
      assert(fromDriverName("oracle.jdbc.OracleDriver", sqlConfigDate, config).isInstanceOf[SqlGeneratorOracle])
    }

    "return a Microsoft SQL generator" in {
      assert(fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigDate, config).isInstanceOf[SqlGeneratorMicrosoft])
      assert(fromDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver", sqlConfigDate, config).isInstanceOf[SqlGeneratorMicrosoft])
    }

    "return a generic SQL generator" in {
      assert(fromDriverName("unknown", sqlConfigDate, config).isInstanceOf[SqlGeneratorGeneric])
    }
  }

  "Oracle SQL generator" should {
    val gen = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigDate, config)
    val genStr = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigString, config)
    val genNum = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigNumber, config)
    val genDateTime = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigDateTime, config)
    val genEscaped = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("AA") == "SELECT COUNT(*) FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

   "generate data queries when list of columns is specified" in {
      assert(gen.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA WHERE ROWNUM <= 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
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
        assert(gen.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE TO_DATE(DD, 'YYYY-MM-DD') = date'2020-08-17'")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE TO_DATE(DD, 'YYYY-MM-DD') >= date'2020-08-17' AND TO_DATE(DD, 'YYYY-MM-DD') <= date'2020-08-30'")
      }

      "with limit records" in {
        assert(gen.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17' AND ROWNUM <= 100")
        assert(gen.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30' AND ROWNUM <= 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(gen.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B)")
      }
    }
  }

  "Microsoft SQL generator" should {
    val genDate = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigDate, config)
    val genDateTime = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigString, config)
    val genStr2 = fromDriverName("net.sourceforge.jtds.jdbc.Driver", DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, dateFormatApp = "yyyyMMdd",  infoDateColumn = "DD"), config)
    val genNum = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigNumber, config)
    val genEscaped = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigEscape, config)
    val genEscaped2 = fromDriverName("net.sourceforge.jtds.jdbc.Driver",  DummySqlConfigFactory.getDummyConfig(infoDateColumn = "[Info date]"), config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("AA") == "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK)")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, None) == "SELECT * FROM AA WITH (NOLOCK)")
    }

    "generate data queries when list of columns is specified" in {
      assert(genDate.getDataQuery("AA", columns, None) == "SELECT AA, DD, [Column with spaces] FROM AA WITH (NOLOCK)")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, Some(100)) == "SELECT TOP 100 * FROM AA WITH (NOLOCK)")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD >= CONVERT(DATE, '2020-08-17') AND DD <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) = CONVERT(DATE, '2020-08-17')")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, DD) <= CONVERT(DATE, '2020-08-30')")
    }

      "date is in STRING ISO format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) = CONVERT(DATE, '2020-08-17')")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, DD) <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in STRING non ISO format" in {
        assert(genStr2.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD = '20200817'")
        assert(genStr2.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD >= '20200817' AND DD <= '20200830'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WITH (NOLOCK) WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "the table name and column name need to be escaped" in {
        assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17')")
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17') AND [Info date] <= CONVERT(DATE, '2020-08-30')")
      }

      "the table name and column name already escaped" in {
        assert(genEscaped2.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] = CONVERT(DATE, '2020-08-17')")
        assert(genEscaped2.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM [Input Table] WITH (NOLOCK) WHERE [Info date] >= CONVERT(DATE, '2020-08-17') AND [Info date] <= CONVERT(DATE, '2020-08-30')")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD >= CONVERT(DATE, '2020-08-17') AND DD <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) = CONVERT(DATE, '2020-08-17')")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, DD) <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in STRING ISO format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) = CONVERT(DATE, '2020-08-17')")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE CONVERT(DATE, DD) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, DD) <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in STRING non-ISO format" in {
        assert(genStr2.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD = '20200817'")
        assert(genStr2.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD >= '20200817' AND DD <= '20200830'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WITH (NOLOCK) WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT TOP 100 * FROM AA WITH (NOLOCK) WHERE DD = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT TOP 100 * FROM AA WITH (NOLOCK) WHERE DD >= CONVERT(DATE, '2020-08-17') AND DD <= CONVERT(DATE, '2020-08-30')")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("AA") == "AA")
      }

      "wrapped query with alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS tbl")
      }
    }
  }

  "Denodo SQL generator" should {
    val gen = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigDate, config)
    val genStr = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigString, config)
    val genNum = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigNumber, config)
    val genDateTime = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigDateTime, config)
    val genEscaped = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("AA") == "SELECT COUNT(*) FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(gen.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
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
        assert(gen.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(gen.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B) tbl")
      }
    }
  }

  "SAS SQL generator" should {
    val connection = getConnection

    val gen = fromDriverName("com.sas.rio.MVADriver", sqlConfigDate, config)
    val genStr = fromDriverName("com.sas.rio.MVADriver", sqlConfigString, config)
    val genNum = fromDriverName("com.sas.rio.MVADriver", sqlConfigNumber, config)
    val genDateTime = fromDriverName("com.sas.rio.MVADriver", sqlConfigDateTime, config)
    val genEscaped = fromDriverName("com.sas.rio.MVADriver", sqlConfigEscape, config)

    gen.setConnection(connection)
    genStr.setConnection(connection)
    genNum.setConnection(connection)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("AA") == "SELECT COUNT(*) AS cnt 'cnt' FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("company", Nil, None) == "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company")
    }

    "generate data queries when list of columns is specified" in {
      assert(gen.getDataQuery("company", columns, None) == "SELECT AA, DD, 'Column with spaces'n FROM company")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("company", Nil, Some(100)) == "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD = date'2020-08-17'")
        assert(gen.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS cnt 'cnt' FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "date is in DATETIME format" in {
        assertThrows[UnsupportedOperationException] {
          genDateTime.getDataQuery("AA", date1, date1, Nil, None)
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
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD = date'2020-08-17'")
        assert(gen.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("company", date1, date1, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("company", date1, date1, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD = 20200817")
        assert(genNum.getDataQuery("company", date1, date2, Nil, None) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("company", date1, date1, Nil, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD = date'2020-08-17' LIMIT 100")
        assert(gen.getDataQuery("company", date1, date2, Nil, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', DESCRIPTION 'DESCRIPTION', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30' LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(gen.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B)")
      }
    }
  }

  "Hive SQL generator" should {
    val gen = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigDate, config)
    val genStr = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigString, config)
    val genNum = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigNumber, config)
    val genDateTime = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigDateTime, config)
    val genEscaped = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigEscape, config)
    val genEscaped2 = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", DummySqlConfigFactory.getDummyConfig(infoDateColumn = "`Info date`"), config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("AA") == "SELECT COUNT(*) FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(gen.getDataQuery("AA", columns, None) == "SELECT AA, DD, `Column with spaces` FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = to_date('2020-08-17')")
        assert(gen.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= to_date('2020-08-17') AND DD <= to_date('2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) = to_date('2020-08-17')")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) >= to_date('2020-08-17') AND CAST(DD AS DATE) <= to_date('2020-08-30')")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
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
        assert(gen.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = to_date('2020-08-17')")
        assert(gen.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= to_date('2020-08-17') AND DD <= to_date('2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = to_date('2020-08-17')")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= to_date('2020-08-17') AND CAST(DD AS DATE) <= to_date('2020-08-30')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = to_date('2020-08-17') LIMIT 100")
        assert(gen.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= to_date('2020-08-17') AND DD <= to_date('2020-08-30') LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(gen.getDtable("AA") == "(AA) tbl")
      }

      "wrapped query without alias for SQL queries " in {
        assert(gen.getDtable("SELECT A FROM B") == "(SELECT A FROM B) tbl")
      }
    }
  }

  "PostgreSQL SQL generator" should {
    val genDate = fromDriverName("org.postgresql.Driver", sqlConfigDate, config)
    val genDateTime = fromDriverName("org.postgresql.Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("org.postgresql.Driver", sqlConfigString, config)
    val genNum = fromDriverName("org.postgresql.Driver", sqlConfigNumber, config)
    val genEscaped = fromDriverName("org.postgresql.Driver", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("AA") == "SELECT COUNT(*) FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(genDate.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = date'2020-08-17'")
        assert(genDate.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
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
        assert(genDate.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17'")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30' LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) t")
      }
    }
  }

  "DB2 SQL generator" should {
    val genDate = fromDriverName("com.ibm.db2.jcc.DB2Driver", sqlConfigDate, config)
    val genDateTime = fromDriverName("com.ibm.db2.jcc.DB2Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("com.ibm.db2.jcc.DB2Driver", sqlConfigString, config)
    val genNum = fromDriverName("com.ibm.db2.jcc.DB2Driver", sqlConfigNumber, config)
    val genEscaped = fromDriverName("com.ibm.db2.jcc.DB2Driver", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("AA") == "SELECT COUNT(*) AS CNT FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(genDate.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = DATE '2020-08-17'")
        assert(genDate.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= DATE '2020-08-17' AND DD <= DATE '2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) = DATE '2020-08-17'")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) >= DATE '2020-08-17' AND CAST(DD AS DATE) <= DATE '2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "the table name and column name need to be escaped" in {
        assert(genEscaped.getCountQuery("Input Table", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM \"Input Table\" WHERE \"Info date\" = DATE '2020-08-17'")
        assert(genEscaped.getCountQuery("Input Table", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM \"Input Table\" WHERE \"Info date\" >= DATE '2020-08-17' AND \"Info date\" <= DATE '2020-08-30'")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = DATE '2020-08-17'")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= DATE '2020-08-17' AND DD <= DATE '2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = DATE '2020-08-17'")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= DATE '2020-08-17' AND CAST(DD AS DATE) <= DATE '2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = DATE '2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= DATE '2020-08-17' AND DD <= DATE '2020-08-30' LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS T")
      }
    }
  }

  "HSQL generator" should {
    val genDate = fromDriverName("org.hsqldb.jdbc.JDBCDriver", sqlConfigDate, config)
    val genDateTime = fromDriverName("org.hsqldb.jdbc.JDBCDriver", sqlConfigDateTime, config)
    val genStr = fromDriverName("org.hsqldb.jdbc.JDBCDriver", sqlConfigString, config)
    val genNum = fromDriverName("org.hsqldb.jdbc.JDBCDriver", sqlConfigNumber, config)
    val genEscaped = fromDriverName("org.hsqldb.jdbc.JDBCDriver", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("AA") == "SELECT COUNT(*) AS CNT FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(genDate.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND DD <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(DD AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "the table name and column name need to be escaped" in {
        assert(genEscaped.getCountQuery("SELECT", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM \"SELECT\" WHERE \"Info date\" = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genEscaped.getCountQuery("SELECT", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM \"SELECT\" WHERE \"Info date\" >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND \"Info date\" <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND DD <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(DD AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = TO_DATE('2020-08-17', 'YYYY-MM-DD') LIMIT 100")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND DD <= TO_DATE('2020-08-30', 'YYYY-MM-DD') LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) t")
      }
    }

    "escapeIdentifier" should {
      "throw an exception if a column contains a single quote" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC ' DEF")
        }
      }

      "throw an exception if a column contains a double quote" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC \" DEF")
        }
      }

      "throw an exception if a column contains a semicolon" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC ; DEF")
        }
      }
    }
  }

  "Generic SQL generator" should {
    val genDate = fromDriverName("generic", sqlConfigDate, config)
    val genDateTime = fromDriverName("generic", sqlConfigDateTime, config)
    val genStr = fromDriverName("generic", sqlConfigString, config)
    val genNum = fromDriverName("generic", sqlConfigNumber, config)
    val genEscaped = fromDriverName("generic", sqlConfigEscape, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("AA") == "SELECT COUNT(*) AS CNT FROM AA")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, None) == "SELECT * FROM AA")
    }

    "generate data queries when list of columns is specified" in {
      assert(genDate.getDataQuery("AA", columns, None) == "SELECT AA, DD, \"Column with spaces\" FROM AA")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("AA", Nil, Some(100)) == "SELECT * FROM AA LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = date'2020-08-17'")
        assert(genDate.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("AA", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD = 20200817")
        assert(genNum.getCountQuery("AA", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
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
        assert(genDate.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17'")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30'")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) = date'2020-08-17'")
        assert(genDateTime.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE CAST(DD AS DATE) >= date'2020-08-17' AND CAST(DD AS DATE) <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = '2020-08-17'")
        assert(genStr.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= '2020-08-17' AND DD <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("AA", date1, date1, Nil, None) ==
          "SELECT * FROM AA WHERE DD = 20200817")
        assert(genNum.getDataQuery("AA", date1, date2, Nil, None) ==
          "SELECT * FROM AA WHERE DD >= 20200817 AND DD <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("AA", date1, date1, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD = date'2020-08-17' LIMIT 100")
        assert(genDate.getDataQuery("AA", date1, date2, Nil, Some(100)) ==
          "SELECT * FROM AA WHERE DD >= date'2020-08-17' AND DD <= date'2020-08-30' LIMIT 100")
      }
    }

    "getDtable" should {
      "return the original table when a table is provided" in {
        assert(genDate.getDtable("AA") == "AA")
      }

      "wrapped query without alias for SQL queries " in {
        assert(genDate.getDtable("SELECT A FROM B") == "(SELECT A FROM B) AS t")
      }
    }

    "escapeIdentifier" should {
      "throw an exception if a column contains a single quote" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC ' DEF")
        }
      }

      "throw an exception if a column contains a double quote" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC \" DEF")
        }
      }

      "throw an exception if a column contains a semicolon" in {
        assertThrows[IllegalArgumentException] {
          genDate.escapeIdentifier("ABC ; DEF")
        }
      }
    }
  }

}
