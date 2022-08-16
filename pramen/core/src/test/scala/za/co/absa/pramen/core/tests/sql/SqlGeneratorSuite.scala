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
import org.scalatest.WordSpec
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.mocks.DummySqlConfigFactory
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.sql.{SqlColumnType, SqlGeneratorGeneric, SqlGeneratorMicrosoft, SqlGeneratorOracle}

import java.time.LocalDate

class SqlGeneratorSuite extends WordSpec with RelationalDbFixture {

  import za.co.absa.pramen.core.sql.SqlGenerator._

  private val sqlConfigDate = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE, infoDateColumn = "D")
  private val sqlConfigDateTime = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATETIME, infoDateColumn = "D")
  private val sqlConfigString = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.STRING, infoDateColumn = "D")
  private val sqlConfigNumber = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.NUMBER, infoDateColumn = "D", dateFormatApp = "yyyyMMdd")

  private val sqlConfigWithListOfColumns = DummySqlConfigFactory.getDummyConfig(infoDateType = SqlColumnType.DATE,
    columns = Seq("A", "D", "CAST(E, DATE) AS E"),
    infoDateColumn = "D")

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
    val genCol = fromDriverName("oracle.jdbc.OracleDriver", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("A") == "SELECT * FROM A")
    }

   "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("A", Some(100)) == "SELECT * FROM A WHERE ROWNUM <= 100")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(gen.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = date'2020-08-17'")
        assert(gen.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT * FROM A WHERE D = date'2020-08-17' AND ROWNUM <= 100")
        assert(gen.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' AND ROWNUM <= 100")
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

  "Microsoft SQL generator" should {
    val genDate = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigDate, config)
    val genDateTime = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigString, config)
    val genNum = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigNumber, config)
    val genCol = fromDriverName("net.sourceforge.jtds.jdbc.Driver", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK)")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("A") == "SELECT * FROM A WITH (NOLOCK)")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A WITH (NOLOCK)")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("A", Some(100)) == "SELECT TOP 100 * FROM A WITH (NOLOCK)")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17') AND D <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D) = CONVERT(DATE, '2020-08-17')")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, D) <= CONVERT(DATE, '2020-08-30')")
    }

      "date is in STRING format" in {
        assert(genStr.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = '2020-08-17'")
        assert(genStr.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D = 20200817")
        assert(genNum.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) AS CNT FROM A WITH (NOLOCK) WHERE D >= 20200817 AND D <= 20200830")
      }
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17') AND D <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D) = CONVERT(DATE, '2020-08-17')")
        assert(genDateTime.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE CONVERT(DATE, D) >= CONVERT(DATE, '2020-08-17') AND CONVERT(DATE, D) <= CONVERT(DATE, '2020-08-30')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WITH (NOLOCK) WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT TOP 100 * FROM A WITH (NOLOCK) WHERE D = CONVERT(DATE, '2020-08-17')")
        assert(genDate.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT TOP 100 * FROM A WITH (NOLOCK) WHERE D >= CONVERT(DATE, '2020-08-17') AND D <= CONVERT(DATE, '2020-08-30')")
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
  }

  "Denodo SQL generator" should {
    val gen = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigDate, config)
    val genStr = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigString, config)
    val genNum = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigNumber, config)
    val genCol = fromDriverName("com.denodo.vdp.jdbc.Driver", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("A") == "SELECT * FROM A")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("A", Some(100)) == "SELECT * FROM A")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(gen.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = date'2020-08-17'")
        assert(gen.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT * FROM A WHERE D = date'2020-08-17'")
        assert(gen.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT * FROM A WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
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
  }

  "SAS SQL generator" should {
    val connection = getConnection

    val gen = fromDriverName("com.sas.rio.MVADriver", sqlConfigDate, config)
    val genStr = fromDriverName("com.sas.rio.MVADriver", sqlConfigString, config)
    val genNum = fromDriverName("com.sas.rio.MVADriver", sqlConfigNumber, config)
    val genCol = fromDriverName("com.sas.rio.MVADriver", sqlConfigWithListOfColumns, config)

    gen.setConnection(connection)
    genStr.setConnection(connection)
    genNum.setConnection(connection)
    genCol.setConnection(connection)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) AS cnt 'cnt' FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("company") == "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("company") == "SELECT A, D, CAST(E, DATE) AS E FROM company")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("company", Some(100)) == "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company LIMIT 100")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(gen.getDataQuery("company", date1, date1, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = date'2020-08-17'")
        assert(gen.getDataQuery("company", date1, date2, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= date'2020-08-17' AND D <= date'2020-08-30'")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("company", date1, date1, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("company", date1, date2, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("company", date1, date1, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = 20200817")
        assert(genNum.getDataQuery("company", date1, date2, None) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("company", date1, date1, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D = date'2020-08-17' LIMIT 100")
        assert(gen.getDataQuery("company", date1, date2, Some(100)) ==
          "SELECT ID 'ID', NAME 'NAME', EMAIL 'EMAIL', FOUNDED 'FOUNDED', LAST_UPDATED 'LAST_UPDATED', INFO_DATE 'INFO_DATE' FROM company WHERE D >= date'2020-08-17' AND D <= date'2020-08-30' LIMIT 100")
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
    val gen = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigDate, config)
    val genStr = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigString, config)
    val genNum = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigNumber, config)
    val genCol = fromDriverName("com.cloudera.hive.jdbc41.HS2Driver", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(gen.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(gen.getDataQuery("A") == "SELECT * FROM A")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(gen.getDataQuery("A", Some(100)) == "SELECT * FROM A LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(gen.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE D = to_date('2020-08-17')")
        assert(gen.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30')")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(gen.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = to_date('2020-08-17')")
        assert(gen.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(gen.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT * FROM A WHERE D = to_date('2020-08-17') LIMIT 100")
        assert(gen.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT * FROM A WHERE D >= to_date('2020-08-17') AND D <= to_date('2020-08-30') LIMIT 100")
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
  }

  "PostgreSQL SQL generator" should {
    val genDate = fromDriverName("org.postgresql.Driver", sqlConfigDate, config)
    val genDateTime = fromDriverName("org.postgresql.Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("org.postgresql.Driver", sqlConfigString, config)
    val genNum = fromDriverName("org.postgresql.Driver", sqlConfigNumber, config)
    val genCol = fromDriverName("org.postgresql.Driver", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("A") == "SELECT * FROM A")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("A", Some(100)) == "SELECT * FROM A LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD') LIMIT 100")
        assert(genDate.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD') LIMIT 100")
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
  }

  "Generic SQL generator" should {
    val genDate = fromDriverName("generic", sqlConfigDate, config)
    val genDateTime = fromDriverName("org.postgresql.Driver", sqlConfigDateTime, config)
    val genStr = fromDriverName("generic", sqlConfigString, config)
    val genNum = fromDriverName("generic", sqlConfigNumber, config)
    val genCol = fromDriverName("generic", sqlConfigWithListOfColumns, config)

    "generate count queries without date ranges" in {
      assert(genDate.getCountQuery("A") == "SELECT COUNT(*) FROM A")
    }

    "generate data queries without date ranges" in {
      assert(genDate.getDataQuery("A") == "SELECT * FROM A")
    }

    "generate data queries when list of columns is specified" in {
      assert(genCol.getDataQuery("A") == "SELECT A, D, CAST(E, DATE) AS E FROM A")
    }

    "generate data queries with limit clause date ranges" in {
      assert(genDate.getDataQuery("A", Some(100)) == "SELECT * FROM A LIMIT 100")
    }

    "generate ranged count queries" when {
      "date is in DATE format" in {
        assert(genDate.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getCountQuery("A", date1, date1) ==
          "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getCountQuery("A", date1, date2) ==
          "SELECT COUNT(*) FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
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
    }

    "generate ranged data queries" when {
      "date is in DATE format" in {
        assert(genDate.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDate.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in DATETIME format" in {
        assert(genDateTime.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE CAST(D AS DATE) = TO_DATE('2020-08-17', 'YYYY-MM-DD')")
        assert(genDateTime.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE CAST(D AS DATE) >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND CAST(D AS DATE) <= TO_DATE('2020-08-30', 'YYYY-MM-DD')")
      }

      "date is in STRING format" in {
        assert(genStr.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = '2020-08-17'")
        assert(genStr.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= '2020-08-17' AND D <= '2020-08-30'")
      }

      "date is in NUMBER format" in {
        assert(genNum.getDataQuery("A", date1, date1, None) ==
          "SELECT * FROM A WHERE D = 20200817")
        assert(genNum.getDataQuery("A", date1, date2, None) ==
          "SELECT * FROM A WHERE D >= 20200817 AND D <= 20200830")
      }

      "with limit records" in {
        assert(genDate.getDataQuery("A", date1, date1, Some(100)) ==
          "SELECT * FROM A WHERE D = TO_DATE('2020-08-17', 'YYYY-MM-DD') LIMIT 100")
        assert(genDate.getDataQuery("A", date1, date2, Some(100)) ==
          "SELECT * FROM A WHERE D >= TO_DATE('2020-08-17', 'YYYY-MM-DD') AND D <= TO_DATE('2020-08-30', 'YYYY-MM-DD') LIMIT 100")
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
  }

}
