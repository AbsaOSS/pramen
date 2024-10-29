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

  "Generic SQL generator" should {
  }

}
