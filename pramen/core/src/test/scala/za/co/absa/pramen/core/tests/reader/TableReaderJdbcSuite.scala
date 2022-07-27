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

package za.co.absa.pramen.core.tests.reader

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.reader.TableReaderJdbc
import za.co.absa.pramen.core.sql.SqlGeneratorOracle

class TableReaderJdbcSuite extends WordSpec with SparkTestBase {
  "TableReaderJdbc" should {
    val conf = ConfigFactory.parseString(
      """reader {
        |  jdbc {
        |    driver = "oracle.jdbc.OracleDriver"
        |    connection.string = "url"
        |    user = "user"
        |    password = "password"
        |  }
        |
        |  has.information.date.column = true
        |
        |  information.date.column = "INFO_DATE"
        |  information.date.type = "number"
        |  information.date.app.format = "yyyy-MM-DD"
        |  information.date.sql.format = "YYYY-mm-DD"
        |
        |}""".stripMargin)

    "be able to be constructed properly from config" in {
      val reader = TableReaderJdbc("table", Seq.empty, conf.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.jdbcConfig.database.isEmpty)
      assert(jdbc.jdbcConfig.driver == "oracle.jdbc.OracleDriver")
      assert(jdbc.jdbcConfig.primaryUrl.get == "url")
      assert(jdbc.jdbcConfig.user == "user")
      assert(jdbc.jdbcConfig.password == "password")
      assert(jdbc.infoDateColumn == "INFO_DATE")
      assert(jdbc.infoDateType == "number")
      assert(jdbc.infoDateFormatApp == "yyyy-MM-DD")
      assert(jdbc.infoDateFormatSql == "YYYY-mm-DD")
      assert(jdbc.hasInfoDate)
      assert(!jdbc.saveTimestampsAsDates)
    }

    "ensure sql query generator is properly selected" in {
      val reader = TableReaderJdbc("table", Seq.empty, conf.getConfig("reader"), "reader")

      assert(reader.sqlGen.isInstanceOf[SqlGeneratorOracle])
    }

    "ensure jdbc config properties are passed correctly" in {
      val textConfig = conf
        .withValue("reader.save.timestamps.as.dates", ConfigValueFactory.fromAnyRef(true))
        .withValue("reader.correct.decimals.in.schema", ConfigValueFactory.fromAnyRef(true))
      val reader = TableReaderJdbc("table",
        Seq.empty,
        textConfig.getConfig("reader"), "reader")

      val jdbc = reader.getJdbcConfig

      assert(jdbc.saveTimestampsAsDates)
      assert(jdbc.correctDecimalsInSchema)
    }

  }

}
