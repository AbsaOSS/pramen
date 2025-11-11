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

package za.co.absa.pramen.core.integration

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.JdbcUrlSelectorImpl
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.{JdbcNativeUtils, ResourceUtils}

import java.sql.Date
import java.time.LocalDate

class IncrementalPipelineJdbcLongSuite extends AnyWordSpec
  with SparkTestBase
  with RelationalDbFixture
  with BeforeAndAfter
  with BeforeAndAfterAll
  with TempDirFixture
  with TextComparisonFixture {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  lazy val pramenDb: PramenDb = PramenDb(jdbcConfig)

  private val infoDate = LocalDate.of(2021, 2, 18)

  private val BATCH_ID_COLUMN = "pramen_batchid"
  private val INFO_DATE_COLUMN = "pramen_info_date"

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
    RdbExampleTable.IncrementalTable.initTable(getConnection)
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  "end to end normal mode" in {
    val expectedOffsetOnly1: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |""".stripMargin

    val expectedOffsetOnlyAll: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    withTempDirectory("incremental1") { tempDir =>
      val conf = getConfig(tempDir)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val dfTable1Before = spark.read.format("delta").load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)

      val statement = getConnection.createStatement
      RdbExampleTable.IncrementalTable.inserts2.foreach(sql => statement.executeUpdate(sql))
      getConnection.commit()

      val exitCode2 = AppRunner.runPipeline(conf)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format("delta").load(table1Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedOffsetOnlyAll)
    }
    succeed
  }

  def getConfig(basePath: String,
                isRerun: Boolean = false,
                isHistoricalRun: Boolean = false,
                historyRunMode: String = "force",
                hasInfoDate: Boolean = false,
                useInfoDate: LocalDate = infoDate,
                resource: String = "/test/config/incremental_pipeline_jdbc.conf"): Config = {
    val configContents = ResourceUtils.getResourceString(resource)
    val basePathEscaped = basePath.replace("\\", "\\\\")
    val historicalConfigStr = if (isHistoricalRun) {
      s"""pramen.load.date.from = "${useInfoDate.minusDays(1)}"
         |pramen.load.date.to = "$useInfoDate"
         |pramen.runtime.run.mode = "$historyRunMode"
         |""".stripMargin
    } else {
      ""
    }

    val conf = ConfigFactory.parseString(
        s"""base.path = "$basePathEscaped"
           |pramen.runtime.is.rerun = $isRerun
           |pramen.current.date = "$useInfoDate"
           |$historicalConfigStr
           |has.information.date.column = $hasInfoDate
           |
           |pramen.bookkeeping.jdbc {
           |  driver = "$driver"
           |  url = "$url"
           |  user = "$user"
           |  password = "$password"
           |}
           |$configContents
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

  private def debugSql(sql: String): Unit = {
    JdbcNativeUtils.withResultSet(new JdbcUrlSelectorImpl(jdbcConfig), sql, 1) { rs =>
      val mt = rs.getMetaData

      for (i <- 1 to mt.getColumnCount) {
        print(mt.getColumnName(i) + "\t")
      }
      println("")

      while (rs.next()) {
        for (i <- 1 to mt.getColumnCount) {
          print(rs.getString(i) + "\t")
        }
        println("")
      }
    }
  }
}
