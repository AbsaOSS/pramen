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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.rdb.{PramenDb, RdbJdbc}
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.{FsUtils, ResourceUtils, UsingUtils}

import java.time.LocalDate

class BulkLoadLongSuite extends AnyWordSpec
  with SparkTestBase
  with TempDirFixture
  with RelationalDbFixture
  with BeforeAndAfter
  with BeforeAndAfterAll
  with TextComparisonFixture {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  var pramenDb: PramenDb = _

  private val infoDate = LocalDate.of(2021, 2, 18)

  before {
    if (pramenDb != null) pramenDb.close()
    UsingUtils.using(RdbJdbc(jdbcConfig)) { rdb =>
      rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    }
    pramenDb = PramenDb(jdbcConfig)

    RdbExampleTable.IncrementalTable.initTable(getConnection)
  }

  override def afterAll(): Unit = {
    if (pramenDb != null) pramenDb.close()
    super.afterAll()
  }

  "Bulk load should" should {
    val csvData =
      """id,name,dt
        |1,John,2020-12-31
        |2,Jack,2021-01-01
        |3,Jill,2021-01-02
        |4,May,2021-01-03
        |5,Bob,2021-01-04
        |6,Anne,2021-01-05
        |7,Carl,2021-01-06
        |8,Dana,2021-01-07
        |9,Eve,2021-01-08
        |10,Frank,2021-01-09
        |11,Gina,2021-01-10
        |12,Hugo,2021-01-11
        |13,Bob,2021-01-12
        |14,Anne,2021-01-13
        |15,Carl,2021-01-14
        |16,Dana,2021-01-15
        |17,Eve,2021-01-16
        |18,Frank,2021-01-17
        |19,Gina,2021-01-18
        |20,Hugo,2021-01-19
        |21,Bob,2021-01-20
        |22,Anne,2021-01-21
        |23,Carl,2021-01-22
        |24,Dana,2021-01-23
        |25,Eve,2021-01-24
        |26,Frank,2021-01-25
        |27,Gina,2021-01-26
        |28,Hugo,2021-01-27
        |29,Bob,2021-01-28
        |30,Anne,2021-01-29
        |31,Carl,2021-01-30
        |32,Dana,2021-01-31
        |33,Eve,2021-02-01
        |34,Frank,2021-02-02
        |35,Gina,2021-02-03
        |36,Hugo,2021-02-04
        |37,Bob,2021-02-05
        |38,Anne,2021-02-06
        |39,Carl,2021-02-07
        |40,Dana,2021-02-08
        |41,Eve,2021-02-09
        |42,Frank,2021-02-10
        |43,Gina,2021-02-11
        |44,Hugo,2021-02-12
        |45,Bob,2021-02-13
        |46,Anne,2021-02-14
        |47,Carl,2021-02-15
        |48,Dana,2021-02-16
        |49,Eve,2021-02-17
        |50,Frank,2021-02-18
        |51,Gina,2021-02-19
        |52,Hugo,2021-02-20
        |53,Bob,2021-02-21
        |54,Anne,2021-02-22
        |55,Carl,2021-02-23
        |56,Dana,2021-02-24
        |57,Eve,2021-02-25
        |58,Frank,2021-02-26
        |59,Gina,2021-02-27
        |60,Hugo,2021-02-28
        |61,Bob,2021-03-01
        |62,Anne,2021-03-02
        |""".stripMargin

    "be able to access inner source configuration for strict months" in {
      withTempDirectory("integration_inner_source") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val landingPath = new Path(tempDir, "landing")

        fsUtils.writeFile(new Path(landingPath, "landing_file1.csv"), csvData)

        val conf = getConfig(tempDir)

        val exitCode = AppRunner.runBulkPipelines(conf)
        assert(exitCode == 0)

        val table2P = new Path(tempDir, "table2")

        val df = spark.read.parquet(table2P.toString)
        //df.show(1000, truncate = false)

        val table2Path1 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=2021-01-01")
        val table2Path2 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=2021-02-01")

        assert(fsUtils.exists(table2Path1))
        assert(fsUtils.exists(table2Path2))

        assert(df.count() == 59)

        // Running the job for the second time shouyld not change the output
        val exitCode2 = AppRunner.runBulkPipelines(conf)
        assert(exitCode2 == 0)

        val df2 = spark.read.parquet(table2P.toString)
        assert(df2.count() == 59)
      }
    }
  }

  def getConfig(basePath: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_bulk_load.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen {
         |  load.date.from = "2021-01-01"
         |  load.date.to = "2021-02-28"
         |  runtime.run.mode = bulk
         |
         |  bookkeeping.jdbc {
         |   driver = "$driver"
         |    url = "$url"
         |    user = "$user"
         |    password = "$password"
         |  }
         |}
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
