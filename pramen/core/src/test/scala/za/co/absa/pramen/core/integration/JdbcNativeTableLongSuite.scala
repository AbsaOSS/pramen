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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate

class JdbcNativeTableLongSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture with TextComparisonFixture with RelationalDbFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }


  "JDBC native pipeline" should {
    "support input.table setting" in {
      withTempDirectory("postgre_inf") { tempDir =>
        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")

        val conf = getConfig(tempDir)
        val exitCode = AppRunner.runPipeline(conf)
        assert(exitCode == 0)

        val resultDf = spark.read.parquet(table1Path.toString)

        assert(resultDf.count() == 4)
      }
    }
  }

  def getConfig(basePath: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_ingestion_native.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |jdbc.url="$url"
         |jdbc.user="$user"
         |jdbc.password="$password"
         |
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
