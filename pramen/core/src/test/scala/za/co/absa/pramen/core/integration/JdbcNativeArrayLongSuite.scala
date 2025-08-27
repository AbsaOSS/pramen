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
import za.co.absa.pramen.core.utils.{ResourceUtils, SparkUtils}

import java.time.LocalDate

class JdbcNativeArrayLongSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture with TextComparisonFixture with RelationalDbFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Arrays.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Arrays.dropTable(getConnection)
    super.afterAll()
  }


  "JDBC native pipeline" should {
    "support input.table setting" in {
      val expectedData =
        """[ {
          |  "ID" : 0,
          |  "STR_ARRAY" : [ "String1", "String2" ],
          |  "BOOL_ARRAY" : [ true, false, true ],
          |  "SHORT_ARRAY" : [ 10, 20, 30 ],
          |  "INT_ARRAY" : [ 100, 200, 300 ],
          |  "LONG_ARRAY" : [ 10000000000, 20000000000 ],
          |  "DEC_ARRAY" : [ 123.45, 678.9 ],
          |  "DATE_ARRAY" : [ "2025-01-01", "2025-12-31" ],
          |  "TS_ARRAY" : [ "2025-01-01T10:00:00.000+02:00", "2025-06-01T12:30:00.000+02:00" ],
          |  "BIN_ARRAY" : [ "3q2+7w==", "AQID" ]
          |}, {
          |  "ID" : 1,
          |  "STR_ARRAY" : [ "String3", null ],
          |  "BOOL_ARRAY" : [ true, null, true ],
          |  "SHORT_ARRAY" : [ 50, null ],
          |  "INT_ARRAY" : [ null, 200, 300 ],
          |  "LONG_ARRAY" : [ 21234540000, null ],
          |  "DEC_ARRAY" : [ null, 678.9 ],
          |  "DATE_ARRAY" : [ "2025-01-01", null ],
          |  "TS_ARRAY" : [ null, "2025-06-01T12:30:00.000+02:00" ],
          |  "BIN_ARRAY" : [ "AavNIw==", null ]
          |}, {
          |  "ID" : 2
          |} ]""".stripMargin
      withTempDirectory("jdbc_native_array") { tempDir =>
        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")

        val conf = getConfig(tempDir)
        val exitCode = AppRunner.runPipeline(conf)
        assert(exitCode == 0)

        val resultDf = spark.read.parquet(table1Path.toString)

        assert(resultDf.count() == 3)

        val actualData = SparkUtils.convertDataFrameToPrettyJSON(resultDf)

        compareTextVertical(actualData, expectedData)
      }
    }
  }

  def getConfig(basePath: String, useJdbsNative: Boolean = true): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_ingestion_native_array.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |jdbc.url="$url"
         |jdbc.user="$user"
         |jdbc.password="$password"
         |jdbc.native=$useJdbsNative
         |
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
