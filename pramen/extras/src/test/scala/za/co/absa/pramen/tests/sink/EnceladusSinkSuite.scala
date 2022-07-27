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

package za.co.absa.pramen.tests.sink

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.builtin.sink.EnceladusSink
import za.co.absa.pramen.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.utils.FsUtils

import java.nio.file.{Files, Paths}
import java.time.LocalDate

class EnceladusSinkSuite extends WordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "EnceladusSink" should {
    val conf = ConfigFactory.parseString(
      s"""info.date.column = "info_date"
         |partition.pattern = "{year}/{month}/{day}/v{version}"
         |format = "json"
         |mode = "overwrite"
         |save.empty = true
         |
         |info.file {
         |  generate = true
         |
         |  source.application = "MyApp"
         |  country = "Africa"
         |  history.type = "Snapshot"
         |  timestamp.format = "dd-MM-yyyy HH:mm:ss Z"
         |  date.format = "yyyy-MM-dd"
         |}
         |""".stripMargin)

    "work as expected" when {
      var sink: EnceladusSink = null

      "constructed from a config" in {
        sink = EnceladusSink.apply(conf, "", spark)

        assert(sink.isInstanceOf[EnceladusSink])
      }

      "connect does nothing" in {
        sink.connect()
      }

      withTempDirectory("enceladus_sink") { tempDir =>
        val outputPath = new Path(tempDir, "output")
        val partitionPath = new Path(outputPath, "2022/02/18/v1")
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        "send sends data to the target directory" in {
          val count = sink.send(exampleDf,
            "dummy_table",
            null,
            infoDate,
            Map("path" -> outputPath.toUri.toString)
          )

          assert(count == 3)
          assert(fsUtils.exists(partitionPath))
          assert(fsUtils.getFilesRecursive(partitionPath, "*.json").nonEmpty)
        }

        "info file should be as expected" in {
          val infoFileContents = Files.readAllLines(Paths.get(partitionPath.toString, "_INFO")).toArray.mkString("\n")

          assert(infoFileContents.contains(""""software" : "pramen","""))
          assert(infoFileContents.contains(""""controlValue" : "3""""))
          assert(infoFileContents.contains(""""informationDate" : "2022-02-18","""))
        }
      }

      "close does nothing" in {
        sink.close()
      }
    }
  }

}
