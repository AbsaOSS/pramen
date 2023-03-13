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

package za.co.absa.pramen.extras.tests.sink

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.extras.mocks.QueryExecutorSpy
import za.co.absa.pramen.extras.sink.{EnceladusSink, StandardizationSink}
import za.co.absa.pramen.extras.sink.EnceladusSink.{DATASET_NAME_KEY, DATASET_VERSION_KEY, HIVE_TABLE_KEY}
import za.co.absa.pramen.extras.utils.FsUtils

import java.lang
import java.nio.file.{Files, Paths}
import java.time.LocalDate

class StandardizationSinkSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "StandardizationSink" should {
    val conf = ConfigFactory.parseString(
      s"""info.file {
         |  generate = true
         |
         |  source.application = "MyApp"
         |  country = "Africa"
         |  history.type = "Snapshot"
         |  timestamp.format = "dd-MM-yyyy HH:mm:ss Z"
         |  date.format = "yyyy-MM-dd"
         |}
         |""".stripMargin)

    "work for raw + publish" when {
      var sink: StandardizationSink = null

      "constructed from a config" in {
        sink = StandardizationSink.apply(conf, "", spark)

        assert(sink.isInstanceOf[StandardizationSink])
      }

      "connect does nothing" in {
        sink.connect()
      }

      withTempDirectory("std_sink") { tempDir =>
        val rawPath = new Path(tempDir, "raw")
        val rawPartitionPath = new Path(rawPath, "2022/02/18/v1")
        val publishPath = new Path(tempDir, "publish")
        val publishPartitionPath = new Path(publishPath, s"enceladus_info_date=$infoDate/enceladus_info_version=1")
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        "send sends data to the target directory" in {
          val count = sink.send(exampleDf,
            "dummy_table",
            null,
            infoDate,
            Map(
              "raw.base.path" -> rawPath.toUri.toString,
              "publish.base.path" -> publishPath.toUri.toString,
              "info.version" -> "1"
            )
          )

          assert(count == 3)
          assert(fsUtils.exists(rawPartitionPath))
          assert(fsUtils.getFilesRecursive(rawPartitionPath, "*.json").nonEmpty)
          assert(fsUtils.exists(publishPartitionPath))
          assert(fsUtils.getFilesRecursive(publishPartitionPath, "*.parquet").nonEmpty)
        }

        "info file should be as expected" in {
          val infoFileContents = Files.readAllLines(Paths.get(rawPartitionPath.toString, "_INFO")).toArray.mkString("\n")

          assert(infoFileContents.contains(""""software" : "pramen","""))
          assert(infoFileContents.contains(""""controlValue" : "3""""))
          assert(infoFileContents.contains(""""informationDate" : "2022-02-18","""))
        }
      }

      "close does nothing" in {
        sink.close()
      }
    }

    "work for publish only" when {
      var sink: StandardizationSink = null

      "constructed from a config" in {
        sink = StandardizationSink.apply(conf, "", spark)

        assert(sink.isInstanceOf[StandardizationSink])
      }

      withTempDirectory("std_sink") { tempDir =>
        val publishPath = new Path(tempDir, "publish")
        val publishPartitionPath = new Path(publishPath, s"enceladus_info_date=$infoDate/enceladus_info_version=1")
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        "send sends data to the target directory" in {
          val count = sink.send(exampleDf,
            "dummy_table",
            null,
            infoDate,
            Map(
              "publish.base.path" -> publishPath.toUri.toString,
              "info.version" -> "1"
            )
          )

          assert(count == 3)
          assert(fsUtils.exists(publishPartitionPath))
          assert(fsUtils.getFilesRecursive(publishPartitionPath, "*.parquet").nonEmpty)
        }

        "info file should be as expected" in {
          val infoFileContents = Files.readAllLines(Paths.get(publishPartitionPath.toString, "_INFO")).toArray.mkString("\n")

          assert(infoFileContents.contains(""""software" : "pramen","""))
          assert(infoFileContents.contains(""""controlValue" : "3""""))
          assert(infoFileContents.contains(""""informationDate" : "2022-02-18","""))
        }
      }
    }

    "getHiveRepairEnceladusQuery()" should {
      "return a valid query when a db is setup" in {
        val updatedConf = conf.
           withValue("hive.database", ConfigValueFactory.fromAnyRef("mydb"))

        val sink = StandardizationSink.apply(updatedConf, "", spark)

        val query = sink.getHiveRepairQuery("my_table")

        assert(query == "MSCK REPAIR TABLE mydb.my_table")
      }

      "return a valid query when a db is not setup" in {
        val sink = StandardizationSink.apply(conf, "", spark)

        val query = sink.getHiveRepairQuery("my_table")

        assert(query == "MSCK REPAIR TABLE my_table")
      }
    }

    "getHiveRepairQuery()" should {
      "return the repair query" in {
        val sink = StandardizationSink.apply(conf, "", spark)

        assert(sink.getHiveRepairQuery("db1.table1") == "MSCK REPAIR TABLE db1.table1")
      }
    }
  }
}