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

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.extras.mocks.QueryExecutorSpy
import za.co.absa.pramen.extras.sink.EnceladusSink
import za.co.absa.pramen.extras.sink.EnceladusSink.{DATASET_NAME_KEY, DATASET_VERSION_KEY, HIVE_TABLE_KEY}
import za.co.absa.pramen.extras.utils.FsUtils

import java.lang
import java.nio.file.{Files, Paths}
import java.time.LocalDate

class EnceladusSinkSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {

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
          val sinkResult = sink.send(exampleDf,
            "dummy_table",
            null,
            infoDate,
            Map("path" -> outputPath.toUri.toString, "info.version" -> "1")
          )

          assert(sinkResult.recordsSent == 3)
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

    "autoDetectVersionNumber" should {
      val sink = EnceladusSink.apply(conf, "", spark)

      withTempDirectory("enceladus_raw") { tempRaw =>
        withTempDirectory("enceladus_publish") { tempPublish =>
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v1"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v2"))

          Files.createDirectories(Paths.get(tempPublish, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=1"))

          val rawBasePath = new Path(tempRaw, "my_table")
          val publishBasePath = new Path(tempPublish, "my_table")

          implicit val qe: QueryExecutorSpy = new QueryExecutorSpy()

          val version = sink.autoDetectVersionNumber("my_table", infoDate, rawBasePath, Some(publishBasePath), None)

          assert(version == 2)
        }
      }
    }
  }

  "runEnceladus()" should {
    "run Enceladus if configured" in {
      val conf = ConfigFactory.parseString(
        s"""info.date.column = "info_date"
           |format = "json"
           |mode = "overwrite"
           |
           |info.file.generate = false
           |enceladus.run.main.class = "za.co.absa.pramen.extras.mocks.AppMainSilentMock"
           |
           |""".stripMargin)

      implicit val qe: QueryExecutorSpy = new QueryExecutorSpy

      val sink = EnceladusSink.apply(conf, "", spark)

      sink.runEnceladusIfNeeded("my_table",
        infoDate,
        2,
        new Path("/dummy"),
        Map(
          DATASET_NAME_KEY -> "m_dayaset",
          DATASET_VERSION_KEY -> "22"
        )
      )

      assert(qe.executed.isEmpty)
    }

    "forward Hive Exception" in {
      val conf = ConfigFactory.parseString(
        s"""info.date.column = "info_date"
           |format = "json"
           |mode = "overwrite"
           |
           |info.file.generate = false
           |enceladus.run.main.class = "za.co.absa.pramen.extras.mocks.AppMainSilentMock"
           |
           |""".stripMargin)

      implicit val qe: QueryExecutorSpy = new QueryExecutorSpy(throwException = Some(new lang.IllegalStateException))

      val sink = EnceladusSink.apply(conf, "", spark)

      assertThrows[IllegalStateException] {
        sink.runEnceladusIfNeeded("my_table",
          infoDate,
          2,
          new Path("/dummy"),
          Map(
            DATASET_NAME_KEY -> "m_dayaset",
            DATASET_VERSION_KEY -> "22",
            HIVE_TABLE_KEY -> "test_table"
          )
        )
      }

      assert(qe.executed.isEmpty)
    }

    "forward the exception if something goes wrong" in {
      val conf = ConfigFactory.parseString(
        s"""info.date.column = "info_date"
           |format = "json"
           |mode = "overwrite"
           |
           |info.file.generate = false
           |enceladus.run.main.class = "za.co.absa.pramen.extras.mocks.AppMainMock"
           |
           |""".stripMargin)

      implicit val qe: QueryExecutorSpy = new QueryExecutorSpy

      val sink = EnceladusSink.apply(conf, "", spark)

      val ex = intercept[RuntimeException] {
        sink.runEnceladusIfNeeded("my_table",
          infoDate,
          2,
          new Path("/dummy"),
          Map(
            DATASET_NAME_KEY -> "m_dataset",
            DATASET_VERSION_KEY -> "22"
          )
        )
      }

      assert(ex.getCause.getMessage.contains("Main reached"))
      assert(ex.getCause.getMessage.contains("--dataset-name m_dataset --dataset-version 22 --report-date 2022-02-18 --report-version 2 --menas-auth-keytab menas.keytab --raw-format json"))
    }
  }
}
