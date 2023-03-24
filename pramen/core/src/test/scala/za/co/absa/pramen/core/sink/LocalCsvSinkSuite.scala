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

package za.co.absa.pramen.core.sink

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.mocks.sink.CsvConversionParamsFactory
import za.co.absa.pramen.core.utils.{FsUtils, SparkUtils}

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.LocalDate

class LocalCsvSinkSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private val exampleDf: DataFrame = Seq(
    ("A", 10, 1649319691L, "2022-01-18"),
    ("B", 20, 1649318691L, "2022-02-28")
  ).toDF("str", "int", "ts", "date")
    .withColumn("date", $"date".cast("date"))
    .withColumn("ts", to_timestamp($"ts"))

  "apply()" should {
    "be able to construct a LocalCsvSink" in {
      val conf = ConfigFactory.parseString(
        """ temp.hadoop.path = "/tmp/csv_sink"
          | file.name.pattern = "FILE_@timestamp"
          | file.name.timestamp.pattern = "yyyyMMdd_HHmmss"
          |
          | column.name.transform = "make_upper"
          |
          | date.format = "yyyy-MM-dd"
          | timestamp.format = "yyyy-MM-dd HH:mm:ss Z"
          |
          | option {
          |   sep = "|"
          |   quoteAll = "false"
          |   header = "true"
          |}""".stripMargin
      )
      val sink = LocalCsvSink(conf, "parent", spark)

      assert(sink.isInstanceOf[LocalCsvSink])
    }

    "throw an exception on error in config" in {
      val conf = ConfigFactory.parseString(
        """option {
          |   sep = "|"
          |}""".stripMargin
      )

      val ex = intercept[IllegalArgumentException] {
        LocalCsvSink(conf, "parent", spark)
      }

      assert(ex.getMessage.contains("Mandatory configuration options are missing"))
    }
  }

  "write()" should {
    "do nothing if the input data frame is empty" in {
      val df = exampleDf.filter($"int" < 0)

      val sink = getUseCase("/dummy")

      val sinkResult = sink.send(df, "table1", null, infoDate, Map[String, String](
        "path" -> "/dummy"
      ))

      assert(sinkResult.recordsSent == 0L)
    }

    "write an empty CSV file when it is turned on" in {
      val df = exampleDf.select("str", "int", "date").filter($"int" < 0)

      withTempDirectory("sink_localcsv") { tempDir =>
        val remoteDir = new Path(tempDir, "remote")
        val localDir = new Path(tempDir, "local")

        val sink = getUseCase(remoteDir.toString, fileNamePattern = "A_@tableName_@infoDate", createEmptyCsv = true)

        val sinkResult = sink.send(df, "table1", null, infoDate, Map[String, String](
          "path" -> localDir.toString
        ))

        val actualFileName = Paths.get(localDir.toString, "A_table1_2022-02-18.csv")

        assert(sinkResult.recordsSent == 0)
        assert(Files.exists(actualFileName))

        val contents = Files.readAllLines(actualFileName).toArray.mkString("\n")

        assert(contents.isEmpty)
      }
    }

    "write a header-only CSV file when it is turned on" in {
      val df = exampleDf.select("str", "int", "date").filter($"int" < 0)

      val expectedContent = "str,int,date\n"

      withTempDirectory("sink_localcsv") { tempDir =>
        val remoteDir = new Path(tempDir, "remote")
        val localDir = new Path(tempDir, "local")

        val sink = getUseCase(remoteDir.toString,
          fileNamePattern = "A_@tableName_@infoDate",
          createEmptyCsv = true,
          options = Map[String, String](
            "header" -> "true",
            "quoteAll" -> "false"
          ))

        val sinkResult = sink.send(df, "table1", null, infoDate, Map[String, String](
          "path" -> localDir.toString
        ))

        val actualFileName = Paths.get(localDir.toString, "A_table1_2022-02-18.csv")

        assert(sinkResult.recordsSent == 0)
        assert(Files.exists(actualFileName))

        val contents = Files.readAllLines(actualFileName).toArray.mkString("\n")

        compareText(contents, expectedContent)
      }
    }

    "write a quoted header-only CSV file when it is turned on" in {
      val df = exampleDf.select("str", "int", "date").filter($"int" < 0)

      val expectedContent = "\"str\",\"int\",\"date\"\n"

      withTempDirectory("sink_localcsv") { tempDir =>
        val remoteDir = new Path(tempDir, "remote")
        val localDir = new Path(tempDir, "local")

        val sink = getUseCase(remoteDir.toString,
          fileNamePattern = "A_@tableName_@infoDate",
          createEmptyCsv = true,
          options = Map[String, String](
            "header" -> "true",
            "quoteAll" -> "true"
          ))

        val sinkResult = sink.send(df, "table1", null, infoDate, Map[String, String](
          "path" -> localDir.toString
        ))

        val actualFileName = Paths.get(localDir.toString, "A_table1_2022-02-18.csv")

        assert(sinkResult.recordsSent == 0)
        assert(Files.exists(actualFileName))

        val contents = Files.readAllLines(actualFileName).toArray.mkString("\n")

        compareText(contents, expectedContent)
      }
    }

    "write a CSV file ot the target directory" in {
      val expectedContent =
        """"str","int","date"
          |"A","10","2022-01-18"
          |"B","20","2022-02-28"
          |""".stripMargin

      withTempDirectory("sink_localcsv") { tempDir =>
        val df = exampleDf.select("str", "int", "date")

        val remoteDir = new Path(tempDir, "remote")
        val localDir = new Path(tempDir, "local")

        val sink = getUseCase(remoteDir.toString, fileNamePattern = "A_@tableName_@infoDate")

        val sinkResult = sink.send(df, "table1", null, infoDate, Map[String, String](
          "path" -> localDir.toString
        ))

        val actualFileName = Paths.get(localDir.toString, "A_table1_2022-02-18.csv")

        assert(sinkResult.recordsSent == 2)
        assert(Files.exists(actualFileName))

        val contents = Files.readAllLines(actualFileName).toArray.mkString("\n")

        compareText(contents, expectedContent)
      }
    }

    "throw an exception if the output path is missing" in {
      val df = exampleDf.filter($"int" < 0)

      val sink = getUseCase("/dummy")

      val ex = intercept[IllegalArgumentException] {
        sink.send(df, "table1", null, infoDate, Map.empty[String, String])
      }

      assert(ex.getMessage.contains("Missing required parameter of LocalCsvSink"))
    }
  }

  "getEffectiveOptions()" should {
    "apply the application defaults" in {
      val sink = getUseCase("/dummy")

      val effectiveOptions = sink.getEffectiveOptions(Map.empty[String, String])

      assert(effectiveOptions("header") == "true")
      assert(effectiveOptions("quoteAll") == "true")
    }

    "allow overriding the application defaults" in {
      val sink = getUseCase("/dummy")

      val effectiveOptions = sink.getEffectiveOptions(Map[String, String](
        "header" -> "1",
        "quoteAll" -> "2",
        "sep" -> "|"
      ))

      assert(effectiveOptions("header") == "1")
      assert(effectiveOptions("quoteAll") == "2")
      assert(effectiveOptions("sep") == "|")
    }
  }

  "copyToLocal()" should {
    "copy files from a hadoop folder to a local directory" in {
      withTempDirectory("sink_localcsv") { tempDir =>
        val sink = getUseCase(tempDir)

        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val remoteDir = new Path(tempDir, "remote")
        val localDir = new Path(tempDir, "local")

        fsUtils.fs.mkdirs(remoteDir)
        fsUtils.fs.mkdirs(localDir)
        fsUtils.writeFile(new Path(remoteDir, "file.csv"), "Test content")

        val actualFileName = sink.copyToLocal("table1", infoDate, remoteDir, localDir.toString, fsUtils)

        val contents = Files.readAllLines(Paths.get(actualFileName)).toArray.mkString("\n")

        assert(actualFileName.contains(s"local${File.separator}table1_2022-02-18_"))
        assert(contents == "Test content")
      }
    }
  }

  "getFileName()" should {
    "replace template variables with actual values" in {
      val sink = getUseCase("/dummy")

      val actual = sink.getFileName("A_@tableName_@infoDate_@timestamp", "HHmm", "table1", infoDate)

      assert(actual.startsWith("A_table1_2022-02-18_"))
    }
  }

  "getFinalFileName()" should {
    "add the path and file extension to the file name" in {
      val sink = getUseCase("/dummy", "A_@tableName_@infoDate")

      val actual = sink.getFinalFileName("table1", infoDate, "/bigdata")

      val expected = s"${File.separator}bigdata${File.separator}A_table1_2022-02-18.csv"
      assert(actual == expected)
    }
  }

  "convertDateTimeToString()" should {
    "convert date and timestamp fields to strings of the given format" in {
      val expected =
        """[ {
          |  "str" : "A",
          |  "int" : 10,
          |  "ts" : "2022-04-07_102131",
          |  "date" : "18-01-2022"
          |}, {
          |  "str" : "B",
          |  "int" : 20,
          |  "ts" : "2022-04-07_100451",
          |  "date" : "28-02-2022"
          |} ]""".stripMargin

      val sink = getUseCase("/dummy")

      val dfOut = sink.convertDateTimeToString(exampleDf, "dd-MM-yyyy", "yyyy-MM-dd_HHmmss")

      val actual = SparkUtils.convertDataFrameToPrettyJSON(dfOut)

      compareText(actual, expected)
    }
  }

  "applyColumnTransformations()" should {
    "support 'no_change'" in {
      val expected =
        """[ {
          |  "str" : "A",
          |  "INT" : 10
          |}, {
          |  "str" : "B",
          |  "INT" : 20
          |} ]""".stripMargin

      val sink = getUseCase("/dummy", columnNameTransform = ColumnNameTransform.NoChange)

      val dfOut = sink.applyColumnTransformations(exampleDf.select("str", "INT"))

      val actual = SparkUtils.convertDataFrameToPrettyJSON(dfOut)

      compareText(actual, expected)
    }

    "support 'make_upper'" in {
      val expected =
        """[ {
          |  "STR" : "A",
          |  "INT" : 10
          |}, {
          |  "STR" : "B",
          |  "INT" : 20
          |} ]""".stripMargin

      val sink = getUseCase("/dummy", columnNameTransform = ColumnNameTransform.MakeUpper)

      val dfOut = sink.applyColumnTransformations(exampleDf.select("str", "INT"))

      val actual = SparkUtils.convertDataFrameToPrettyJSON(dfOut)

      compareText(actual, expected)
    }
    "support 'make_lower'" in {
      val expected =
        """[ {
          |  "str" : "A",
          |  "int" : 10
          |}, {
          |  "str" : "B",
          |  "int" : 20
          |} ]""".stripMargin

      val sink = getUseCase("/dummy", columnNameTransform = ColumnNameTransform.MakeLower)

      val dfOut = sink.applyColumnTransformations(exampleDf.select("str", "INT"))

      val actual = SparkUtils.convertDataFrameToPrettyJSON(dfOut)

      compareText(actual, expected)
    }
  }

  def getUseCase(tempDirectory: String,
                 fileNamePattern: String = "@tableName_@infoDate_@timestamp",
                 options: Map[String, String] = Map.empty[String, String],
                 columnNameTransform: ColumnNameTransform = ColumnNameTransform.NoChange,
                 createEmptyCsv: Boolean = false
                ): LocalCsvSink = {
    val params = CsvConversionParamsFactory.getDummyCsvConversionParams(csvOptions = options,
      tempHadoopPath = tempDirectory,
      fileNamePattern = fileNamePattern,
      columnNameTransform = columnNameTransform,
      createEmptyCsv = createEmptyCsv)

    new LocalCsvSink(ConfigFactory.empty(), params)
  }
}
