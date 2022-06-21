/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.writer

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.WordSpec
import za.co.absa.pramen.api.v2.Query
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.framework.utils.SparkUtils

import java.nio.file.Paths
import java.time.LocalDate

class TableWriterDeltaSuite extends WordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._
  
  private val infoDateColumn = "INFORMATION_DATE"
  private val infoDate = LocalDate.of(2021, 8, 16)
  private val infoDateFormat = "yyyy-MM-dd"

  "TableWriterDelta" should {
    "be able to construct a new writer from config" in {
      val conf = ConfigFactory.empty()
      val writer = TableWriterDelta.apply(infoDateColumn, infoDateFormat, Query.Path("/tmp/dir"), 100, conf, "")

      assert(writer.isInstanceOf[TableWriterDelta])
    }

    "be able to pass extra options to the writer" in {
      withTempDirectory("delta_test") { tempDir =>
        val conf = ConfigFactory.parseString("""option.myoption = 1""")
        val writer = TableWriterDelta.apply(infoDateColumn, infoDateFormat, Query.Path("/tmp/dir"), 100, conf, "")

        assert(writer.getExtraOptions.contains("myoption"))
        assert(writer.getExtraOptions("myoption") == "1")
      }
    }

    "be able to write results to a delta folder" in {
      val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "INFORMATION_DATE" : "2021-08-16"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "INFORMATION_DATE" : "2021-08-16"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "INFORMATION_DATE" : "2021-08-16"
        |} ]""".stripMargin

      withTempDirectory("delta_test") { tempDir =>
        val writer = getWriter(tempDir)

        val df = getDataFrame1

        writer.write(df, infoDate, Some(3))

        val actual = deltaToJson(tempDir)

        compareText(actual, expected)
      }
    }

    "be able to overwrite a delta folder" in {
      val expected =
        """[ {
          |  "a" : "D",
          |  "b" : 4,
          |  "INFORMATION_DATE" : "2021-08-16"
          |}, {
          |  "a" : "E",
          |  "b" : 5,
          |  "INFORMATION_DATE" : "2021-08-16"
          |}, {
          |  "a" : "F",
          |  "b" : 6,
          |  "INFORMATION_DATE" : "2021-08-16"
          |} ]""".stripMargin

      withTempDirectory("delta_test") { tempDir =>
        val writer = getWriter(tempDir)

        val df1 = getDataFrame1
        val df2 = getDataFrame2

        writer.write(df1, infoDate, Some(3))
        writer.write(df2, infoDate, Some(3))

        val actual = deltaToJson(tempDir)

        compareText(actual, expected)
      }
    }

// Partitioned tables are not supported in Spark used for unit tests
//    "be able to overwrite a delta table" in {
//      val expected =
//        """[ {
//          |  "a" : "D",
//          |  "b" : 4,
//          |  "INFORMATION_DATE" : "2021-08-16"
//          |}, {
//          |  "a" : "E",
//          |  "b" : 5,
//          |  "INFORMATION_DATE" : "2021-08-16"
//          |}, {
//          |  "a" : "F",
//          |  "b" : 6,
//          |  "INFORMATION_DATE" : "2021-08-16"
//          |} ]""".stripMargin
//
//      withTempDirectory("delta_test") { tempDir =>
//        val writer = new TableWriterDelta(infoDateColumn, infoDateFormat, Query.Table("tempTable1"), 5, Map.empty[String, String])
//
//        val df1 = getDataFrame1
//
//        writer.write(df1, infoDate, Some(3))
//
//        val actual = SparkUtils.prettyJSON(
//          spark.table("tempTable1").orderBy("b")
//          .toJSON
//          .collect()
//          .mkString("[", ",", "]"))
//
//        compareText(actual, expected)
//      }
//    }

    "fail if input schema changes" in {
      withTempDirectory("delta_test") { tempDir =>
        val writer = getWriter(tempDir)

        val df1 = getDataFrame1
        val df2 = getDataFrame2.withColumn("b", col("b").cast(StringType))

        writer.write(df1, infoDate, Some(3))

        val ex = intercept[AnalysisException] {
          writer.write(df2, infoDate, Some(3))
        }

        assert(ex.getMessage().contains("Failed to merge fields 'b' and 'b'"))
      }
    }
  }

  private def getWriter(tempDir: String): TableWriterDelta = {
    val outputDir = Paths.get(tempDir, "out").toString
    new TableWriterDelta(infoDateColumn, infoDateFormat, Query.Path(outputDir), 5, Map.empty[String, String])
  }

  private def deltaToJson(path: String): String = {
    SparkUtils.prettyJSON(spark.read
      .format("delta")
      .load(Paths.get(path, "out").toString)
      .toJSON
      .collect()
      .mkString("[", ",", "]"))
  }

  private def getDataFrame1: DataFrame = {
    List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  }

  private def getDataFrame2: DataFrame = {
    List(("D", 4), ("E", 5), ("F", 6)).toDF("a", "b")
  }
}
