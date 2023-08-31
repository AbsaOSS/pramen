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

package za.co.absa.pramen.core.tests.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.pipeline.TransformExpression
import za.co.absa.pramen.core.utils.SparkUtils
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.LocalDate

class SparkUtilsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture  {
  import spark.implicits._

  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private val infoDate = LocalDate.of(2021, 12, 18)

  "sanitizeDfColumns()" should {
    "rename spaces of input dataframe columns" in {
      val expected =
      """[ {
        |  "a_a" : "A",
        |  "_b_" : 1
        |}, {
        |  "a_a" : "B",
        |  "_b_" : 2
        |}, {
        |  "a_a" : "C",
        |  "_b_" : 3
        |} ]"""

      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a a", " b ")

      val actualDf = sanitizeDfColumns(df, " ")

      val actual = convertDataFrameToPrettyJSON(actualDf).stripMargin.linesIterator.mkString("").trim

      assert(stripLineEndings(actual) == stripLineEndings(expected))
    }

    "rename special characters of input dataframe columns" in {
      val expected =
        """[ {
          |  "a_a" : "A",
          |  "_b_" : 1
          |}, {
          |  "a_a" : "B",
          |  "_b_" : 2
          |}, {
          |  "a_a" : "C",
          |  "_b_" : 3
          |} ]"""

      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a:a", "<b>")

      val actualDf = sanitizeDfColumns(df, " :<>")

      val actual = convertDataFrameToPrettyJSON(actualDf).stripMargin.linesIterator.mkString("").trim

      assert(stripLineEndings(actual) == stripLineEndings(expected))
    }

    "rename columns that start with .tbl" in {
      val expected =
        """[ {  "a_a" : "A",  "b" : 1}, {  "a_a" : "B",  "b" : 2}, {  "a_a" : "C",  "b" : 3} ]"""

      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("tbl.a a", "tbl.b")

      val actualDf = sanitizeDfColumns(df, " ")

      val actual = convertDataFrameToPrettyJSON(actualDf).stripMargin.linesIterator.mkString("").trim

      assert(stripLineEndings(actual) == stripLineEndings(expected))
    }

    "convert schema from Spark to Json and back should produce the same schema" in {
      val testCaseSchema = StructType(
        Array(
          StructField("id", LongType),
          StructField("long_f", LongType),
          StructField("decimal_f", DecimalType(10, 8)),
          StructField("struct_array", StructType(Array(
            StructField("double_f", DoubleType),
            StructField("decimal2_f", DecimalType(38, 18))
          ))),
          StructField("nested_array", ArrayType(StructType(Array(
            StructField("int_f", IntegerType),
            StructField("inner_array", ArrayType(StructType(Array(
              StructField("string_f", StringType),
              StructField("date_f", DateType),
              StructField("inner_struct", StructType(Array(
                StructField("timestamp_f", TimestampType),
                StructField("short_f", ShortType)
              )))
            ))))
          ))))
        ))

      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], testCaseSchema)

      val schemaJson = df.schema.json

      val sparkSchema = SparkUtils.schemaFromJson(schemaJson).get

      assert(sparkSchema == testCaseSchema)
    }
  }

  "applyTransformations" should {
    "do nothing for empty transformations" in {

      val dfOut = applyTransformations(exampleDf, Nil)

      assert(dfOut.schema.fields.length == 2)
      assert(dfOut.count() == 3)
    }

    "apply specified transformations" in {
      val schemaTransformations = List(TransformExpression("c", Some("cast(b as string)"), None))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 3)
      assert(dfOut.schema.fields(2).name == "c")
      assert(dfOut.count() == 3)
      assert(!dfOut.schema.fields(2).metadata.contains("comment"))
    }

    "support comment metadata" in {
      val schemaTransformations = List(TransformExpression("c", Some("cast(b as string)"), Some("dummy")))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 3)
      assert(dfOut.schema.fields(2).name == "c")
      assert(dfOut.schema.fields(2).metadata.contains("comment"))
      assert(dfOut.schema.fields(2).metadata.getString("comment") == "dummy")
      assert(dfOut.count() == 3)
    }
    "support adding comments" in {
      val schemaTransformations = List(TransformExpression("b", None, Some("dummy")))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 2)
      assert(dfOut.schema.fields(1).name == "b")
      assert(dfOut.schema.fields(1).metadata.contains("comment"))
      assert(dfOut.schema.fields(1).metadata.getString("comment") == "dummy")
    }
    "support dropping of columns when the expression is empty" in {
      val schemaTransformations = List(TransformExpression("b", None, None))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 1)
      assert(dfOut.schema.fields.head.name == "a")
    }
    "support dropping of columns when the expression is an empty string" in {
      val schemaTransformations = List(TransformExpression("b", Some(""), None))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 1)
      assert(dfOut.schema.fields.head.name == "a")
    }
    "support dropping of columns when the expression is 'drop''" in {
      val schemaTransformations = List(TransformExpression("b", Some(" DROP"), None))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 1)
      assert(dfOut.schema.fields.head.name == "a")
    }
  }

  "applyFilters" should {
    "do nothing for empty filters" in {
      val dfOut = applyFilters(exampleDf, Nil, null, null, null)

      assert(dfOut.count() == 3)
    }

    "apply specified filters" in {
      val filters = List("b > 1")

      val dfOut = applyFilters(exampleDf, filters, infoDate, infoDate, infoDate)

      assert(dfOut.count() == 2)
    }

    "apply filters with an info date" in {
      val filters = List("b < unix_timestamp(@infoDate)")

      val dfOut = applyFilters(exampleDf, filters, infoDate, infoDate, infoDate)

      assert(dfOut.count() == 3)
    }

    "apply filters with a date as string" in {
      val filters = List("b < unix_timestamp(date'@date')")

      val dfOut = applyFilters(exampleDf, filters, infoDate, infoDate, infoDate)

      assert(dfOut.count() == 3)
    }

    "apply filters with a date range" in {
      val filters = List("b >= unix_timestamp(date'@dateFrom') AND b < unix_timestamp(date'@dateTo')")

      val inputDf = exampleDf.withColumn("b", lit(1639824275))

      val dfOut = applyFilters(inputDf, filters, infoDate, infoDate.minusDays(1), infoDate.plusDays(1))

      assert(dfOut.count() == 3)
    }
  }

  "addProcessingTimestamp" should {
    "add a timestamp field if it does not exist" in {
      val actualDf = addProcessingTimestamp(exampleDf, "ts")

      assert(actualDf.schema.fields.exists(f => f.name == "ts"))

      val tsField = actualDf.schema.fields.find(f => f.name == "ts")

      assert(tsField.get.dataType.isInstanceOf[TimestampType])
    }

    "do nothing is the field already exist" in {
      val actualDf = addProcessingTimestamp(exampleDf.withColumn("ts", lit("a")), "ts")

      assert(actualDf.schema.fields.exists(f => f.name == "ts"))

      val tsField = actualDf.schema.fields.find(f => f.name == "ts")

      assert(tsField.get.dataType.isInstanceOf[StringType])
    }
  }

  "hasDataInPartition" should {
    "return false when partition folder does not exist" in {
      withTempDirectory("spark_utils_test") { tempDir =>
        val actual = hasDataInPartition(infoDate, "pramen_info_date", "yyyy-MM-dd", tempDir)

        assert(!actual)
      }
    }

    "return false if the partition folder exists, but doesn't have files" in {
      withTempDirectory("spark_utils_test") { tempDir =>
        val partitionPath = new Path(tempDir, s"pramen_info_date=$infoDate")

        val fs = partitionPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.mkdirs(partitionPath)

        val actual = hasDataInPartition(infoDate, "pramen_info_date", "yyyy-MM-dd", tempDir)

        assert(!actual)
      }
    }

    "return false if the partition folder exists, but contains only hidden files" in {
      withTempDirectory("spark_utils_test") { tempDir =>
        val partitionPath = new Path(tempDir, s"pramen_info_date=$infoDate")

        val fs = partitionPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.mkdirs(partitionPath)
        fs.create(new Path(partitionPath, "_SUCCESS")).close()

        val actual = hasDataInPartition(infoDate, "pramen_info_date", "yyyy-MM-dd", tempDir)

        assert(!actual)
      }
    }

    "return true if the partition folder exists and contains a file" in {
      withTempDirectory("spark_utils_test") { tempDir =>
        val partitionPath = new Path(tempDir, s"pramen_info_date=$infoDate")

        val fs = partitionPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.mkdirs(partitionPath)
        fs.create(new Path(partitionPath, "1.dat")).close()

        val actual = hasDataInPartition(infoDate, "pramen_info_date", "yyyy-MM-dd", tempDir)

        assert(actual)
      }
    }

    "return true if the partition folder exists and contains non-hidden subdirs" in {
      withTempDirectory("spark_utils_test") { tempDir =>
        val partitionPath = new Path(tempDir, s"pramen_info_date=$infoDate")

        val fs = partitionPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.mkdirs(partitionPath)
        fs.mkdirs(new Path(partitionPath, "subpath"))

        val actual = hasDataInPartition(infoDate, "pramen_info_date", "yyyy-MM-dd", tempDir)

        assert(actual)
      }
    }
  }

  "getPartitionPath" should {
    "construct path with an info date date" in {
      val actual = getPartitionPath(infoDate, "pramen_info_date", "yyyy-MM-dd", "s3://bucket/path")

      assert(actual.toUri.toString == "s3://bucket/path/pramen_info_date=2021-12-18")
    }
  }

  "collectTable" should {
    "return a dataframe as array of arrays with default number of records" in {
      val table = SparkUtils.collectTable(exampleDf)

      assert(table.length == 4) // headers + 3 rows
      assert(table(0).length == 2) // 2 columns
      assert(table(1).length == 2) // 2 columns
      assert(table(2).length == 2) // 2 columns
      assert(table(0)(0) == "a")
      assert(table(0)(1) == "b")
      assert(table(1)(0) == "A")
      assert(table(1)(1) == "1")
      assert(table(2)(0) == "B")
      assert(table(2)(1) == "2")
    }

    "return a dataframe as array of arrays with number of records = 2" in {
      val table = SparkUtils.collectTable(exampleDf, 2)

      assert(table.length == 3) // headers + 3 rows
      assert(table(0).length == 2) // 2 columns
      assert(table(1).length == 2) // 2 columns
      assert(table(0)(0) == "a")
      assert(table(0)(1) == "b")
      assert(table(1)(0) == "A")
      assert(table(1)(1) == "1")
    }

    "return a dataframe as array of arrays with full table" in {
      val table = SparkUtils.collectTable(exampleDf, 0)

      assert(table.length == 4) // headers + 3 rows
    }
  }
}
