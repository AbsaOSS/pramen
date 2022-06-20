/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.framework.tests.utils

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.job.TransformExpression
import za.co.absa.pramen.framework.utils.SparkUtils
import za.co.absa.pramen.framework.utils.SparkUtils._

import java.time.LocalDate

class SparkUtilsSuite extends WordSpec with SparkTestBase {
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

      import spark.implicits._

      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a a", " b ")

      val actualDf = sanitizeDfColumns(df)

      val actual = convertDataFrameToPrettyJSON(actualDf).stripMargin.lines.mkString("").trim

      assert(stripLineEndings(actual) == stripLineEndings(expected))
    }

    "rename columns that start with .tbl" in {
      val expected =
        """[ {  "a_a" : "A",  "b" : 1}, {  "a_a" : "B",  "b" : 2}, {  "a_a" : "C",  "b" : 3} ]"""

      import spark.implicits._

      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("tbl.a a", "tbl.b")

      val actualDf = sanitizeDfColumns(df)

      val actual = convertDataFrameToPrettyJSON(actualDf).stripMargin.lines.mkString("").trim

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
      assert(dfOut.count == 3)
    }

    "apply specified transformations" in {
      val schemaTransformations = List(TransformExpression("c", "cast(b as string)"))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 3)
      assert(dfOut.schema.fields(2).name == "c")
      assert(dfOut.count == 3)
    }
  }

  "applyFilters" should {
    "do nothing for empty filters" in {
      val dfOut = applyFilters(exampleDf, Nil, null)

      assert(dfOut.count == 3)
    }

    "apply specified filters" in {
      val filters = List("b > 1")

      val dfOut = applyFilters(exampleDf, filters, infoDate)

      assert(dfOut.count == 2)
    }

    "apply filters with an info date" in {
      val filters = List("b < unix_timestamp(@infoDate)")

      val dfOut = applyFilters(exampleDf, filters, infoDate)

      assert(dfOut.count == 3)
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

}
