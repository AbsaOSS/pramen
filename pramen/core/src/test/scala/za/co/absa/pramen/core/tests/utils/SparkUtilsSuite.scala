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
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.api.FieldChange._
import za.co.absa.pramen.core.pipeline.TransformExpression
import za.co.absa.pramen.core.samples.SampleCaseClass2
import za.co.absa.pramen.core.utils.SparkUtils
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.LocalDate

class SparkUtilsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture  {
  import spark.implicits._

  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private val infoDate = LocalDate.of(2021, 12, 18)

  "getStructType" should {
    "convert a case class to a StructType" in {
      val expected =
      """{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "strValue",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "intValue",
        |    "type" : "integer",
        |    "nullable" : false,
        |    "metadata" : { }
        |  }, {
        |    "name" : "longValue",
        |    "type" : "long",
        |    "nullable" : false,
        |    "metadata" : { }
        |  } ]
        |}""".stripMargin
      val actual = SparkUtils.getStructType[SampleCaseClass2].prettyJson

      compareText(actual, expected)
    }
  }

  "dataFrameToJson" should {
    "convert a dataframe to a json string" in {
      val expected =
        """[ {
          |  "a" : "A",
          |  "b" : 1
          |}, {
          |  "a" : "B",
          |  "b" : 2
          |}, {
          |  "a" : "C",
          |  "b" : 3
          |} ]""".stripMargin

      val actual = SparkUtils.dataFrameToJson(exampleDf)

      assert(actual == expected)
    }
  }

  "convertDataFrameToPrettyJSON" should {
    "convert a dataframe to a pretty json string" in {
      val expected =
        """[ {
          |  "a" : "A",
          |  "b" : 1
          |} ]""".stripMargin

      val actual = SparkUtils.convertDataFrameToPrettyJSON(exampleDf, 1)

      assert(actual == expected)
    }
  }

  "sanitizeDfColumns()" should {
    "rename spaces of input dataframe columns" in {
      val expected =
      """[ {
        |  "a_a" : "A",
        |  "_b_" : 1,
        |  "c" : 4
        |}, {
        |  "a_a" : "B",
        |  "_b_" : 2,
        |  "c" : 5
        |}, {
        |  "a_a" : "C",
        |  "_b_" : 3,
        |  "c" : 6
        |} ]"""

      val df = List(("A", 1, 4), ("B", 2, 5), ("C", 3, 6)).toDF("a a", " b ", "c")

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

  "schemaFromJson" should {
    "return None on a wrong schema" in {
      val schemaJson = """wrong_json"""

      val schema = SparkUtils.schemaFromJson(schemaJson)

      assert(schema.isEmpty)
    }

    "convert a json schema to StructType" in {
      val schemaJson = """{"type":"struct","fields":[{"name":"a","type":"string","nullable":true,"metadata":{}}]}"""

      val schema = SparkUtils.schemaFromJson(schemaJson)

      assert(schema.isDefined)
      assert(schema.get.fields.head.name == "a")
    }
  }

  "compareSchemas" should {
    "detect new columns" in {
      val schema1 = exampleDf.schema
      val schema2 = exampleDf.withColumn("c", lit(1)).schema

      val diff = compareSchemas(schema1, schema2)

      assert(diff.length == 1)
      assert(diff.head.isInstanceOf[NewField])
      assert(diff.head.asInstanceOf[NewField].columnName == "c")
      assert(diff.head.asInstanceOf[NewField].dataType == "integer")
    }

    "detect deleted columns" in {
      val schema1 = exampleDf.withColumn("c", lit(1)).schema
      val schema2 = exampleDf.schema

      val diff = compareSchemas(schema1, schema2)

      assert(diff.length == 1)
      assert(diff.head.isInstanceOf[DeletedField])
      assert(diff.head.asInstanceOf[DeletedField].columnName == "c")
      assert(diff.head.asInstanceOf[DeletedField].dataType == "integer")
    }

    "detect changed data types" in {
      val schema1 = exampleDf.schema
      val schema2 = exampleDf.withColumn("b", lit(1.1)).schema

      val diff = compareSchemas(schema1, schema2)

      assert(diff.length == 1)
      assert(diff.head.isInstanceOf[ChangedType])
      assert(diff.head.asInstanceOf[ChangedType].columnName == "b")
      assert(diff.head.asInstanceOf[ChangedType].oldType == "integer")
      assert(diff.head.asInstanceOf[ChangedType].newType == "double")
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
    "support dropping of columns when the expression is 'drop'" in {
      val schemaTransformations = List(TransformExpression("b", Some(" DROP"), None))

      val dfOut = applyTransformations(exampleDf, schemaTransformations)

      assert(dfOut.schema.fields.length == 1)
      assert(dfOut.schema.fields.head.name == "a")
    }
    "support dropping of columns when the expression is 'drop' and there is a comment" in {
      val schemaTransformations = List(TransformExpression("b", Some(" DROP"), Some("Dropping...")))

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

  "transformSchemaForCatalog" should {
    "transform string fields to varchar if metadata is defined" in {
      val expected =
        """id INT,
          |name VARCHAR(5) COMMENT 'Employee name',
          |subordinates STRUCT<id: INT, name: VARCHAR(7) COMMENT 'Subordinate name'>,
          |addresses ARRAY<VARCHAR(15)> COMMENT 'Address',
          |phone_numbers ARRAY<STRUCT<type: STRING, number: VARCHAR(10) COMMENT 'Phone number'>>,
          |matrix ARRAY<ARRAY<VARCHAR(1)>> COMMENT 'Some code'"""
          .stripMargin.replace("\r", "").replace("\n", "")

      val metadata1 = new MetadataBuilder().putLong("maxLength", 5).putString("comment", "Employee name").build()
      val metadata2 = new MetadataBuilder().putLong("maxLength", 7).putString("comment", "Subordinate name").build()
      val metadata3 = new MetadataBuilder().putLong("maxLength", 15).putString("comment", "Address").build()
      val metadata4 = new MetadataBuilder().putLong("maxLength", 10).putString("comment", "Phone number").build()
      val metadata5 = new MetadataBuilder().putLong("maxLength", 1).putString("comment", "Some code").build()

      val schema = StructType(
        StructField("id", IntegerType, nullable = false) ::
          StructField("name", StringType, nullable = true, metadata = metadata1) ::
          StructField("subordinates", StructType(
            StructField("id", IntegerType, nullable = true) ::
              StructField("name", StringType, nullable = false, metadata = metadata2) :: Nil)
          ) ::
          StructField("addresses", ArrayType(StringType, containsNull = true), nullable = true, metadata = metadata3) ::
          StructField("phone_numbers", ArrayType(StructType(
            StructField("type", StringType, nullable = true) ::
              StructField("number", StringType, nullable = true, metadata = metadata4) :: Nil)
          )) ::
          StructField("matrix", ArrayType(ArrayType(StringType)), metadata = metadata5) ::
          Nil)

      val transformedSchema = SparkUtils.transformSchemaForCatalog(schema)
      val actual = transformedSchema.toDDL

      assert(transformedSchema.fields.head.nullable)
      assert(transformedSchema.fields(1).dataType.isInstanceOf[VarcharType])
      assert(transformedSchema.fields(1).dataType.asInstanceOf[VarcharType].length == 5)
      assert(transformedSchema.fields(2).dataType.asInstanceOf[StructType].fields(1).dataType .isInstanceOf[VarcharType])
      assert(transformedSchema.fields(3).dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[VarcharType])
      assert(transformedSchema.fields(4).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.head.dataType.isInstanceOf[StringType])
      assert(transformedSchema.fields(4).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType.isInstanceOf[VarcharType])
      assert(transformedSchema.fields(5).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.isInstanceOf[VarcharType])

      // Comments and non-nul handling has added to Spark since 3.0, so testing this fully starting from 3.0
      if (spark.version.split('.').head.toInt >= 3.0) {
        assert(actual.replace("`", "") == expected)
      }
    }
  }

  "removeNestedMetadata" should {
    "remove metadata, but only from nested fields" in {
      val metadata1 = new MetadataBuilder().putLong("maxLength", 5).putString("comment", "Employee name").build()
      val metadata2 = new MetadataBuilder().putLong("maxLength", 7).putString("comment", "Subordinate name").build()
      val metadata3 = new MetadataBuilder().putLong("maxLength", 15).putString("comment", "Address").build()
      val metadata4 = new MetadataBuilder().putLong("maxLength", 10).putString("comment", "Phone number").build()
      val metadata5 = new MetadataBuilder().putLong("maxLength", 1).putString("comment", "Some code").build()

      val schema1 = StructType(
        StructField("id", IntegerType, nullable = false) ::
          StructField("name", StringType, nullable = true, metadata = metadata1) ::
          StructField("subordinates", StructType(
            StructField("id", IntegerType, nullable = true) ::
              StructField("name", StringType, nullable = false, metadata = metadata2) :: Nil)
          ) ::
          StructField("addresses", ArrayType(StringType, containsNull = true), nullable = true, metadata = metadata3) ::
          StructField("phone_numbers", ArrayType(StructType(
            StructField("type", StringType, nullable = true) ::
              StructField("number", StringType, nullable = true, metadata = metadata4) :: Nil)
          )) ::
          StructField("matrix", ArrayType(ArrayType(StringType)), metadata = metadata5) ::
          Nil)

      val schema2 = StructType(
        StructField("id", IntegerType, nullable = true) ::
          StructField("name", StringType, nullable = false, metadata = metadata1) ::
          StructField("subordinates", StructType(
            StructField("id", IntegerType, nullable = true) ::
              StructField("name", StringType, nullable = false) :: Nil)
          ) ::
          StructField("addresses", ArrayType(StringType, containsNull = false), nullable = true, metadata = metadata3) ::
          StructField("phone_numbers", ArrayType(StructType(
            StructField("type", StringType, nullable = true) ::
              StructField("number", StringType, nullable = true) :: Nil)
          )) ::
          StructField("matrix", ArrayType(ArrayType(StringType)), metadata = metadata5) ::
          Nil)

      val transformedSchema1 = removeNestedMetadata(schema1)
      val transformedSchema2 = removeNestedMetadata(schema2)

      assert(transformedSchema1.toDDL == transformedSchema2.toDDL)

      assert(transformedSchema1.fields(1).metadata.contains("maxLength"))
      assert(!transformedSchema1.fields(2).dataType.asInstanceOf[StructType].fields(1).metadata.contains("maxLength"))
      assert(transformedSchema1.fields.head.nullable)
      assert(transformedSchema2.fields(3).nullable)
      assert(transformedSchema2.fields(3).dataType.asInstanceOf[ArrayType].containsNull)
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

  "showString" should {
    "produce a text version of a dataframe" in {
      val expected =
        """+---+---+
          ||a  |b  |
          |+---+---+
          ||A  |1  |
          ||B  |2  |
          ||C  |3  |
          |+---+---+
          |""".stripMargin

      val actual = showString(exampleDf)

      compareText(actual, expected)
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
