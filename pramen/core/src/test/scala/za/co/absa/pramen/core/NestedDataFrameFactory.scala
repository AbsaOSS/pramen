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

package za.co.absa.pramen.core

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object NestedDataFrameFactory {
  val testCaseSchema: StructType = StructType(
    Array(
      StructField("id", LongType),
      StructField("key1", LongType),
      StructField("key2", LongType),
      StructField("struct1", StructType(Array(
        StructField("key3", IntegerType),
        StructField("key4", IntegerType)
      ))),
      StructField("struct2", StructType(Array(
        StructField("inner1", StructType(Array(
          StructField("key5", LongType),
          StructField("key6", LongType),
          StructField("skey1", StringType)
        )))
      ))),
      StructField("struct3", StructType(Array(
        StructField("inner3", StructType(Array(
          StructField("array3", ArrayType(StructType(Array(
            StructField("a1", LongType),
            StructField("a2", LongType),
            StructField("a3", StringType)
          ))))
        )))
      ))),
      StructField("array1", ArrayType(StructType(Array(
        StructField("key7", LongType),
        StructField("key8", LongType),
        StructField("skey2", StringType)
      )))),
      StructField("array2", ArrayType(StructType(Array(
        StructField("key2", LongType),
        StructField("inner2", ArrayType(StructType(Array(
          StructField("key9", LongType),
          StructField("key10", LongType),
          StructField("struct3", StructType(Array(
            StructField("k1", IntegerType),
            StructField("k2", IntegerType)
          )))
        ))))
      ))))
    ))

  private val testCaseWithMapSchema = new StructType()
    .add("name", StringType)
    .add("addresses", ArrayType(new StructType()
      .add("city", StringType)
      .add("state", StringType)))
    .add("properties", MapType(StringType, StringType))

  private val tstCaseWithMapData = Seq(
    Row("John", List(Row("Newark", "NY"), Row("Brooklyn", "NY")), Map("hair" -> "black", "eyes" -> "brown", "height" -> "178")),
    Row("Kate", List(Row("San Jose", "CA"), Row("Sandiago", "CA")), Map("hair" -> "brown", "eyes" -> "black", "height" -> "165")),
    Row("William", List(Row("Las Vegas", "NV")), Map("hair" -> "red", "eye" -> "gray", "height" -> "185")),
    Row("Sarah", null, Map("hair" -> "blond", "eyes" -> "red", "height" -> "162")),
    Row("Michael", List(Row("Sacramento", "CA"), Row("San Diego", "CA")), Map("white" -> "black", "eyes" -> "black", "height" -> "180"))
  )

  def getNestedTestCase(implicit spark: SparkSession): DataFrame = {
    spark.read
      .schema(testCaseSchema)
      .json(getClass.getResource("/test/nestedDf1.json").getPath)
  }

  def getMapTestCase(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(tstCaseWithMapData),
      testCaseWithMapSchema
    ).orderBy("name")
  }
}
