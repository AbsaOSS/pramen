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

package za.co.absa.pramen.tests.avro

import org.apache.spark.sql.functions.struct
import org.scalatest.WordSpec
import za.co.absa.pramen.NestedDataFrameFactory
import za.co.absa.pramen.avro.AvroUtils
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.utils.ResourceUtils.getResourceString
import za.co.absa.pramen.framework.utils.SparkUtils

class AvroUtilsSuite extends WordSpec with SparkTestBase with TextComparisonFixture {

  import spark.implicits._

  "convertSparkToAvroSchema" should {
    "convert basic schema with nullable values" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      val allColumns = struct(df.columns.map(c => df(c)): _*)

      val avro = AvroUtils.convertSparkToAvroSchema(allColumns.expr.dataType)

      val avroWithNullsFixed = AvroUtils.fixNullableFields(avro)

      val actualOrigin = avro.toString()
      val actualFixed = avroWithNullsFixed.toString()

      val expectedOrigin = """{"type":"record","name":"topLevelRecord","fields":[{"name":"a","type":["string","null"]},{"name":"b","type":"int"}]}"""

      val expectedFixed = """{"type":"record","name":"topLevelRecord","fields":[{"name":"a","type":["null","string"],"default":null},{"name":"b","type":"int"}]}"""

      assert(actualOrigin == expectedOrigin)
      assert(actualFixed == expectedFixed)
    }

    "convert nested schema with nullable values" in {
      val df = NestedDataFrameFactory.getNestedTestCase

      val allColumns = struct(df.columns.map(c => df(c)): _*)

      val avro = AvroUtils.convertSparkToAvroSchema(allColumns.expr.dataType)

      val avroWithNullsFixed = AvroUtils.fixNullableFields(avro)

      val actualOrigin = SparkUtils.prettyJSON(avro.toString())
      val actualFixed = SparkUtils.prettyJSON(avroWithNullsFixed.toString())

      val expectedOrigin = getResourceString("/test/nestedDf1_origin_avro.json")
      val expectedFixed = getResourceString("/test/nestedDf1_fixed_avro.json")

      compareText(actualOrigin, expectedOrigin)
      compareText(actualFixed, expectedFixed)
    }

    "convert nested schema with a map" in {
      val df = NestedDataFrameFactory.getMapTestCase

      val allColumns = struct(df.columns.map(c => df(c)): _*)

      val avro = AvroUtils.convertSparkToAvroSchema(allColumns.expr.dataType)

      val avroWithNullsFixed = AvroUtils.fixNullableFields(avro)

      val actualOrigin = SparkUtils.prettyJSON(avro.toString())
      val actualFixed = SparkUtils.prettyJSON(avroWithNullsFixed.toString())

      val expectedOrigin = getResourceString("/test/nestedMap_origin_avro.json")
      val expectedFixed = getResourceString("/test/nestedMap_fixed_avro.json")

      compareText(actualOrigin, expectedOrigin)
      compareText(actualFixed, expectedFixed)
    }
  }

}
