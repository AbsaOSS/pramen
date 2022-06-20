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

package za.co.absa.pramen.framework.job

import org.scalatest.WordSpec
import za.co.absa.pramen.api.metastore.MetastoreReader
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.mocks.metastore.MetastoreReaderMock
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.LocalDate

class MetastoreSchemaTransformerSuite extends WordSpec with SparkTestBase with TextComparisonFixture {
  private val infoDate = LocalDate.of(2022, 1 , 11)
  private val expectedSchema =
    """root
      | |-- id: integer (nullable = false)
      | |-- id2: integer (nullable = false)
      | |-- name2: string (nullable = true)""".stripMargin

  private val expectedData =
    """[ {
      |  "id" : 1,
      |  "id2" : 101,
      |  "name2" : "a1"
      |}, {
      |  "id" : 2,
      |  "id2" : 102,
      |  "name2" : "b1"
      |}, {
      |  "id" : 3,
      |  "id2" : 103,
      |  "name2" : "c1"
      |} ]""".stripMargin

  "getTable()" should {
    val mr = getTestCase

    val actual = mr.getTable("table1", None, None)

    "schema should match" in {
      val actualSchema = actual.schema.treeString

      compareText(actualSchema, expectedSchema)
    }

    "data should match" in {
      val actualData = SparkUtils.prettyJSON(actual.toJSON.collect().mkString("[", ",", "]"))

      compareText(actualData, expectedData)
    }
  }

  "getLatest()" should {
    val mr = getTestCase

    val actual = mr.getLatest("table1", None)

    "schema should match" in {
      val actualSchema = actual.schema.treeString

      compareText(actualSchema, expectedSchema)
    }

    "data should match" in {
      val actualData = SparkUtils.prettyJSON(actual.toJSON.collect().mkString("[", ",", "]"))

      compareText(actualData, expectedData)
    }
  }

  "getLatestAvailableDate()" in {
    val mr = getTestCase

    val date = mr.getLatestAvailableDate("table1", None)

    assert(date.contains(infoDate))
  }

  def getTestCase: MetastoreReader = {
    val metastoreReader = getMetastoreReaderMock

    val transformations = Seq(TransformExpression("id2", "id + 100"),
      TransformExpression("id3", "id + 1000"),
      TransformExpression("name2", "concat(name, '1')"),
      TransformExpression("id3", ""),
      TransformExpression("name", "drop")
    )

    new MetastoreSchemaTransformer(metastoreReader, Map("table1" -> transformations, "table2" -> transformations))
  }

  def getMetastoreReaderMock: MetastoreReader = {
    val df = spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "name")

    new MetastoreReaderMock(Seq("table1" -> df, "table2" -> df), infoDate)
  }
}
