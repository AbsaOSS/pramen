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

package za.co.absa.pramen.core.transformers

import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{MetastoreReader, Reason}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.LocalDate

class IdentityTransformerSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture {

  import spark.implicits._

  private val infoDateWithData = LocalDate.of(2023, 1, 18)
  private val infoDateWithEmptyDf = LocalDate.of(2023, 1, 19)
  private val exampleDf = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  private val emptyDf = exampleDf.filter($"a" === "_")

  "validate()" should {
    "pass when the mandatory option is present" in {
      val (transformer, metastore) = getUseCase

      val outcome = transformer.validate(metastore, infoDateWithData, Map("input.table" -> "table1"))

      assert(outcome == Reason.Ready)
    }

    "pass when the legacy mandatory option is present" in {
      val (transformer, metastore) = getUseCase

      val outcome = transformer.validate(metastore, infoDateWithData, Map("table" -> "table1"))

      assert(outcome == Reason.Ready)
    }

    "pass when empty is allowed" in {
      val (transformer, metastore) = getUseCase

      val outcome = transformer.validate(metastore, infoDateWithEmptyDf, Map("table" -> "table1", "empty.allowed" -> "true"))

      assert(outcome == Reason.Ready)
    }

    "return SkipOnce when empty is not allowed" in {
      val (transformer, metastore) = getUseCase

      val outcome = transformer.validate(metastore, infoDateWithEmptyDf, Map("table" -> "table1", "empty.allowed" -> "false"))

      assert(outcome.isInstanceOf[Reason.SkipOnce])
    }

    "fail when the mandatory option is absent" in {
      val (transformer, metastore) = getUseCase

      val ex = intercept[IllegalArgumentException] {
        transformer.validate(metastore, infoDateWithData, Map.empty)
      }

      assert(ex.getMessage.contains("Option 'input.table' is not defined"))
    }
  }

  "run()" should {
    "return the expected output" in {
      val expected =
        """+---+---+
          ||a  |b  |
          |+---+---+
          ||A  |1  |
          ||B  |2  |
          ||C  |3  |
          |+---+---+
          |""".stripMargin
      val (transformer, metastore) = getUseCase

      val outcome = transformer.run(metastore, infoDateWithData, Map("table" -> "table1"))
        .orderBy("a")
      val actual = SparkUtils.showString(outcome)

      assert(outcome.count() == 3)
      compareText(actual, expected)
    }
  }

  def getUseCase: (IdentityTransformer, MetastoreReader) = {
    val metastoreReadeMock = mock(classOf[MetastoreReader])

    whenMock(metastoreReadeMock.getTable("table1")).thenReturn(exampleDf)
    whenMock(metastoreReadeMock.getTable("table1", Some(infoDateWithData), Some(infoDateWithData))).thenReturn(exampleDf)
    whenMock(metastoreReadeMock.getTable("table1", Some(infoDateWithEmptyDf), Some(infoDateWithEmptyDf))).thenReturn(emptyDf)

    (new IdentityTransformer(), metastoreReadeMock)
  }
}
