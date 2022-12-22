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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.samples.SampleCaseClass2
import za.co.absa.pramen.core.utils.{CsvUtils, SparkUtils}

class CsvUtilsSuite extends AnyWordSpec {
  "getHeaders" should {
    "return CSV headers from a case class" in {
      val expected = """strValue,intValue,longValue"""

      val actual = CsvUtils.getHeaders[SampleCaseClass2]()

      assert(actual == expected)
    }

    "return CSV headers with a custom separator" in {
      val expected = """strValue|intValue|longValue"""

      val actual = CsvUtils.getHeaders[SampleCaseClass2]('|')

      assert(actual == expected)
    }
  }

  "getStructType" should {
    "return a struct type from a case class" in {
      val expected = """{
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
                       |}""".stripMargin.replace("\r\n", "\n")

      val actual = SparkUtils.getStructType[SampleCaseClass2].prettyJson.replace("\r\n", "\n")

      assert(actual == expected)
    }
  }

  "getRecord" should {
    "return a csv record from an instance of a case class" in {
      val expected = "String1,100000,10000000000\n"
      val sample = SampleCaseClass2.getDummy

      val actual = CsvUtils.getRecord(sample)

      assert(actual == expected)
    }

    "return a csv record with a custom separator" in {
      val expected = "String1|100000|10000000000\n"
      val sample = SampleCaseClass2.getDummy

      val actual = CsvUtils.getRecord(sample, '|')

      assert(actual == expected)
    }
  }

}
