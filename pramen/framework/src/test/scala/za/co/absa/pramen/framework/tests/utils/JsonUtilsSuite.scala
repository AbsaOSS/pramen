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

import org.scalatest.WordSpec
import za.co.absa.pramen.framework.samples.SampleCaseClass
import za.co.absa.pramen.framework.utils.JsonUtils

class JsonUtilsSuite extends WordSpec {

  private val sample = SampleCaseClass.getDummy
  private val sampleJson =
    """{
      |  "strValue" : "String1",
      |  "intValue" : 100000,
      |  "longValue" : 10000000000,
      |  "dateValue" : "2020-08-10",
      |  "listStr" : [ "Str1", "Str2" ]
      |}""".stripMargin.replace("\r\n", "\n")

  "asJson" should {
    "return a JSON representation of a case class" in {
      val expectedJson = """{"strValue":"String1","intValue":100000,"longValue":10000000000,"dateValue":"2020-08-10","listStr":["Str1","Str2"]}"""

      val actualJson = JsonUtils.asJson(sample)

      assert(actualJson == expectedJson)
    }
  }

  "asJsonPretty" should {
    "return the number of milliseconds for values less than 1 second" in {
      val sample = SampleCaseClass.getDummy
      val actualJson = JsonUtils.asJsonPretty(sample)

      assert(actualJson == sampleJson)
    }
  }

  "fromJson" should {
    "return the original case class" in {
      val deserialized = JsonUtils.fromJson[SampleCaseClass](sampleJson)
      assert(deserialized == sample)
    }
  }

}
