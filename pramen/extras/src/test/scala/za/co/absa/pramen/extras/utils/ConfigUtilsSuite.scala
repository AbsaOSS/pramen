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

package za.co.absa.pramen.extras.utils

import com.typesafe.config.ConfigException.WrongType
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.fixtures.{TempDirFixture, TextComparisonFixture}

import java.util
import scala.collection.JavaConverters._

class ConfigUtilsSuite extends AnyWordSpec with TempDirFixture with TextComparisonFixture {
  private val testConfig = ConfigFactory.parseResources("test/config/testconfig.conf").resolve()
  private val keysToRedact = Set("mytest.password", "no.such.key")

  "getOptionInt()" should {
    "return a long value" in {
      val v = ConfigUtils.getOptionInt(testConfig, "mytest.int.value")
      assert(v.isDefined)
      assert(v.get == 2000000)
    }

    "return None when key is not found" in {
      val v = ConfigUtils.getOptionInt(testConfig, "mytest.int.bogus")
      assert(v.isEmpty)
    }

    "throw WrongType exception if the value has a wrong type" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getOptionInt(testConfig, "mytest.str.value")
      }
      assert(ex.getMessage.contains("has type STRING rather than NUMBER"))
    }
  }

  "getOptionString" should {
    "return a string value for a string type" in {
      val v = ConfigUtils.getOptionString(testConfig, "mytest.str.value")
      assert(v.isDefined)
      assert(v.get == "Hello")
    }

    "return a string value for a long type" in {
      val v = ConfigUtils.getOptionString(testConfig, "mytest.long.value")
      assert(v.isDefined)
      assert(v.get == "1000000000000")
    }

    "return a string value for a date type" in {
      val v = ConfigUtils.getOptionString(testConfig, "mytest.date.value")
      assert(v.isDefined)
      assert(v.get == "2020-08-10")
    }

    "return None when ke is not found" in {
      val v = ConfigUtils.getOptionString(testConfig, "mytest.str.bogus")
      assert(v.isEmpty)
    }

    "throw WrongType exception if the value has a wrong type" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getOptionString(testConfig, "mytest.array")
      }
      assert(ex.getMessage.contains("has type LIST rather than STRING"))
    }
  }

  "getRedactedConfig()" should {
    "be able to redact input config" in {
      val redacted = ConfigUtils.getRedactedConfig(testConfig, keysToRedact)

      assert(redacted.getString("mytest.password") == "[redacted]")
    }
  }

  "getFlatConfig()" should {
    "flatten the config" in {
      val flat = ConfigUtils.getFlatConfig(testConfig)

      assert(flat("mytest.password") == "xyz")
      assert(flat("mytest.days.ok").asInstanceOf[util.ArrayList[Int]].asScala.toList == List(1, 2, 3))
    }
  }

  "getRedactedFlatConfig()" should {
    "redact keys containing the list of tokens" in {
      val flat = ConfigUtils.getRedactedFlatConfig(ConfigUtils.getFlatConfig(testConfig),
        Set("extra", "password"))

      assert(flat("mytest.password") == "[redacted]")
      assert(flat("mytest.extra.options.value1") == "[redacted]")
      assert(flat("mytest.extra.options.value2") == "[redacted]")
      assert(flat("mytest.int.value").toString == "2000000")
      assert(flat("mytest.days.ok").asInstanceOf[util.ArrayList[Int]].asScala.toList == List(1, 2, 3))
    }
  }

  "getRedactedValue()" should {
    "redact keys containing the list of tokens" in {
      val tokens = Set("secret", "password", "session.token")

      assert(ConfigUtils.getRedactedValue("mytest.password", "pwd", tokens) == "[redacted]")
      assert(ConfigUtils.getRedactedValue("mytest.secret", "pwd", tokens) == "[redacted]")
      assert(ConfigUtils.getRedactedValue("mytest.session.token", "pwd", tokens) == "[redacted]")
      assert(ConfigUtils.getRedactedValue("mytest.session.name", "name", tokens) == "name")
    }
  }

  "getOptListStrings()" should {
    "return a list if it is set" in {
      val list = ConfigUtils.getOptListStrings(testConfig, "mytest.list.str")

      assert(list.nonEmpty)
      assert(list == Seq("A", "B", "C"))
    }

    "return a list of strings even if elements are values" in {
      val list = ConfigUtils.getOptListStrings(testConfig, "mytest.array")

      assert(list.nonEmpty)
      assert(list == Seq("5", "10", "7", "4"))
    }

    "return an empty list if no such key" in {
      val list = ConfigUtils.getOptListStrings(testConfig, "mytest.dummy")

      assert(list.isEmpty)
    }

    "throw WrongType exception if a wrong type of value is set" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getOptListStrings(testConfig, "mytest.password")
      }
      assert(ex.getMessage.contains("has type STRING rather than LIST"))
    }
  }

  "getExtraOptions()" should {
    "return a new config if the prefix path exists" in {
      val map = ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options")

      assert(map.size == 2)
      assert(map("value1") == "value1")
      assert(map("value2") == "100")
    }

    "return an empty map if no such key" in {
      val map = ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options.dummy")

      assert(map.isEmpty)
    }

    "return arrays as strings if extra options contain lists" in {
      val map = ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options2")

      assert(map.size == 3)
      assert(map("value1") == "value1")
      assert(map("value2") == "100")
      assert(map("value3") == "[10, 5, 7, 4]")
    }

    "throw WrongType exception if the path is not a config" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options.value1")
      }
      assert(ex.getMessage.contains("has type STRING rather than OBJECT"))
    }
  }

  "validatePathsExistence()" should {
    "pass if all required paths exist" in {
      ConfigUtils.validatePathsExistence(testConfig, "", "mytest.long.value" :: Nil)
    }

    "throw an exception if a mandatory key is missing" in {
      val ex = intercept[IllegalArgumentException] {
        ConfigUtils.validatePathsExistence(testConfig, "", "mytest.bogus.value" :: Nil)
      }
      assert(ex.getMessage.contains("Mandatory configuration options are missing: mytest.bogus.value"))
    }
  }
}

