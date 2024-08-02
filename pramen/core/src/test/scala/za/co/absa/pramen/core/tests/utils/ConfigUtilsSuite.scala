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

import com.typesafe.config.ConfigException.{Missing, WrongType}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.utils.ConfigUtils

import java.nio.file.{Files, Paths}
import java.time.format.DateTimeParseException
import java.time.{DateTimeException, DayOfWeek, LocalDate}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ConfigUtilsSuite extends AnyWordSpec with TempDirFixture with TextComparisonFixture {
  private val testConfig = ConfigFactory.parseResources("test/config/testconfig.conf").resolve()
  private val testMetastoreConfig = ConfigFactory.parseResources("test/config/metastore.conf")
  private val dateFormat = "yyyy-MM-dd"
  private val keysToRedact = Set("mytest.password", "no.such.key")

  "getOptionLong()" should {
    "return a long value" in {
      val v = ConfigUtils.getOptionLong(testConfig, "mytest.long.value")
      assert(v.isDefined)
      assert(v.get == 1000000000000L)
    }

    "return None when key is not found" in {
      val v = ConfigUtils.getOptionLong(testConfig, "mytest.long.bogus")
      assert(v.isEmpty)
    }

    "throw WrongType exception if the value has a wrong type" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getOptionLong(testConfig, "mytest.str.value")
      }
      assert(ex.getMessage.contains("has type STRING rather than NUMBER"))
    }
  }

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

  "getOptionConfigList" should {
    "return an empty seq if the key not found" in {
      val v = ConfigUtils.getOptionConfigList(testConfig, "mytest.array.bogus")
      assert(v.isEmpty)
    }

    "return a seq of config values for a single object array" in {
      val configList = ConfigUtils.getOptionConfigList(testConfig, "mytest.object.array")

      assert(configList.length == 3)
      assert(configList.head.getString("name") == "a")
      assert(configList(1).getString("name") == "b")
      assert(configList(2).getString("name") == "c")
    }

    "return a seq of config values for multiple object arrays" in {
      val conf1 = ConfigFactory.parseString(
        """my.array.1 = [
          |  { name = "a", id = 1 },
          |  { name = "b", id = 2 },
          |  { name = "c", id = 3 }
          |]
          |""".stripMargin)
      val conf2 = ConfigFactory.parseString(
        """my.array.2 = [
          |  { name = "d", id = 4 },
          |  { name = "e", id = 5 },
          |  { name = "f", id = 6 }
          |]
          |""".stripMargin)
      val conf3 = ConfigFactory.parseString(
        """my.array.3 = [
          |  { name = "g", id = 7 },
          |  { name = "h", id = 8 },
          |  { name = "i", id = 9 }
          |]
          |""".stripMargin)

      // Even with the gap list or a non-numeric section, will not be skipped
      val conf4 = ConfigFactory.parseString(
        """my.array.abs = [
          |  { name = "q", id = 10 }
          |]
          |""".stripMargin)

      val configList = ConfigUtils.getOptionConfigList(conf1.withFallback(conf2).withFallback(conf3).withFallback(conf4), "my.array")

      assert(configList.length == 10)
      assert(configList.head.getString("name") == "a")
      assert(configList(1).getString("name") == "b")
      assert(configList(2).getString("name") == "c")
      assert(configList(3).getString("name") == "d")
      assert(configList(4).getString("name") == "e")
      assert(configList(5).getString("name") == "f")
      assert(configList(6).getString("name") == "g")
      assert(configList(7).getString("name") == "h")
      assert(configList(8).getString("name") == "i")
      assert(configList(9).getString("name") == "q")

      assert(configList.head.getInt("id") == 1)
      assert(configList(1).getInt("id") == 2)
      assert(configList(2).getInt("id") == 3)
      assert(configList(3).getInt("id") == 4)
      assert(configList(4).getInt("id") == 5)
      assert(configList(5).getInt("id") == 6)
      assert(configList(6).getInt("id") == 7)
      assert(configList(7).getInt("id") == 8)
      assert(configList(8).getInt("id") == 9)
      assert(configList(9).getInt("id") == 10)
    }
  }

  "getDate" should {
    "return a date value when a date field is specified" in {
      val v = ConfigUtils.getDate(testConfig, "mytest.date.value", dateFormat)
      assert(v == LocalDate.of(2020, 8, 10))
    }

    "throw Missing exception when key is not found" in {
      val ex = intercept[Missing] {
        ConfigUtils.getDate(testConfig, "mytest.date.bogus", dateFormat)
      }
      assert(ex.getMessage.contains("No configuration setting found for key"))
    }

    "throw WrongType exception if the value has a wrong type" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getDate(testConfig, "mytest.array", dateFormat)
      }
      assert(ex.getMessage.contains("has type LIST rather than STRING"))
    }

    "throw parsing exception if the date format is wrong" in {
      val ex = intercept[DateTimeParseException] {
        ConfigUtils.getDate(testConfig, "mytest.str.value", dateFormat)
      }
      assert(ex.getMessage.contains("Text 'Hello' could not be parsed"))
    }
  }

  "getDateOpt" should {
    "return a date value for a date type" in {
      val v = ConfigUtils.getDateOpt(testConfig, "mytest.date.value", dateFormat)
      assert(v.isDefined)
      assert(v.get == LocalDate.of(2020, 8, 10))
    }

    "return None when ke is not found" in {
      val v = ConfigUtils.getDateOpt(testConfig, "mytest.date.bogus", dateFormat)
      assert(v.isEmpty)
    }

    "throw WrongType exception if the value has a wrong type" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getDateOpt(testConfig, "mytest.array", dateFormat)
      }
      assert(ex.getMessage.contains("has type LIST rather than STRING"))
    }

    "throw parsing exception if the date format is wrong" in {
      val ex = intercept[DateTimeParseException] {
        ConfigUtils.getDateOpt(testConfig, "mytest.str.value", dateFormat)
      }
      assert(ex.getMessage.contains("Text 'Hello' could not be parsed"))
    }
  }

  "getDaysOfWeek" should {
    "return days of week when proper days of week are specified" in {
      val v = ConfigUtils.getDaysOfWeek(testConfig, "mytest.days.ok")
      assert(v == List(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY))
    }

    "throw Missing exception when key is not found" in {
      val ex = intercept[Missing] {
        ConfigUtils.getDaysOfWeek(testConfig, "mytest.days.bogus")
      }
      assert(ex.getMessage.contains("No configuration setting found for key"))
    }

    "throw WrongType exception if a string is passed" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getDaysOfWeek(testConfig, "mytest.str.value")
      }
      assert(ex.getMessage.contains("has type STRING rather than LIST"))
    }

    "throw WrongType exception if a list of strings is passed" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getDaysOfWeek(testConfig, "mytest.list.str")
      }
      assert(ex.getMessage.contains("has type list of STRING rather than list of NUMBER"))
    }

    "throw parsing exception if day of week is too small" in {
      val ex = intercept[DateTimeException] {
        ConfigUtils.getDaysOfWeek(testConfig, "mytest.days.wrong1")
      }
      assert(ex.getMessage.contains("Invalid value for DayOfWeek: 0"))
    }

    "throw parsing exception if day of week is too large" in {
      val ex = intercept[DateTimeException] {
        ConfigUtils.getDaysOfWeek(testConfig, "mytest.days.wrong2")
      }
      assert(ex.getMessage.contains("Invalid value for DayOfWeek: 8"))
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

  "getFlatConfigOfPrimitiveValues()" should {
    "flatten the config including the arrays and objects" in {
      val flat = ConfigUtils.getFlatConfigOfPrimitiveValues(testConfig)

      assert(flat("mytest.password") == "xyz")
      assert(flat("mytest.days.ok").asInstanceOf[util.ArrayList[Int]].asScala.toList == List(1, 2, 3))
      assert(flat("mytest.object.array[0].numbers").asInstanceOf[util.ArrayList[Int]].asScala.toList == List(1, 2, 3))
      assert(flat("mytest.object.array[0].options[1].opt2") == 200)
      assert(flat("mytest.object.array[1].name") == "b")
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

  "renderRedactedKeyValue()" should {
    "redact keys containing the list of tokens" in {
      val tokens = Set("secret", "password", "session.token")

      assert(ConfigUtils.renderRedactedKeyValue("mytest.password", "pwd", tokens) == "mytest.password = [redacted]")
      assert(ConfigUtils.renderRedactedKeyValue("mytest.secret", "pwd", tokens) == "mytest.secret = [redacted]")
      assert(ConfigUtils.renderRedactedKeyValue("mytest.session.token", "pwd", tokens) == "mytest.session.token = [redacted]")
      assert(ConfigUtils.renderRedactedKeyValue("mytest.session.name", "name", tokens) == "mytest.session.name = \"name\"")
    }
  }

  "convertToMap()" should {
    "convert a simple config" in {
      val conf = ConfigFactory.parseString(
        s"""
           |a = 2
           |b = 3
           |c = "string"
           |""".stripMargin
      )

      val map = ConfigUtils.convertToMap(conf)
      val expectedMap = Map(
        "a" -> 2,
        "b" -> 3,
        "c" -> "string"
      )
      assert(map == expectedMap)
    }

    "convert a complex config" in {
      val conf = ConfigFactory.parseString(
        s"""
           |this {
           |  field {
           |    is {
           |      a_list = [
           |        1,
           |        "2",
           |        [3, 4],
           |        { "5" = "6" }
           |      ]
           |      and_a_simple_value = 3
           |    }
           |  }
           |}
           |another_field = 2
           |""".stripMargin
      )

      val map = ConfigUtils.convertToMap(conf)

      val expectedMap = Map(
        "this" -> Map(
          "field" -> Map(
            "is" -> Map (
              "a_list" -> Seq(
                1,
                "2",
                Seq(3, 4),
                Map("5" -> "6")
              ),
              "and_a_simple_value" -> 3
            )
          )
        ),
        "another_field" -> 2
      )
      assert(map == expectedMap)
    }
  }

  "setSystemPropertyStringFallback()" should {
    "set nothing if a value is already set" in {
      System.setProperty("mytest.str.value", "test2")

      ConfigUtils.setSystemPropertyStringFallback(testConfig, "mytest.str.value")

      assert(System.getProperty("mytest.str.value") == "test2")
    }

    "set value if not set" in {
      System.clearProperty("mytest.str.value")

      ConfigUtils.setSystemPropertyStringFallback(testConfig, "mytest.str.value")

      assert(System.getProperty("mytest.str.value") == "Hello")
    }
  }


  "setSystemPropertyFileFallback()" should {
    "set nothing if a value is already set" in {
      System.setProperty("mytest.str.value", "test2")

      ConfigUtils.setSystemPropertyFileFallback(testConfig, "mytest.str.value")

      assert(System.getProperty("mytest.str.value") == "test2")
    }

    "set the path to the file if path exists" in {
      withTempDirectory("conf1")(dir => {
        val filePath = Paths.get(dir, "test1.tmp")
        val filePathStr = filePath.toAbsolutePath.toString
        System.clearProperty("file.path")
        Files.createFile(filePath)

        val conf2 = testConfig.withValue("file.path", ConfigValueFactory.fromAnyRef(filePathStr))

        ConfigUtils.setSystemPropertyFileFallback(conf2, "file.path")

        assert(System.getProperty("file.path") == filePathStr)
      })
    }

    "set the path to the file if it is in the current directory" in {
      val filePath = Paths.get("test2.tmp")
      val filePathStr = "/path/does/not/exist/test2.tmp"
      System.clearProperty("file.path")
      Files.createFile(filePath)
      val conf2 = testConfig.withValue("file.path", ConfigValueFactory.fromAnyRef(filePathStr))

      ConfigUtils.setSystemPropertyFileFallback(conf2, "file.path")

      Files.delete(filePath)

      assert(System.getProperty("file.path") == "test2.tmp")
    }

    "set nothing if cannot find the file" in {
      val filePathStr = "/path/does/not/exist"
      System.clearProperty("file.path")

      val conf2 = testConfig.withValue("file.path", ConfigValueFactory.fromAnyRef(filePathStr))

      ConfigUtils.setSystemPropertyFileFallback(conf2, "file.path")

      assert(System.getProperty("file.path") == null)
    }
  }

  "getListStringsByPrefix" should {
    "return a list if it is set" in {
      val list = ConfigUtils.getListStringsByPrefix(testConfig, "mytest.str.list.item")

      assert(list == Seq("a", "b", "c"))
    }

    "return an empty list if it is not set" in {
      val list = ConfigUtils.getListStringsByPrefix(testConfig, "mytest.str.list.no.item")

      assert(list.isEmpty)
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

    "return a new config even if the prefix is empty" in {
      val conf = ConfigFactory.parseString(
        """
          |value1 = "value1"
          |value2 = 100
          |""".stripMargin)
      val map = ConfigUtils.getExtraOptions(conf, "")

      assert(map.size == 2)
      assert(map("value1") == "value1")
      assert(map("value2") == "100")
    }

    "return a new map if the prefix path exists" in {
      val map = ConfigUtils.getExtraOptions(
        Map[String, String]("mytest.extra.options.value1" -> "value1", "mytest.extra.options.value2" -> "100"),
        "mytest.extra.options")

      assert(map.size == 2)
      assert(map("value1") == "value1")
      assert(map("value2") == "100")
    }

    "return an empty map if no such key" in {
      val map = ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options.dummy")

      assert(map.isEmpty)
    }

    "return an empty map if the input map is empty" in {
      val map = ConfigUtils.getExtraOptions(Map.empty[String, String], "a.b")

      assert(map.isEmpty)
    }

    "return arrays as strings if extra options contain lists" in {
      val map = ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options2")

      assert(map.size == 3)
      assert(map("value1") == "value1")
      assert(map("value2") == "100")
      assert(map("value3") == "[10, 5, 7, 4]")
    }

    "work with list of objects" in {
      val conf = ConfigFactory.parseString(
        """obj {
          |  test = [
          |    {
          |      data = "abc.dat"
          |      control = "abc.ctrl"
          |    },
          |    {
          |      data = "cde.txt"
          |      control = "cde.ctrl"
          |    }
          |  ]
          |}
          |""".stripMargin)

      val map = ConfigUtils.getExtraOptions(conf, "obj")

      assert(map("test") == """[{"control":"abc.ctrl","data":"abc.dat"},{"control":"cde.ctrl","data":"cde.txt"}]""")

      val convertedBackConfig = ConfigFactory.parseString(s"""test = ${map("test")}""")

      val configList = convertedBackConfig.getConfigList("test")

      assert(configList.size() == 2)
      assert(configList.get(0).getString("data") == "abc.dat")
      assert(configList.get(0).getString("control") == "abc.ctrl")
      assert(configList.get(1).getString("data") == "cde.txt")
      assert(configList.get(1).getString("control") == "cde.ctrl")
    }

    "throw WrongType exception if the path is not a config" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getExtraOptions(testConfig, "mytest.extra.options.value1")
      }
      assert(ex.getMessage.contains("has type STRING rather than OBJECT"))
    }
  }

  "getConfig()" should {
    "return a new config if the prefix path exists" in {
      val extraConf = ConfigUtils.getExtraConfig(testConfig, "mytest.extra.options")

      assert(extraConf.hasPath("value1"))
      assert(extraConf.hasPath("value2"))
      assert(extraConf.getString("value1") == "value1")
      assert(extraConf.getLong("value2") == 100L)
    }

    "return an empty config if no such key" in {
      val extraConf = ConfigUtils.getExtraConfig(testConfig, "mytest.extra.options.dummy")

      assert(extraConf.isEmpty)
    }

    "throw WrongType exception if the path is not a config" in {
      val ex = intercept[WrongType] {
        ConfigUtils.getExtraConfig(testConfig, "mytest.extra.options.value1")
      }
      assert(ex.getMessage.contains("has type STRING rather than OBJECT"))
    }
  }

  "renderEffectiveConfigProps" should {
    "convert a config to a properties list" in {
      val actual = ConfigUtils.renderEffectiveConfigProps(testConfig, Set("mytest.int.value"), Set("password"))

      assert(actual.contains("mytest.date.value = 2020-08-10"))
      assert(actual.contains("mytest.extra.options2.value3 = [10, 5, 7, 4]"))
      assert(actual.contains("mytest.object.array[0].options[0].opt1 = a"))
      assert(actual.contains("mytest.matrix[0] = [1, 2, 3]"))
      assert(actual.contains("mytest.int.value = [redacted]"))
      assert(actual.contains("mytest.password = [redacted]"))
    }
  }

  "renderEffectiveConfigHocon" should {
    "convert a config to a hocon string" in {
      val actual = ConfigUtils.renderEffectiveConfigHocon(testConfig, Set("mytest.int.value"))

      assert(actual.contains("pointer=2000000"))
      assert(actual.contains("value=\"[redacted]\""))
    }
  }

  "renderExtraOptions" should {
    "redact keys that contain words to redact" in {
      val map = Map("key1" -> "value1", "MyPassword" -> "value2", "SECRET" -> "value3")

      val rendered = new ListBuffer[String]
      ConfigUtils.renderExtraOptions(map, Set("password", "secret"))(s => {
        rendered += s
      })

      assert(rendered.size == 3)
      assert(rendered.head == "key1 = \"value1\"")
      assert(rendered(1) == "MyPassword = [redacted]")
      assert(rendered(2) == "SECRET = [redacted]")
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

  "toProperties()" should {
    "convert a config to properties" in {
      val prop = ConfigUtils.toProperties(testConfig)

      assert(prop.get("mytest.long.value") == "1000000000000")
      assert(prop.get("mytest.str.value") == "Hello")
      assert(prop.get("mytest.date.value") == "2020-08-10")
      assert(prop.get("mytest.days.ok") == "[1, 2, 3]")
    }
  }

  "toYaml" should {
    "convert a config to yaml" in {
      val expectedYaml =
      """mytest:
        |  array: [ 5, 10, 7, 4 ]
        |  date.value: 2020-08-10
        |  days:
        |    ok: [ 1, 2, 3 ]
        |    wrong1: [ 0 ]
        |    wrong2: [ 8 ]
        |  double.value: 3.14159265
        |  extra:
        |    options:
        |      value1: value1
        |      value2: 100
        |    options2:
        |      value1: value1
        |      value2: 100
        |      value3: [ 10, 5, 7, 4 ]
        |  int:
        |    pointer: 2000000
        |    value: 2000000
        |  list.str: [ A, B, C ]
        |  long.value: 1000000000000
        |  matrix:
        |  - [ 1, 2, 3 ]
        |  - [ 4, 5, 6 ]
        |  - [ 7, 8, 9 ]
        |  object.array:
        |  - name: a
        |    numbers: [ 1, 2, 3 ]
        |    options:
        |    - opt1: a
        |      opt2: 100
        |    - opt1: c
        |      opt2: 200
        |    value: 1
        |  - name: b
        |    value: 2
        |  - name: c
        |    value: 3
        |  password: xyz
        |  str:
        |    list.item:
        |      1: a
        |      2: b
        |      3: c
        |    value: Hello
        |  string:
        |    quoted: "This is a \"test\" ! \n"
        |    special: "This is a 'test'"""".stripMargin

      val actualYaml = ConfigUtils.toYaml(testConfig)

      compareText(actualYaml, expectedYaml)
    }

    "convert a metastore config to yaml" in {
      val expectedYaml =
        """default:
          |  info.date:
          |    column: pramen_info_date
          |    format: yyyy-MM-dd
          |  records.per.partition: 100000
          |pramen.metastore.tables:
          |- description: "Table 1 description"
          |  format: parquet
          |  information.date:
          |    column: INFORMATION_DATE
          |    format: yyyy-MM-dd
          |    start: 2017-01-31
          |  name: table1_sync
          |  path: /tmp/dummy/table1
          |  records.per.partition: 1000000
          |- description: "Table 2 description"
          |  format: delta
          |  name: table2_sync
          |  path: /tmp/dummy/table2
          |- description: "Output table"
          |  format: delta
          |  name: table_out
          |  path: /tmp/dummy/table_out
          |run.transformers:
          |- info.date: 2022-02-14
          |  output.table: table_out1
          |  transformer.class: some.my.dummy.MyClass1
          |- info.date: 2022-02-15
          |  output.table: table_out2
          |  transformer.class: some.my.dummy.MyClass2
""".stripMargin
      val actualYaml = ConfigUtils.toYaml(testMetastoreConfig)

      compareText(actualYaml, expectedYaml)
    }
  }


}
