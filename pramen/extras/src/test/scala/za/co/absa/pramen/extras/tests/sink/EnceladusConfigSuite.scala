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

package za.co.absa.pramen.extras.tests.sink

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.sink.EnceladusConfig

import java.time.ZoneId

class EnceladusConfigSuite extends AnyWordSpec with SparkTestBase {
  "fromConfig" should {
    "construct a config from minimal settings" in {
      val conf = ConfigFactory.parseString(
        s"""info.date.column = "ABC"
           |partition.pattern = "DEF"
           |format = "json"
           |mode = "append"
           |save.empty = false
           |
           |option {
           |  my.option1 = "1"
           |  my.option2 = "2"
           |}
           |info.file {
           |  generate = false
           |  source.application = "App1"
           |}
           |""".stripMargin)

      val enceladusConfig = EnceladusConfig.fromConfig(conf)

      assert(enceladusConfig.timezoneId == ZoneId.systemDefault())
      assert(enceladusConfig.infoDateColumn == "ABC")
      assert(enceladusConfig.partitionPattern == "DEF")
      assert(enceladusConfig.format == "json")
      assert(enceladusConfig.mode == "append")
      assert(enceladusConfig.formatOptions.size == 2)
      assert(enceladusConfig.formatOptions("my.option1") == "1")
      assert(enceladusConfig.formatOptions("my.option2") == "2")
      assert(!enceladusConfig.saveEmpty)
      assert(!enceladusConfig.generateInfoFile)
      assert(!enceladusConfig.preferAddPartition)
      assert(enceladusConfig.enceladusMainClass == EnceladusConfig.DEFAULT_ENCELADUS_RUN_MAIN_CLASS)
      assert(enceladusConfig.enceladusCmdLineTemplate == EnceladusConfig.DEFAULT_ENCELADUS_COMMAND_LINE_TEMPLATE)
      assert(enceladusConfig.publishPartitionPattern == "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}")
    }

    "construct a config with app version and timezone" in {
      val conf = ConfigFactory.parseString(
        s"""info.date.column = "ABC"
           |partition.pattern = "DEF"
           |format = "json"
           |mode = "append"
           |save.empty = false
           |prefer.add.partition = true
           |
           |enceladus.run.main.class = "A"
           |enceladus.command.line.template = "B"
           |publish.partition.template = "aaa"
           |
           |timezone = "Africa/Johannesburg"
           |
           |option {
           |  my.option1 = "1"
           |  my.option2 = "2"
           |}
           |info.file {
           |  generate = false
           |  source.application = "App1"
           |}
           |""".stripMargin)

      val enceladusConfig = EnceladusConfig.fromConfig(conf)

      assert(enceladusConfig.pramenVersion != "Unspecified")
      assert(enceladusConfig.timezoneId == ZoneId.of("Africa/Johannesburg"))
      assert(enceladusConfig.infoDateColumn == "ABC")
      assert(enceladusConfig.partitionPattern == "DEF")
      assert(enceladusConfig.format == "json")
      assert(enceladusConfig.mode == "append")
      assert(enceladusConfig.formatOptions.size == 2)
      assert(enceladusConfig.formatOptions("my.option1") == "1")
      assert(enceladusConfig.formatOptions("my.option2") == "2")
      assert(!enceladusConfig.saveEmpty)
      assert(!enceladusConfig.generateInfoFile)
      assert(enceladusConfig.preferAddPartition)
      assert(enceladusConfig.enceladusMainClass == "A")
      assert(enceladusConfig.enceladusCmdLineTemplate == "B")
      assert(enceladusConfig.publishPartitionPattern == "aaa")
    }
  }
}
