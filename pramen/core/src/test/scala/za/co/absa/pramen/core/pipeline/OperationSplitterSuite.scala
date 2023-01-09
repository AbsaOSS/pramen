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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.mocks.notify.NotificationTargetSpy

class OperationSplitterSuite extends AnyWordSpec with SparkTestBase {
  private val appConfig: Config = ConfigFactory.parseString(
    s"""
       | pramen.notification.targets = [
       |    {
       |      name = "hyperdrive1"
       |      factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
       |
       |      kafka.topic = "mytopic"
       |
       |      kafka.option {
       |         bootstrap.servers = "dummy:9092,dummy:9093"
       |      }
       |    },
       |    {
       |      name = "custom1"
       |      factory.class = "za.co.absa.pramen.core.mocks.notify.NotificationTargetSpy"
       |
       |      my.config1 = "mykey1"
       |      my.config2 = "mykey2"
       |    },
       |  ]
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  private val tableConf: Config = ConfigFactory.parseString(
    s"""notification.target {
       |       my.config2 = "mykey22"
       |       my.config3 = "mykey33"
       |}
       |notification {
       |  token = "AA"
       |}
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "getNotificationTargets()" should {
    "get notification targets for a table config" in {
      val jobTarget = OperationSplitter.getNotificationTarget(appConfig, "custom1", tableConf)

      assert(jobTarget.name == "custom1")
      assert(jobTarget.target.isInstanceOf[NotificationTargetSpy])
      assert(jobTarget.target.config.getString("my.config1") == "mykey1")
      assert(jobTarget.target.config.getString("my.config2") == "mykey22")
      assert(jobTarget.target.config.getString("my.config3") == "mykey33")
      assert(jobTarget.options.contains("token"))
      assert(jobTarget.options("token") == "AA")
    }

    "throws an exception if the target does not exist" in {
      assertThrows[IllegalArgumentException] {
        OperationSplitter.getNotificationTarget(appConfig, "does_not_exist", tableConf)
      }
    }
  }
}