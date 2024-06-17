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

package za.co.absa.pramen.core.tests.notify

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.notify.{HyperdriveNotificationTarget, NotificationTargetManager}

class NotificationTargetManagerSuite extends AnyWordSpec with SparkTestBase {
  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen.notification.targets = [
       |    {
       |      name = "hyperdrive1"
       |      factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
       |
       |      kafka.topic = "mytopic"
       |
       |      timeout.seconds = 10
       |
       |      kafka.option {
       |         bootstrap.servers = "dummy:9092,dummy:9093"
       |         sasl.mechanism = "GSSAPI"
       |         security.protocol = "SASL_SSL"
       |      }
       |    }
       |  ]
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "getByName()" should {
    "return a notification target" in {
      val nt = NotificationTargetManager.getByName("hyperdrive1", conf, None)

      assert(nt.isInstanceOf[HyperdriveNotificationTarget])
    }

    "throw an exception if the notification target does not exist" in {
      assertThrows[IllegalArgumentException] {
        NotificationTargetManager.getByName("nonexistent", conf, None)
      }
    }
  }
}
