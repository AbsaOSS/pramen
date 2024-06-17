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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{RunStatus, TaskStatus}
import za.co.absa.pramen.core.TaskNotificationFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.mocks.notify.SingleMessageProducerSpy
import za.co.absa.pramen.core.notify.HyperdriveNotificationTarget

class HyperdriveNotificationTargetSuite extends AnyWordSpec with SparkTestBase {
  "apply()" should {
    val conf = ConfigFactory.parseString(
      s"""
         |  name = "hyperdrive1"
         |  factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
         |
         |  kafka.topic = "mytopic"
         |
         |  timeout.seconds = 10
         |
         |  kafka.option {
         |     bootstrap.servers = "dummy:9092,dummy:9093"
         |     max.block.ms = 2000
         |  }
         |""".stripMargin)

    "create a new object from config" in {
      val notificationTarget = HyperdriveNotificationTarget(conf, "", spark)

      assert(notificationTarget.isInstanceOf[HyperdriveNotificationTarget])
      assert(notificationTarget.config == conf)
    }
  }

  "connect()" should {
    "do nothing" in {
      val (notificationTarget, producer) = getUseCase

      notificationTarget.connect()

      assert(producer.connectInvoked == 0)
      assert(producer.sendInvoked == 0)
      assert(producer.closeInvoked == 0)
    }
  }

  "send()" should {
    "send a notification on success" in {
      val (notificationTarget, producer) = getUseCase
      val options = Map("hyperdrive.token" -> "ABC")

      val taskNotification = TaskNotificationFactory.getDummyTaskNotification(options = options)

      notificationTarget.sendNotification(null, taskNotification)

      assert(producer.connectInvoked == 1)
      assert(producer.sendInvoked == 1)
      assert(producer.closeInvoked == 0)
      assert(producer.lastTopicName == "dummy_topic")
      assert(producer.lastMessage == "ABC")
    }

    "don't send a notification if the task has is not succeeded" in {
      val (notificationTarget, producer) = getUseCase
      val options = Map("hyperdrive.token" -> "ABC")

      val taskNotification = TaskNotificationFactory.getDummyTaskNotification(status = RunStatus.Skipped("dummy"),  options = options)

      notificationTarget.sendNotification(null, taskNotification)

      assert(producer.connectInvoked == 0)
      assert(producer.sendInvoked == 0)
    }

    "don't send a notification if a notification token is not defined" in {
      val (notificationTarget, producer) = getUseCase

      val taskNotification = TaskNotificationFactory.getDummyTaskNotification()

      notificationTarget.sendNotification(null, taskNotification)

      assert(producer.connectInvoked == 0)
      assert(producer.sendInvoked == 0)
    }
  }

  "close()" should {
    "close the producer" in {
      val (notificationTarget, producer) = getUseCase
      val options = Map("hyperdrive.token" -> "ABC")

      val taskNotification = TaskNotificationFactory.getDummyTaskNotification(options = options)

      notificationTarget.sendNotification(null, taskNotification)
      notificationTarget.close()

      assert(producer.connectInvoked == 1)
      assert(producer.sendInvoked == 1)
      assert(producer.closeInvoked == 1)
      assert(producer.lastTopicName == "dummy_topic")
      assert(producer.lastMessage == "ABC")
    }
  }

  private def getUseCase: (HyperdriveNotificationTarget, SingleMessageProducerSpy) = {
    val conf = ConfigFactory.empty()
    val producer = new SingleMessageProducerSpy
    val notificationTarget = new HyperdriveNotificationTarget(conf, producer, "dummy_topic")

    (notificationTarget, producer)
  }
}
