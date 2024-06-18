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

package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.status.{RunStatus, TaskResult}
import za.co.absa.pramen.api.{ExternalChannelFactory, NotificationTarget, PipelineInfo}
import za.co.absa.pramen.core.notify.mq.{SingleMessageProducer, SingleMessageProducerKafka}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.Emoji._

class HyperdriveNotificationTarget(conf: Config,
                                   producer: SingleMessageProducer,
                                   topic: String) extends NotificationTarget {
  val TOKEN_KEY = "hyperdrive.token"

  private val log = LoggerFactory.getLogger(this.getClass)

  override def config: Config = conf

  override def connect(): Unit = {
    // This intentionally left blank. The connect for this notification target is only called if
    // conditions are met to send a notification.
  }

  override def sendNotification(pipelineInfo: PipelineInfo, notification: TaskResult): Unit = {
    if (notification.options.contains(TOKEN_KEY)) {
      val token = notification.options(TOKEN_KEY)

      if (notification.runStatus.isInstanceOf[RunStatus.Succeeded]) {
        log.info(s"Sending '$token' to the Hyperdrive Kafka topic: '$topic'...")
        producer.connect()
        producer.send(topic, token)
        log.info(s"$VOLTAGE Successfully send the notification topic to Kafka.")
      } else {
        log.info(s"Not sending '$token' to the Hyperdrive Kafka topic: '$topic' for the unsuccessful job...")
      }
    } else {
      log.warn(s"$WARNING Token is not configured for ${notification.outputTable.name}. Hyperdrive notification won't be sent. Please, set 'notification.$TOKEN_KEY' option for the job.")
    }
  }

  override def close(): Unit = producer.close()
}

object HyperdriveNotificationTarget extends ExternalChannelFactory[NotificationTarget] {
  val KAFKA_TOPIC_KEY = "kafka.topic"
  val KAFKA_OPTION_KEY = "kafka.option"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): HyperdriveNotificationTarget = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(KAFKA_TOPIC_KEY, KAFKA_OPTION_KEY))

    val topic = conf.getString(KAFKA_TOPIC_KEY)
    val kafkaConf = conf.getConfig(KAFKA_OPTION_KEY)

    val producer = new SingleMessageProducerKafka(conf, kafkaConf)

    new HyperdriveNotificationTarget(conf, producer, topic)
  }
}
