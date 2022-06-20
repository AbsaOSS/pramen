/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.mq

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class MqNotifierKafka(topic: String, token: String, kafkaConfig: Config) extends MqNotifier {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val producer = new SingleMessageProducerKafka(topic, kafkaConfig)

  override def sendNotification(): Unit = {
    log.info(s"Sending '$token' to Kafka topic: '$topic'...")
    producer.send(token)
    log.info(s"Successfully send the notification topic to Kafka.")
  }
}
