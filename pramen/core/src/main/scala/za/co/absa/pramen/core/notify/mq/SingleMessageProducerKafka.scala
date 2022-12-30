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

package za.co.absa.pramen.core.notify.mq

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.ConfigUtils

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

class SingleMessageProducerKafka(kafkaConfig: Config) extends SingleMessageProducer {
  val SEND_TIMEOUT_SECONDS = 120

  val props: Properties = ConfigUtils.toProperties(kafkaConfig)

  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer])

  var producer: KafkaProducer[String, String] = _

  override def connect(): Unit = {
    if (producer == null) {
      producer = new KafkaProducer[String, String](props)
    }
  }

  override def send(topic: String, message: String, numberOrRetries: Int): Unit = {
    connect()

    val record = new ProducerRecord[String, String](topic, null, message)
    try {
      producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    } catch {
      case NonFatal(ex) =>
        if (numberOrRetries <= 0) {
          throw ex
        } else {
          send(topic, message, numberOrRetries - 1)
        }
    }
  }

  override def close(): Unit = {
    if (producer != null) {
      producer.flush()
      producer.close()
      producer = null
    }
  }
}
