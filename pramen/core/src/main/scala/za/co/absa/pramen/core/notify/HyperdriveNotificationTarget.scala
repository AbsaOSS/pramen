package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, NotificationTarget, TaskNotification}
import za.co.absa.pramen.core.mq.{SingleMessageProducer, SingleMessageProducerKafka}
import za.co.absa.pramen.core.utils.ConfigUtils

class HyperdriveNotificationTarget(conf: Config,
                                   producer: SingleMessageProducer,
                                   topic: String) extends NotificationTarget {
  val TOKEN_KEY = "hyperdrive.token"

  private val log = LoggerFactory.getLogger(this.getClass)

  override def config: Config = conf

  override def connect(): Unit = {
    producer.connect()
  }

  override def sendNotification(notification: TaskNotification): Unit = {
    if (notification.options.contains(TOKEN_KEY)) {
      val token = notification.options(TOKEN_KEY)

      log.info(s"Sending '$token' to the Hyperdrive Kafka topic: '$topic'...")
      producer.send(token)
      log.info(s"Successfully send the notification topic to Kafka.")
    } else {
      log.warn(s"Token is not configured for ${notification.tableName}. Hyperdrive notification won't be sent.")
    }
  }

  override def close(): Unit = producer.close()
}

object HyperdriveNotificationTarget extends ExternalChannelFactory[NotificationTarget] {
  val TOPIC_KEY = "topic"
  val KAFKA_KEY = "kafka"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): HyperdriveNotificationTarget = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(TOPIC_KEY, KAFKA_KEY))

    val topic = conf.getString(TOPIC_KEY)
    val kafkaConf = conf.getConfig(KAFKA_KEY)

    val producer = new SingleMessageProducerKafka(topic, kafkaConf)

    new HyperdriveNotificationTarget(conf, producer, topic)
  }
}
