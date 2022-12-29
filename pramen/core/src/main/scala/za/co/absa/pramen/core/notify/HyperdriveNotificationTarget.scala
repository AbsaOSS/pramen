package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, NotificationTarget, TaskNotification, TaskStatus}
import za.co.absa.pramen.core.notify.mq.{SingleMessageProducer, SingleMessageProducerKafka}
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

      if (notification.status.isInstanceOf[TaskStatus.Skipped]) {
        log.info(s"Sending '$token' to the Hyperdrive Kafka topic: '$topic'...")
        producer.send(topic, token)
        log.info(s"Successfully send the notification topic to Kafka.")
      } else {
        log.info(s"Not sending '$token' to the Hyperdrive Kafka topic: '$topic' for the unsuccessful job...")
      }
    } else {
      log.warn(s"Token is not configured for ${notification.tableName}. Hyperdrive notification won't be sent. Please, set 'notification.$TOKEN_KEY' option for the job.")
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

    val producer = new SingleMessageProducerKafka(kafkaConf)

    new HyperdriveNotificationTarget(conf, producer, topic)
  }
}
