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

package za.co.absa.pramen.extras.tests.writer

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.writer.TableWriterKafka

class TableWriterKafkaSuite extends AnyWordSpec with SparkTestBase {
  "TableWriterKafka" should {
    import spark.implicits._

    val df = Range(0, 200).map(x => (x, x)) toDF("a", "b")

    "be able to construct a new writer from config" in {
      val conf = ConfigFactory.parseString(
        s"""writer.kafka {
          |  brokers = "host1:9092"
          |  schema.registry.url = "localhost:8081"
          |  schema.registry.value.naming.strategy = "topic.name"
          |  schema.registry.value.schema.id = 2
          |}""".stripMargin)

      val writer = TableWriterKafka("dummy_topic", conf)

      //val dfOut = writer.getOutputDataFrame(df)

      //assert(dfOut.schema.fields.length == 1)
      //assert(dfOut.schema.fields.head.dataType.isInstanceOf[BinaryType])
      //assert(dfOut.schema.fields.head.name == "value")
    }

    "be able to construct Kafka writer that has keys and values" in {
      val conf = ConfigFactory.parseString(
        """writer.kafka {
          |  brokers = "host1:9092"
          |  schema.registry.url = "host2:8081"
          |  schema.registry.value.naming.strategy = "topic.name"
          |  schema.registry.key.naming.strategy = "record.name"
          |  schema.registry.key.schema.record.name = "record"
          |  schema.registry.key.schema.record.namespace = "namespace"
          |  key.column.names = [ a, b ]
          |}""".stripMargin)

      val writer = TableWriterKafka("dummy_topic", conf)

      //val dfOut = writer.getOutputDataFrame(df)

      //assert(dfOut.schema.fields.length == 2)
      //assert(dfOut.schema.fields.head.dataType.isInstanceOf[BinaryType])
      //assert(dfOut.schema.fields.head.name == "key")
      //assert(dfOut.schema.fields(1).dataType.isInstanceOf[BinaryType])
      //assert(dfOut.schema.fields(1).name == "value")
    }

    "be able to pass extra options" in {
      val conf = ConfigFactory.parseString(
        """writer.kafka {
          |  brokers = "host1:9092"
          |  schema.registry.url = "host2:8081"
          |  schema.registry.value.naming.strategy = "topic.name"
          |  schema.registry.key.naming.strategy = "topic.name"
          |  key.column.names = [ a, b ]
          |  option.kafka.sasl.jaas.config = "some"
          |  option.kafka.sasl.mechanism = "GSSAPI"
          |  option.kafka.security.protocol = "SASL_PLAINTEXT"
          |}""".stripMargin)

      val writer = TableWriterKafka("dummy_topic", conf)

      val extra = writer.getExtraOptions

      assert(extra("kafka.sasl.jaas.config") == "some")
      assert(extra("kafka.sasl.mechanism") == "GSSAPI")
      assert(extra("kafka.security.protocol") == "SASL_PLAINTEXT")
    }

  }

}
