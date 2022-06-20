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

package za.co.absa.pramen.tests.builtin

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.builtin.ParquetToKafkaSyncJob
import za.co.absa.pramen.writer.TableWriterKafka

class ParquetToKafkaSyncSuite extends WordSpec with SparkTestBase {
  "daily Kafka job factory" should {
    "should be able to create new jobs" in {
      val conf = ConfigFactory.parseResources("test/kafka_daily.conf")

      val job = ParquetToKafkaSyncJob(conf, spark)

      assert(job.name.nonEmpty)

      val writer = job.getWriter("topic1").asInstanceOf[TableWriterKafka]
//      val scv = writer.getValueSchemaRegistrySettings
//
//      assert(scv("schema.registry.topic") == "topic1")
//      assert(scv("schema.registry.url") == "sr1")
//      assert(scv("value.schema.naming.strategy") == "topic.name")
    }
  }

}
