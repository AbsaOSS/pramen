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

package za.co.absa.pramen.extras.tests.writer.model

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class NamingStrategySuite extends AnyWordSpec {
  import za.co.absa.pramen.extras.writer.model.NamingStrategy._

  "fromConfigOpt" should {
    "return None if strategy is not specified" in {
      val conf = ConfigFactory.parseString("a = b")

      assert(fromConfigOpt(conf).isEmpty)
    }

    "support 'topic.name' strategy" in {
      val conf = ConfigFactory.parseString("naming.strategy = \"topic.name\"")
      val ns = fromConfigOpt(conf)

      assert(ns.nonEmpty)
      assert(ns.get.namingStrategy == "topic.name")
    }

    "support 'record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "record.name"
          |schema.record.name = "record"
          |schema.record.namespace = "namespace"
          |""".stripMargin)

      val ns = fromConfigOpt(conf)

      assert(ns.nonEmpty)
      assert(ns.get.namingStrategy == "record.name")
      assert(ns.get.recordName.get == "record")
      assert(ns.get.recordNamespace.get == "namespace")
    }

    "support 'topic.record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "topic.record.name"
          |schema.record.name = "record"
          |schema.record.namespace = "namespace"
          |""".stripMargin)

      val ns = fromConfigOpt(conf)

      assert(ns.nonEmpty)
      assert(ns.get.namingStrategy == "topic.record.name")
      assert(ns.get.recordName.get == "record")
      assert(ns.get.recordNamespace.get == "namespace")
    }

    "throw if wrong naming strategy is specified" in {
      val conf = ConfigFactory.parseString("naming.strategy = \"dummy.name\"")

      val ex = intercept[IllegalArgumentException] {
        fromConfigOpt(conf)
      }

      assert(ex.getMessage.contains("Illegal naming strategy"))
    }

    "throw if record name is not specified for 'record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "record.name"
          |schema.record.namespace = "namespace"
          |""".stripMargin)

      val ex = intercept[Missing] {
        fromConfigOpt(conf)
      }

      assert(ex.getMessage.contains("No configuration setting found for key 'schema.record.name'"))
    }

    "throw if record name is not specified for 'topic.record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "topic.record.name"
          |schema.record.namespace = "namespace"
          |""".stripMargin)

      val ex = intercept[Missing] {
        fromConfigOpt(conf)
      }

      assert(ex.getMessage.contains("No configuration setting found for key 'schema.record.name'"))
    }

    "throw if namespace name is not specified for 'record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "record.name"
          |schema.record.name = "namespace"
          |""".stripMargin)

      val ex = intercept[Missing] {
        fromConfigOpt(conf)
      }

      assert(ex.getMessage.contains("No configuration setting found for key 'schema.record.namespace'"))
    }

    "throw if namespace name is not specified for 'topic.record.name' strategy" in {
      val conf = ConfigFactory.parseString(
        """naming.strategy = "topic.record.name"
          |schema.record.name = "namespace"
          |""".stripMargin)

      val ex = intercept[Missing] {
        fromConfigOpt(conf)
      }

      assert(ex.getMessage.contains("No configuration setting found for key 'schema.record.namespace'"))
    }

  }

}
