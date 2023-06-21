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

package za.co.absa.pramen.core.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class DatabricksClientSuite extends AnyWordSpec {

  "canCreate()" should {
    "return true if config contains required keys" in {
      val conf = ConfigFactory.parseString(
        """
          |pramen.py.databricks {
          | host = "https://example.org"
          | token = "[some token]"
          |}
          |""".stripMargin)

      assert(DatabricksClient.canCreate(conf))
    }

    "return false if config does not contain required keys" in {
      val conf = ConfigFactory.empty()

      assert(!DatabricksClient.canCreate(conf))
    }

    "return false if config contains only a databricks host" in {
      val conf = ConfigFactory.parseString("pramen.py.databricks.host = some_host")

      assert(!DatabricksClient.canCreate(conf))

    }

    "return false if config contains only a databricks token" in {
      val conf = ConfigFactory.parseString("pramen.py.databricks.token = some_token")

      assert(!DatabricksClient.canCreate(conf))
    }
  }

  "fromConfig()" should {
    "create a client from config" in {
      val conf = ConfigFactory.parseString(
        """
          |pramen.py.databricks {
          | host = "https://example.org"
          | token = "[some token]"
          |}
          |""".stripMargin)

      val client = DatabricksClient.fromConfig(conf)

      client.isInstanceOf[DatabricksClient]
    }
  }
}
