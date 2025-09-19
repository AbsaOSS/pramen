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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class MetastoreDependencyFactorySuite extends AnyWordSpec {
  "fromConfig" should {
    "load a dependency list with minimal settings" in {
      val conf = ConfigFactory.parseString(
        """tables = ["table1", "table2"]
          |date.from = "@infoDate"
          |""".stripMargin)

      val dependency = MetastoreDependencyFactory.fromConfig(conf, "", strictDependencyManagement = false)

      assert(dependency.tables == Seq("table1", "table2"))
      assert(dependency.dateFromExpr == "@infoDate")
      assert(dependency.dateUntilExpr.isEmpty)
      assert(dependency.triggerUpdates)
      assert(!dependency.isOptional)
      assert(!dependency.isPassive)
    }

    "load a dependency list with strict dependency management" in {
      val conf = ConfigFactory.parseString(
        """tables = ["table1", "table2"]
          |date.from = "@infoDate - 1"
          |""".stripMargin)

      val dependency = MetastoreDependencyFactory.fromConfig(conf, "", strictDependencyManagement = true)

      assert(dependency.tables == Seq("table1", "table2"))
      assert(dependency.dateFromExpr == "@infoDate - 1")
      assert(dependency.dateUntilExpr.contains("@infoDate - 1"))
      assert(dependency.triggerUpdates)
      assert(!dependency.isOptional)
      assert(dependency.isPassive)
    }
  }
}
