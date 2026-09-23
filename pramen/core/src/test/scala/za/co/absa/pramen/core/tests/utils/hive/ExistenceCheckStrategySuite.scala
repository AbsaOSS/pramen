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

package za.co.absa.pramen.core.tests.utils.hive

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.hive.ExistenceCheckStrategy

class ExistenceCheckStrategySuite extends AnyWordSpec {
  "fromString" should {
    "return MetadataQuery when 'metadata_query' is specified" in {
      val strategy = ExistenceCheckStrategy.fromString("metadata_query")

      assert(strategy == ExistenceCheckStrategy.MetadataQuery)
    }

    "return DescribeTable when 'describe_table' is specified" in {
      val strategy = ExistenceCheckStrategy.fromString("describe_table")

      assert(strategy == ExistenceCheckStrategy.DescribeTable)
    }

    "return MetadataAndDescribeQuery when 'metadata_and_describe_query' is specified" in {
      val strategy = ExistenceCheckStrategy.fromString("metadata_and_describe_query")

      assert(strategy == ExistenceCheckStrategy.MetadataAndDescribeQuery)
    }

    "return SelectQuery when 'select_query' is specified" in {
      val strategy = ExistenceCheckStrategy.fromString("select_query")

      assert(strategy == ExistenceCheckStrategy.SelectQuery)
    }

    "throw IllegalArgumentException when an incorrect strategy string is specified" in {
      val ex = intercept[IllegalArgumentException] {
        ExistenceCheckStrategy.fromString("invalid_strategy")
      }

      assert(ex.getMessage.contains("invalid_strategy"))
    }
  }

}
