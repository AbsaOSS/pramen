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

import org.scalatest.wordspec.AnyWordSpec

class DataFormatKindSuite extends AnyWordSpec {
  "fromString" should {
    "support parquet" in {
      assert(DataFormatKind.fromString("Parquet").contains(DataFormatKind.Parquet))
    }

    "support iceberg" in {
      assert(DataFormatKind.fromString("Iceberg").contains(DataFormatKind.Iceberg))
    }

    "support delta" in {
      assert(DataFormatKind.fromString("Delta").contains(DataFormatKind.Delta))
    }

    "be case insensitive" in {
      assert(DataFormatKind.fromString("parquet").contains(DataFormatKind.Parquet))
      assert(DataFormatKind.fromString("PARQUET").contains(DataFormatKind.Parquet))
      assert(DataFormatKind.fromString("iCeBeRg").contains(DataFormatKind.Iceberg))
      assert(DataFormatKind.fromString("DELTA").contains(DataFormatKind.Delta))
    }

    "trim surrounding whitespace" in {
      assert(DataFormatKind.fromString("  parquet  ").contains(DataFormatKind.Parquet))
      assert(DataFormatKind.fromString("\ticeberg\n").contains(DataFormatKind.Iceberg))
      assert(DataFormatKind.fromString(" delta").contains(DataFormatKind.Delta))
    }

    "return None for an unknown format" in {
      assert(DataFormatKind.fromString("orc").isEmpty)
      assert(DataFormatKind.fromString("csv").isEmpty)
      assert(DataFormatKind.fromString("parquet1").isEmpty)
    }

    "return None for an empty string" in {
      assert(DataFormatKind.fromString("").isEmpty)
    }

    "return None for a whitespace-only string" in {
      assert(DataFormatKind.fromString("   ").isEmpty)
    }
  }

  "name" should {
    "return the correct name for parquet" in {
      assert(DataFormatKind.Parquet.name == "parquet")
    }

    "return the correct name for iceberg" in {
      assert(DataFormatKind.Iceberg.name == "iceberg")
    }

    "return the correct name for delta" in {
      assert(DataFormatKind.Delta.name == "delta")
    }
  }

  "values" should {
    "contain all data format kinds in the expected order" in {
      assert(DataFormatKind.values == Seq(DataFormatKind.Parquet, DataFormatKind.Iceberg, DataFormatKind.Delta))
    }

    "allow every value to be resolved from its name" in {
      DataFormatKind.values.foreach { kind =>
        assert(DataFormatKind.fromString(kind.name).contains(kind))
      }
    }
  }
}
