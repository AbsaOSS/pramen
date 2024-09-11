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

package za.co.absa.pramen.api.offset

import org.apache.spark.sql.functions.lit
import org.scalatest.wordspec.AnyWordSpec

class OffsetValueSuite extends AnyWordSpec {
  "OffsetValue" should {
    "be able to create a LongType instance" in {
      val offsetValue = OffsetValue.LongType(42)
      assert(offsetValue.dataTypeString == "long")
      assert(offsetValue.valueString == "42")
      assert(offsetValue.getSparkLit == lit(42L))
    }

    "be able to create a StringType instance" in {
      val offsetValue = OffsetValue.StringType("foo")
      assert(offsetValue.dataTypeString == "string")
      assert(offsetValue.valueString == "foo")
      assert(offsetValue.getSparkLit == lit("foo"))
    }
  }

  "getMinimumForType" should {
    "be able to get minimum value for long type" in {
      val offsetValue = OffsetValue.getMinimumForType("long")
      assert(offsetValue.dataTypeString == "long")
      assert(offsetValue.valueString == Long.MinValue.toString)
    }

    "be able to get minimum value for string type" in {
      val offsetValue = OffsetValue.getMinimumForType("string")
      assert(offsetValue.dataTypeString == "string")
      assert(offsetValue.valueString == "")
    }

    "throw an exception when trying to get minimum value for an unknown type" in {
      assertThrows[IllegalArgumentException] {
        OffsetValue.getMinimumForType("unknown")
      }
    }
  }

  "fromString" should {
    "be able to create a LongType instance from a string" in {
      val offsetValue = OffsetValue.fromString("long", "42")
      assert(offsetValue.dataTypeString == "long")
      assert(offsetValue.valueString == "42")
    }

    "be able to create a StringType instance from a string" in {
      val offsetValue = OffsetValue.fromString("string", "foo")
      assert(offsetValue.dataTypeString == "string")
      assert(offsetValue.valueString == "foo")
    }

    "throw an exception when trying to create an instance from a string with an unknown type" in {
      assertThrows[IllegalArgumentException] {
        OffsetValue.fromString("unknown", "42")
      }
    }
  }

}
