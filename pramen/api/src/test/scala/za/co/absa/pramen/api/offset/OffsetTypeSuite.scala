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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.wordspec.AnyWordSpec

class OffsetTypeSuite extends AnyWordSpec {
  "OffsetType" should {
    "be able to create a DateTimeType instance" in {
      val offsetValue = OffsetType.DateTimeType
      assert(offsetValue.dataTypeString == "datetime")
      assert(offsetValue.getSparkCol(col("a")) == concat(unix_timestamp(col("a")), date_format(col("a"), "SSS")).cast(LongType))
    }

    "be able to create a IntegralType instance" in {
      val offsetValue = OffsetType.IntegralType
      assert(offsetValue.dataTypeString == "integral")
      assert(offsetValue.getSparkCol(col("a")) == col("a").cast(LongType))
    }

    "be able to create a StringType instance" in {
      val offsetValue = OffsetType.StringType
      assert(offsetValue.dataTypeString == "string")
      assert(offsetValue.getSparkCol(col("a")) == col("a").cast(StringType))
    }
  }

  "fromString" should {
    "be able to create a DateTimeType instance from a string" in {
      val offsetValue = OffsetType.fromString("datetime")
      assert(offsetValue.dataTypeString == "datetime")
    }

    "be able to create a IntegralType instance from a string" in {
      val offsetValue = OffsetType.fromString("integral")
      assert(offsetValue.dataTypeString == "integral")
    }

    "be able to create a StringType instance from a string" in {
      val offsetValue = OffsetType.fromString("string")
      assert(offsetValue.dataTypeString == "string")
    }

    "throw an exception when trying to create an instance from a string with an unknown type" in {
      assertThrows[IllegalArgumentException] {
        OffsetType.fromString("unknown")
      }
    }
  }
}
