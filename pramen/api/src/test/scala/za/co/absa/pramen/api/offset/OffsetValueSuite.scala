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
import za.co.absa.pramen.api.offset.OffsetValue.MINIMUM_TIMESTAMP_EPOCH_MILLI

import java.time.Instant

class OffsetValueSuite extends AnyWordSpec {
  "OffsetValue" should {
    "be able to create a DateTimeType instance" in {
      val offsetValue = OffsetValue.DateTimeType(Instant.ofEpochMilli(1726564198000L))
      assert(offsetValue.dataTypeString == "datetime")
      assert(offsetValue.valueString == "1726564198000")
      assert(offsetValue.getSparkLit == lit(1726564198000L))
      assert(offsetValue.getSparkCol(col("a")) == concat(unix_timestamp(col("a")), date_format(col("a"), "SSS")).cast(LongType))
    }

    "be able to create a IntegralType instance" in {
      val offsetValue = OffsetValue.IntegralType(42)
      assert(offsetValue.dataTypeString == "integral")
      assert(offsetValue.valueString == "42")
      assert(offsetValue.getSparkLit == lit(42L))
      assert(offsetValue.getSparkCol(col("a")) == col("a").cast(LongType))
    }

    "be able to create a StringType instance" in {
      val offsetValue = OffsetValue.StringType("foo")
      assert(offsetValue.dataTypeString == "string")
      assert(offsetValue.valueString == "foo")
      assert(offsetValue.getSparkLit == lit("foo"))
      assert(offsetValue.getSparkCol(col("a")) == col("a").cast(StringType))
    }
  }

  "getMinimumForType" should {
    "be able to get minimum value for datetime type" in {
      val offsetValue = OffsetValue.getMinimumForType("datetime")
      assert(offsetValue.dataTypeString == "datetime")
      assert(offsetValue.valueString == MINIMUM_TIMESTAMP_EPOCH_MILLI.toString)
    }

    "be able to get minimum value for integral type" in {
      val offsetValue = OffsetValue.getMinimumForType("integral")
      assert(offsetValue.dataTypeString == "integral")
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
    "be able to create a DateTimeType instance from a string" in {
      val offsetValue = OffsetValue.fromString("datetime", "1726552310000")
      assert(offsetValue.dataTypeString == "datetime")
      assert(offsetValue.valueString == "1726552310000")
    }

    "be able to create a IntegralType instance from a string" in {
      val offsetValue = OffsetValue.fromString("integral", "42")
      assert(offsetValue.dataTypeString == "integral")
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

  "compareTo" should {
    "compare 2 datetime values" in {
      val offsetValue1 = OffsetValue.DateTimeType(Instant.ofEpochMilli(1726564198000L))
      val offsetValue2 = OffsetValue.DateTimeType(Instant.ofEpochMilli(1726564198001L))

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare a datetime value with value of some other type" in {
      val offsetValue1 = OffsetValue.DateTimeType(Instant.ofEpochMilli(1726564198000L))
      val offsetValue2 = OffsetValue.IntegralType(42)

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }

    "compare 2 integral values" in {
      val offsetValue1 = OffsetValue.IntegralType(42)
      val offsetValue2 = OffsetValue.IntegralType(43)

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare an integral value with value of some other type" in {
      val offsetValue1 = OffsetValue.IntegralType(42)
      val offsetValue2 = OffsetValue.StringType("foo")

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }

    "compare 2 string values" in {
      val offsetValue1 = OffsetValue.StringType("bar")
      val offsetValue2 = OffsetValue.StringType("foo")

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare a string value with value of some other type" in {
      val offsetValue1 = OffsetValue.StringType("foo")
      val offsetValue2 = OffsetValue.DateTimeType(Instant.ofEpochMilli(1726564198000L))

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }
  }
}
