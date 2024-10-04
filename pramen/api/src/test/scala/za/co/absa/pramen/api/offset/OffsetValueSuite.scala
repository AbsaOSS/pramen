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
      val offsetValue = OffsetValue.DateTimeValue(Instant.ofEpochMilli(1726564198000L))
      assert(offsetValue.dataType == OffsetType.DateTimeType)
      assert(offsetValue.valueString == "1726564198000")
      assert(offsetValue.getSparkLit == lit(1726564198000L))
      assert(offsetValue.dataType.getSparkCol(col("a")) == concat(unix_timestamp(col("a")), date_format(col("a"), "SSS")).cast(LongType))
    }

    "be able to create a IntegralValue instance" in {
      val offsetValue = OffsetValue.IntegralValue(42)
      assert(offsetValue.dataType == OffsetType.IntegralType)
      assert(offsetValue.valueString == "42")
      assert(offsetValue.getSparkLit == lit(42L))
      assert(offsetValue.dataType.getSparkCol(col("a")) == col("a").cast(LongType))
    }

    "be able to create a StringValue instance" in {
      val offsetValue = OffsetValue.StringValue("foo")
      assert(offsetValue.dataType == OffsetType.StringType)
      assert(offsetValue.valueString == "foo")
      assert(offsetValue.getSparkLit == lit("foo"))
      assert(offsetValue.dataType.getSparkCol(col("a")) == col("a").cast(StringType))
    }
  }

  "getMinimumForType" should {
    "be able to get minimum value for datetime type" in {
      val offsetValue = OffsetValue.getMinimumForType("datetime")
      assert(offsetValue.dataType == OffsetType.DateTimeType)
      assert(offsetValue.valueString == MINIMUM_TIMESTAMP_EPOCH_MILLI.toString)
    }

    "be able to get minimum value for integral type" in {
      val offsetValue = OffsetValue.getMinimumForType("integral")
      assert(offsetValue.dataType == OffsetType.IntegralType)
      assert(offsetValue.valueString == Long.MinValue.toString)
    }

    "be able to get minimum value for string type" in {
      val offsetValue = OffsetValue.getMinimumForType("string")
      assert(offsetValue.dataType == OffsetType.StringType)
      assert(offsetValue.valueString == "")
    }

    "throw an exception when trying to get minimum value for an unknown type" in {
      assertThrows[IllegalArgumentException] {
        OffsetValue.getMinimumForType("unknown")
      }
    }
  }

  "fromString" should {
    "return None for an empty string" in {
      val offsetValue = OffsetValue.fromString("datetime", "")
      assert(offsetValue.isEmpty)
    }

    "be able to create a DateTimeType instance from a string" in {
      val offsetValue = OffsetValue.fromString("datetime", "1726552310000")
      assert(offsetValue.get.dataType == OffsetType.DateTimeType)
      assert(offsetValue.get.valueString == "1726552310000")
    }

    "be able to create a IntegralType instance from a string" in {
      val offsetValue = OffsetValue.fromString("integral", "42")
      assert(offsetValue.get.dataType == OffsetType.IntegralType)
      assert(offsetValue.get.valueString == "42")
    }

    "be able to create a StringType instance from a string" in {
      val offsetValue = OffsetValue.fromString("string", "foo")
      assert(offsetValue.get.dataType == OffsetType.StringType)
      assert(offsetValue.get.valueString == "foo")
    }

    "throw an exception when trying to create an instance from a string with an unknown type" in {
      assertThrows[IllegalArgumentException] {
        OffsetValue.fromString("unknown", "42")
      }
    }
  }

  "compareTo" should {
    "compare 2 datetime values" in {
      val offsetValue1 = OffsetValue.DateTimeValue(Instant.ofEpochMilli(1726564198000L))
      val offsetValue2 = OffsetValue.DateTimeValue(Instant.ofEpochMilli(1726564198001L))

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare a datetime value with value of some other type" in {
      val offsetValue1 = OffsetValue.DateTimeValue(Instant.ofEpochMilli(1726564198000L))
      val offsetValue2 = OffsetValue.IntegralValue(42)

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }

    "compare 2 integral values" in {
      val offsetValue1 = OffsetValue.IntegralValue(42)
      val offsetValue2 = OffsetValue.IntegralValue(43)

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare an integral value with value of some other type" in {
      val offsetValue1 = OffsetValue.IntegralValue(42)
      val offsetValue2 = OffsetValue.StringValue("foo")

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }

    "compare 2 string values" in {
      val offsetValue1 = OffsetValue.StringValue("bar")
      val offsetValue2 = OffsetValue.StringValue("foo")

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)
    }

    "throw an exception when attempting to compare a string value with value of some other type" in {
      val offsetValue1 = OffsetValue.StringValue("foo")
      val offsetValue2 = OffsetValue.DateTimeValue(Instant.ofEpochMilli(1726564198000L))

      assertThrows[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue2)
      }
    }
  }
}
