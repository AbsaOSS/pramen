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

    "be able to create a KafkaValue instance" in {
      val offsetValue = OffsetValue.KafkaValue(Seq(KafkaPartition(0, 10), KafkaPartition(1, 20)))
      assert(offsetValue.dataType == OffsetType.KafkaType)
      assert(offsetValue.valueString == "{\"0\":10,\"1\":20}")
      assert(offsetValue.getSparkLit == lit("{\"0\":10,\"1\":20}"))
      assert(offsetValue.dataType.getSparkCol(col("a")) == col("a"))
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

    "be able to create a DateTimeType instance from a string with different casing and spaces" in {
      val offsetValue = OffsetValue.fromString(" dateTime ", "1726552310000")
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

    "be able to create a KafkaType instance from a string" in {
      val offsetValue = OffsetValue.fromString("kafka", "{\"0\":10,\"1\":20}")
      assert(offsetValue.get.dataType == OffsetType.KafkaType)
      assert(offsetValue.get.valueString == "{\"0\":10,\"1\":20}")
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

    "compare 2 kafka offset values" in {
      val offsetValue0 = OffsetValue.KafkaValue(Seq.empty)
      val offsetValue1 = OffsetValue.KafkaValue(Seq(KafkaPartition(0, 10), KafkaPartition(1, 20)))
      val offsetValue2 = OffsetValue.KafkaValue(Seq(KafkaPartition(0, 10), KafkaPartition(1, 21)))
      val offsetValue3 = OffsetValue.KafkaValue(Seq(KafkaPartition(0, 11), KafkaPartition(1, 20)))
      val offsetValue4 = OffsetValue.KafkaValue(Seq(KafkaPartition(1, 20), KafkaPartition(2, 40)))

      assert(offsetValue1.compareTo(offsetValue2) < 0)
      assert(offsetValue1.compareTo(offsetValue3) < 0)
      assert(offsetValue2.compareTo(offsetValue1) > 0)
      assert(offsetValue3.compareTo(offsetValue1) > 0)
      assert(offsetValue0.compareTo(offsetValue0) == 0)
      assert(offsetValue1.compareTo(offsetValue1) == 0)
      assert(offsetValue2.compareTo(offsetValue2) == 0)

      val ex1 = intercept[IllegalArgumentException] {
        offsetValue0.compareTo(offsetValue1)
      }

      assert(ex1.getMessage == """Cannot compare Kafka offsets with different number of partitions: 0 and 2 ({} vs {"0":10,"1":20}).""")

      val ex2 = intercept[IllegalArgumentException] {
        offsetValue1.compareTo(offsetValue4)
      }

      assert(ex2.getMessage == """Cannot compare Kafka offsets with different partition numbers: 0 and 1 ({"0":10,"1":20} vs {"1":20,"2":40}).""")

      val ex3 = intercept[IllegalArgumentException] {
        offsetValue2.compareTo(offsetValue3)
      }

      assert(ex3.getMessage == """Some offsets are bigger, some are smaller when comparing partitions: {"0":10,"1":21} vs {"0":11,"1":20}.""")
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
