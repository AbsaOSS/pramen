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

package za.co.absa.pramen.core.tests.bookkeeper

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.offset.{KafkaPartition, OffsetType, OffsetValue}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.OffsetManagerUtils

import java.time.Instant

class OffsetManagerUtilsSuite extends AnyWordSpec with SparkTestBase {

  import spark.implicits._

  "getMinMaxValueFromData" should {
    "work for an integral data type" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "offset")

      val (minValue, maxValue) = OffsetManagerUtils.getMinMaxValueFromData(df, "offset", OffsetType.IntegralType).get

      assert(minValue == OffsetValue.IntegralValue(1))
      assert(maxValue == OffsetValue.IntegralValue(3))
    }

    "work for an string data type" in {
      val df = List(("A", 3), ("B", 1), ("C", 2)).toDF("offset", "b")

      val (minValue, maxValue) = OffsetManagerUtils.getMinMaxValueFromData(df, "offset", OffsetType.StringType).get

      assert(minValue == OffsetValue.StringValue("A"))
      assert(maxValue == OffsetValue.StringValue("C"))
    }

    "work for an kafka data type" in {
      val df = List(
        (0, 1L, "a"),
        (0, 2L, "b"),
        (1, 3L, "c"),
        (1, 4L, "d"),
        (2, 1L, "e"),
        (2, 2L, "f"),
        (2, 3L, "g")
      ).toDF("kafka_partition", "kafka_offset", "field")

      val (minValue, maxValue) = OffsetManagerUtils.getMinMaxValueFromData(df, "kafka_offset", OffsetType.KafkaType).get

      assert(minValue == OffsetValue.KafkaValue(Seq(KafkaPartition(0, 1), KafkaPartition(1, 3), KafkaPartition(2, 1))))
      assert(maxValue == OffsetValue.KafkaValue(Seq(KafkaPartition(0, 2), KafkaPartition(1, 4), KafkaPartition(2, 3))))
    }

    "work for an datetime data type" in {
      val baseTime = 1733989092000L
      val df = List(("A", baseTime), ("B", baseTime + 1000), ("C", baseTime + 1500)).toDF("a", "offset")
        .withColumn("offset", (col("offset") / 1000).cast(TimestampType))

      val (minValue, maxValue) = OffsetManagerUtils.getMinMaxValueFromData(df, "offset", OffsetType.DateTimeType).get

      assert(minValue == OffsetValue.DateTimeValue(Instant.ofEpochMilli(baseTime)))
      assert(maxValue == OffsetValue.DateTimeValue(Instant.ofEpochMilli(baseTime + 1500)))
    }
  }
}
