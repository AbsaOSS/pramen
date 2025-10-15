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

package za.co.absa.pramen.core.bookkeeper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.types.StringType
import za.co.absa.pramen.api.offset.OffsetValue.{KAFKA_OFFSET_FIELD, KAFKA_PARTITION_FIELD}
import za.co.absa.pramen.api.offset.{KafkaPartition, OffsetType, OffsetValue}
import za.co.absa.pramen.api.sql.SqlGeneratorBase

object OffsetManagerUtils {
  def getMinMaxValueFromData(df: DataFrame, offsetColumn: String, offsetType: OffsetType): Option[(OffsetValue, OffsetValue)] = {
    if (df.isEmpty) {
      None
    } else {
      if (offsetType == OffsetType.KafkaType) {
        val aggregatedDf = df.groupBy(col(KAFKA_PARTITION_FIELD))
          .agg(
            min(col(KAFKA_OFFSET_FIELD)).as("min_offset"),
            max(col(KAFKA_OFFSET_FIELD)).as("max_offset")
          ).orderBy(KAFKA_PARTITION_FIELD)

        val minValue = OffsetValue.KafkaValue(aggregatedDf.collect().map(row => KafkaPartition(row.getAs[Int](0), row.getAs[Long](1))).toSeq)
        val maxValue = OffsetValue.KafkaValue(aggregatedDf.collect().map(row => KafkaPartition(row.getAs[Int](0), row.getAs[Long](2))).toSeq)

        Some(minValue, maxValue)
      } else {
        val row = df.agg(min(offsetType.getSparkCol(col(offsetColumn)).cast(StringType)),
            max(offsetType.getSparkCol(col(offsetColumn))).cast(StringType))
          .collect()(0)

        val minValue = OffsetValue.fromString(offsetType.dataTypeString, row(0).asInstanceOf[String]).getOrElse(throw new IllegalArgumentException(s"Can't parse offset: ${row(0)}"))
        val maxValue = OffsetValue.fromString(offsetType.dataTypeString, row(1).asInstanceOf[String]).getOrElse(throw new IllegalArgumentException(s"Can't parse offset: ${row(1)}"))

        SqlGeneratorBase.validateOffsetValue(minValue)
        SqlGeneratorBase.validateOffsetValue(maxValue)

        Some(minValue, maxValue)
      }
    }
  }
}
