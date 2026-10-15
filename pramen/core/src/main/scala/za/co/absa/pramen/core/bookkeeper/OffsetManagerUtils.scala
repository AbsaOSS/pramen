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
import za.co.absa.pramen.api.offset.{KafkaPartition, OffsetType, OffsetValue}
import za.co.absa.pramen.api.sql.SqlGeneratorBase

object OffsetManagerUtils {

  /**
    * Merges two Kafka partition sets in such a way that 'primaryValue' is considered a maximum offset
    * for a given increment ingestion job. But if data ingested does not cover all partitions, such partitions
    * might be missing in the list. In this case, values from the 'baseValue' are used.
    */
  def mergeKafkaPartitions(primaryValue: OffsetValue.KafkaValue, baseValue: OffsetValue.KafkaValue): OffsetValue.KafkaValue = {
    val partitions1 = primaryValue.value.map(v => (v.partition, v.offset)).toMap
    val partitions2 = baseValue.value.map(v => (v.partition, v.offset)).toMap
    val maxPartition = Math.max(partitions1.keys.max, partitions2.keys.max)

    val newPartitions = for (i <- 0 to maxPartition) yield  {
      if (partitions1.contains(i))
        KafkaPartition(i, partitions1(i))
      else if (partitions2.contains(i))
        KafkaPartition(i, partitions2(i))
      else
        KafkaPartition(i, 0)
    }

    OffsetValue.KafkaValue(newPartitions)
  }


  /**
    *
    */
  def getMinMaxValueFromData(df: DataFrame, offsetColumn: String, offsetType: OffsetType, currentMaxOffset: Option[OffsetValue]): Option[(OffsetValue, OffsetValue)] = {

    if (df.isEmpty) {
      None
    } else {
      val kafkaOffsetFieldName = s"$offsetColumn.offset"
      val kafkaPartitionFieldName = s"$offsetColumn.partition"

      if (offsetType == OffsetType.KafkaType) {
        val aggregatedDf = df.groupBy(col(kafkaPartitionFieldName))
          .agg(
            min(col(kafkaOffsetFieldName)).as("min_offset"),
            max(col(kafkaOffsetFieldName)).as("max_offset")
          ).orderBy(kafkaPartitionFieldName)

        val minValue: OffsetValue.KafkaValue = OffsetValue.KafkaValue(aggregatedDf.collect().map(row => KafkaPartition(row.getAs[Int](0), row.getAs[Long](1))).toSeq)
        val maxValue = OffsetValue.KafkaValue(aggregatedDf.collect().map(row => KafkaPartition(row.getAs[Int](0), row.getAs[Long](2))).toSeq)

        val (minValueMerged, maxValueMerged) = currentMaxOffset match {
          case Some(value: OffsetValue.KafkaValue) => (mergeKafkaPartitions(minValue, value), mergeKafkaPartitions(maxValue, value))
          case _ => (minValue, maxValue)
        }

        Some(minValueMerged, maxValueMerged)
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
