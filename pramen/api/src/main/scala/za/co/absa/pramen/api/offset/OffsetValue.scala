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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import za.co.absa.pramen.api.offset.OffsetType.{DATETIME_TYPE_STR, INTEGRAL_TYPE_STR, KAFKA_TYPE_STR, STRING_TYPE_STR}

import java.time.Instant
import scala.util.control.NonFatal

sealed trait OffsetValue extends Comparable[OffsetValue] {
  def dataType: OffsetType

  def valueString: String

  def getSparkLit: Column
}

object OffsetValue {
  case class DateTimeValue(t: Instant) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.DateTimeType

    override def valueString: String = t.toEpochMilli.toString

    override def getSparkLit: Column = lit(t.toEpochMilli)

    override def compareTo(other: OffsetValue): Int = {
      other match {
        case DateTimeValue(otherValue) => t.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${other.dataType.dataTypeString}")
      }
    }
  }

  case class IntegralValue(value: Long) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.IntegralType

    override def valueString: String = value.toString

    override def getSparkLit: Column = lit(value)

    override def compareTo(other: OffsetValue): Int = {
      other match {
        case IntegralValue(otherValue) => value.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${other.dataType.dataTypeString}")
      }
    }
  }

  case class StringValue(s: String) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.StringType

    override def valueString: String = s

    override def getSparkLit: Column = lit(s)

    override def compareTo(other: OffsetValue): Int = {
      other match {
        case StringValue(otherValue) => s.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${other.dataType.dataTypeString}")
      }
    }
  }

  case class KafkaValue(value: Seq[KafkaPartition]) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.KafkaType

    override def valueString: String = {
      val q = "\""
      value.sortBy(_.partition)
        .map(p => s"$q${p.partition}$q:${p.offset}")
        .mkString("{", ",", "}")
    }

    override def getSparkLit: Column = lit(valueString)

    override def compareTo(other: OffsetValue): Int = {
      other match {
        case otherKafka@KafkaValue(otherValue) =>
          if (value.length != otherValue.length) {
            throw new IllegalArgumentException(s"Cannot compare Kafka offsets with different number of partitions: ${value.length} and ${otherValue.length} ($valueString vs ${otherKafka.valueString}).")
          } else {
            val comparisons = value.sortBy(_.partition).zip(otherValue.sortBy(_.partition)).map { case (v1, v2) =>
              if (v1.partition != v2.partition) {
                throw new IllegalArgumentException(s"Cannot compare Kafka offsets with different partition numbers: ${v1.partition} and ${v2.partition} ($valueString vs ${otherKafka.valueString}).")
              } else {
                v1.offset.compareTo(v2.offset)
              }
            }
            val existPositive = comparisons.exists(_ > 0)
            val existNegative = comparisons.exists(_ < 0)

            if (existPositive && existNegative) {
              throw new IllegalArgumentException(s"Some offsets are bigger, some are smaller when comparing partitions: $valueString vs ${otherKafka.valueString}.")
            } else if (existPositive) {
              1
            } else if (existNegative) {
              -1
            } else {
              0
            }
          }
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${other.dataType.dataTypeString}")
      }
    }

    def increment: OffsetValue = {
      KafkaValue(value.map(p => p.copy(offset = p.offset + 1)))
    }
  }


  def fromString(dataType: String, value: String): Option[OffsetValue] = {
    if (value == null || value.isEmpty) {
      None
    } else
      dataType.trim.toLowerCase match {
        case DATETIME_TYPE_STR => Some(DateTimeValue(Instant.ofEpochMilli(value.toLong)))
        case INTEGRAL_TYPE_STR => Some(IntegralValue(value.toLong))
        case STRING_TYPE_STR => Some(StringValue(value))
        case KAFKA_TYPE_STR =>
          try {
            Some(KafkaValue(
              value
                .replaceAll("[{}\"]", "")
                .split(",")
                .filter(_.nonEmpty)
                .map { part =>
                  val Array(partStr, offsetStr) = part.split(":")
                  KafkaPartition(partStr.toInt, offsetStr.toLong)
                }.toSeq
            ))
          } catch {
            case NonFatal(ex) => throw new IllegalArgumentException(s"Unexpected Kafka offset: '$value'. Expected a JSON mapping from partition to offset.", ex)
          }
        case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
      }
  }
}
