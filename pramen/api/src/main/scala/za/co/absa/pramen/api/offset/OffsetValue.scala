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
import za.co.absa.pramen.api.offset.OffsetType.{DATETIME_TYPE_STR, INTEGRAL_TYPE_STR, STRING_TYPE_STR}

import java.time.Instant

sealed trait OffsetValue extends Comparable[OffsetValue] {
  def dataType: OffsetType

  def valueString: String

  def getSparkLit: Column
}

object OffsetValue {
  val MINIMUM_TIMESTAMP_EPOCH_MILLI: Long = -62135596800000L

  case class DateTimeValue(t: Instant) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.DateTimeType

    override def valueString: String = t.toEpochMilli.toString

    override def getSparkLit: Column = lit(t.toEpochMilli)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case DateTimeValue(otherValue) => t.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${o.dataType.dataTypeString}")
      }
    }
  }

  case class IntegralValue(value: Long) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.IntegralType

    override def valueString: String = value.toString

    override def getSparkLit: Column = lit(value)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case IntegralValue(otherValue) => value.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${o.dataType.dataTypeString}")
      }
    }
  }

  case class StringValue(s: String) extends OffsetValue {
    override val dataType: OffsetType = OffsetType.StringType

    override def valueString: String = s

    override def getSparkLit: Column = lit(s)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case StringValue(otherValue) => s.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare ${dataType.dataTypeString} with ${o.dataType.dataTypeString}")
      }
    }
  }

  def getMinimumForType(dataType: String): OffsetValue = {
    dataType match {
      case DATETIME_TYPE_STR => DateTimeValue(Instant.ofEpochMilli(MINIMUM_TIMESTAMP_EPOCH_MILLI)) // LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
      case INTEGRAL_TYPE_STR => IntegralValue(Long.MinValue)
      case STRING_TYPE_STR => StringValue("")
      case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
    }
  }

  def fromString(dataType: String, value: String): Option[OffsetValue] = {
    if (value.isEmpty)
      None
    else
      dataType match {
        case DATETIME_TYPE_STR => Some(DateTimeValue(Instant.ofEpochMilli(value.toLong)))
        case INTEGRAL_TYPE_STR => Some(IntegralValue(value.toLong))
        case STRING_TYPE_STR => Some(StringValue(value))
        case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
      }
  }
}
