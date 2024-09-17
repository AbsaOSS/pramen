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
import org.apache.spark.sql.types.{LongType, StringType => SparkStringType}

import java.time.Instant

sealed trait OffsetValue extends Comparable[OffsetValue] {
  def dataTypeString: String

  def valueString: String

  def getSparkLit: Column

  def getSparkCol(col: Column): Column
}

object OffsetValue {
  val DATETIME_TYPE_STR = "datetime"
  val INTEGRAL_TYPE_STR = "integral"
  val STRING_TYPE_STR = "string"

  val MINIMUM_TIMESTAMP_EPOCH_MILLI: Long = -62135596800000L

  case class DateTimeType(t: Instant) extends OffsetValue {
    override val dataTypeString: String = DATETIME_TYPE_STR

    override def valueString: String = t.toEpochMilli.toString

    override def getSparkLit: Column = lit(t.toEpochMilli)

    override def getSparkCol(c: Column): Column = concat(unix_timestamp(c), date_format(c, "SSS")).cast(LongType)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case DateTimeType(otherValue) => t.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare $dataTypeString with ${o.dataTypeString}")
      }
    }
  }

  case class IntegralType(value: Long) extends OffsetValue {
    override val dataTypeString: String = INTEGRAL_TYPE_STR

    override def valueString: String = value.toString

    override def getSparkLit: Column = lit(value)

    override def getSparkCol(c: Column): Column = c.cast(LongType)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case IntegralType(otherValue) => value.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare $dataTypeString with ${o.dataTypeString}")
      }
    }
  }

  case class StringType(s: String) extends OffsetValue {
    override val dataTypeString: String = STRING_TYPE_STR

    override def valueString: String = s

    override def getSparkLit: Column = lit(s)

    override def getSparkCol(c: Column): Column = c.cast(SparkStringType)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case StringType(otherValue) => s.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare $dataTypeString with ${o.dataTypeString}")
      }
    }
  }

  def getMinimumForType(dataType: String): OffsetValue = {
    dataType match {
      case DATETIME_TYPE_STR => DateTimeType(Instant.ofEpochMilli(MINIMUM_TIMESTAMP_EPOCH_MILLI)) // LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
      case INTEGRAL_TYPE_STR => IntegralType(Long.MinValue)
      case STRING_TYPE_STR => StringType("")
      case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
    }
  }

  def fromString(dataType: String, value: String): OffsetValue = dataType match {
    case DATETIME_TYPE_STR => DateTimeType(Instant.ofEpochMilli(value.toLong))
    case INTEGRAL_TYPE_STR => IntegralType(value.toLong)
    case STRING_TYPE_STR => StringType(value)
    case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
  }
}
