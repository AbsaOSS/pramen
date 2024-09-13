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
import org.apache.spark.sql.functions.lit

sealed trait OffsetValue extends Comparable[OffsetValue] {
  def dataTypeString: String

  def valueString: String

  def getSparkLit: Column
}

object OffsetValue {
  val LONG_TYPE_STR = "long"
  val STRING_TYPE_STR = "string"

  case class LongType(value: Long) extends OffsetValue {
    override val dataTypeString: String = LONG_TYPE_STR

    override def valueString: String = value.toString

    override def getSparkLit: Column = lit(value)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case LongType(otherValue) => value.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare $dataTypeString with ${o.dataTypeString}")
      }
    }
  }

  case class StringType(s: String) extends OffsetValue {
    override val dataTypeString: String = STRING_TYPE_STR

    override def valueString: String = s

    override def getSparkLit: Column = lit(s)

    override def compareTo(o: OffsetValue): Int = {
      o match {
        case StringType(otherValue) => s.compareTo(otherValue)
        case _ => throw new IllegalArgumentException(s"Cannot compare $dataTypeString with ${o.dataTypeString}")
      }
    }
  }

  def getMinimumForType(dataType: String): OffsetValue = {
    dataType match {
      case LONG_TYPE_STR => LongType(Long.MinValue)
      case STRING_TYPE_STR => StringType("")
      case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
    }
  }

  def fromString(dataType: String, value: String): OffsetValue = dataType match {
    case LONG_TYPE_STR => LongType(value.toLong)
    case STRING_TYPE_STR => StringType(value)
    case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
  }
}
