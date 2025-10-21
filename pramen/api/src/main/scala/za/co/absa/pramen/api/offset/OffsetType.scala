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

sealed trait OffsetType {
  def dataTypeString: String

  def getSparkCol(col: Column): Column
}

object OffsetType {
  val DATETIME_TYPE_STR = "datetime"
  val INTEGRAL_TYPE_STR = "integral"
  val INTEGRAL_TYPE_ALT_STR = "number"  // Alternative name for 'integral' purely for compatibility with SqlColumnType
  val STRING_TYPE_STR = "string"
  val KAFKA_TYPE_STR = "kafka"

  case object DateTimeType extends OffsetType {
    override val dataTypeString: String = DATETIME_TYPE_STR

    override def getSparkCol(c: Column): Column = concat(unix_timestamp(c), date_format(c, "SSS")).cast(LongType)
  }

  case object IntegralType extends OffsetType {
    override val dataTypeString: String = INTEGRAL_TYPE_STR

    override def getSparkCol(c: Column): Column = c.cast(LongType)
  }

  case object StringType extends OffsetType {
    override val dataTypeString: String = STRING_TYPE_STR

    override def getSparkCol(c: Column): Column = c.cast(SparkStringType)
  }

  case object KafkaType extends OffsetType {
    override val dataTypeString: String = KAFKA_TYPE_STR

    override def getSparkCol(c: Column): Column = c
  }

  def fromString(dataType: String): OffsetType = dataType match {
    case DATETIME_TYPE_STR => DateTimeType
    case INTEGRAL_TYPE_STR => IntegralType
    case INTEGRAL_TYPE_ALT_STR => IntegralType
    case STRING_TYPE_STR => StringType
    case KAFKA_TYPE_STR => KafkaType
    case _ => throw new IllegalArgumentException(s"Unknown offset data type: $dataType")
  }
}
