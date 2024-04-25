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

package za.co.absa.pramen.core.utils.impl

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import java.sql.Types._
import java.sql.{Date, ResultSet, Timestamp}
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.mutable

class ResultSetToRowIterator(rs: ResultSet, sanitizeDateTime: Boolean, incorrectDecimalsAsString: Boolean) extends Iterator[Row] {
  import ResultSetToRowIterator._

  private var didHasNext = false
  private var item: Option[Row] = None
  private val columnCount = rs.getMetaData.getColumnCount
  private val columnIndexToTypeMap = new mutable.HashMap[Int, Int]()

  override def hasNext: Boolean = {
    if (didHasNext) {
      item.isDefined
    } else {
      fetchNext()
      item.isDefined
    }
  }

  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val it = item.get
    item = None
    didHasNext = false
    it
  }

  def getSchema: StructType = {
    val columns = new Array[StructField](columnCount)

    var i = 1
    while (i <= columnCount) {
      columns(i - 1) = getStructField(i)
      i += 1
    }
    StructType(columns)
  }

  def close(): Unit = {
    rs.close()
  }

  private[core] def fetchNext(): Unit = {
    didHasNext = true
    if (rs.next()) {
      val data = new Array[Any](columnCount)
      var i = 1
      while (i <= columnCount) {
        data(i - 1) = getCell(i)
        i += 1
      }
      item = Some(new GenericRow(data))
    } else {
      rs.close()
      item = None
    }
  }

  private[core] def getStructField(columnIndex: Int): StructField = {
    val columnName = rs.getMetaData.getColumnName(columnIndex)
    val dataType = rs.getMetaData.getColumnType(columnIndex)

    dataType match {
      case BIT | BOOLEAN => StructField(columnName, BooleanType)
      case TINYINT       => StructField(columnName, ByteType)
      case SMALLINT      => StructField(columnName, ShortType)
      case INTEGER       => StructField(columnName, IntegerType)
      case BIGINT        => StructField(columnName, LongType)
      case FLOAT         => StructField(columnName, FloatType)
      case DOUBLE        => StructField(columnName, DoubleType)
      case REAL          => StructField(columnName, getDecimalSparkSchema(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case NUMERIC       => StructField(columnName, getDecimalSparkSchema(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case DATE          => StructField(columnName, DateType)
      case TIMESTAMP     => StructField(columnName, TimestampType)
      case _             => StructField(columnName, StringType)
    }
  }

  private[core] def getCell(columnIndex: Int): Any = {
    if (columnIndexToTypeMap.isEmpty) {
      setupColumnTypes()
    }

    val dataType = columnIndexToTypeMap(columnIndex)

    // WARNING. Do not forget that `null` is a valid value returned by RecordSet methods that return a reference type objects.
    dataType match {
      case BIT | BOOLEAN => rs.getBoolean(columnIndex)
      case TINYINT       => rs.getByte(columnIndex)
      case SMALLINT      => rs.getShort(columnIndex)
      case INTEGER       => rs.getInt(columnIndex)
      case BIGINT        => rs.getLong(columnIndex)
      case FLOAT         => rs.getFloat(columnIndex)
      case DOUBLE        => rs.getDouble(columnIndex)
      case REAL          => rs.getBigDecimal(columnIndex)
      case NUMERIC       => rs.getBigDecimal(columnIndex)
      case DATE          => sanitizeDate(rs.getDate(columnIndex))
      case TIMESTAMP     => sanitizeTimestamp(rs.getTimestamp(columnIndex))
      case _             => rs.getString(columnIndex)
    }
  }

  private[core] def getDecimalSparkSchema(precision: Int, scale: Int): DataType = {
    if (scale >= precision || precision <= 0 || scale < 0 || precision > 38 || (precision + scale) > 38) {
      if (incorrectDecimalsAsString) {
        StringType
      } else {
        DecimalType(38, 18)
      }
    } else {
      DecimalType(precision, scale)
    }
  }

  private[core] def setupColumnTypes(): Unit = {
    for (i <- 1 to columnCount) {
      val dataType = rs.getMetaData.getColumnType(i)

      // WARNING. Do not forget that `null` is a valid value returned by RecordSet methods that return a reference type objects.
      val actualDataType = dataType match {
        case BIT | BOOLEAN => BOOLEAN
        case TINYINT       => TINYINT
        case SMALLINT      => SMALLINT
        case INTEGER       => INTEGER
        case BIGINT        => BIGINT
        case FLOAT         => FLOAT
        case DOUBLE        => DOUBLE
        case REAL          => getDecimalDataType(i)
        case NUMERIC       => getDecimalDataType(i)
        case DATE          => DATE
        case TIMESTAMP     => TIMESTAMP
        case _             => VARCHAR
      }

      columnIndexToTypeMap += i -> actualDataType
    }
  }

  private[core] def getDecimalDataType(columnIndex: Int): Int = {
    if (incorrectDecimalsAsString) {
      val precision = rs.getMetaData.getPrecision(columnIndex)
      val scale = rs.getMetaData.getScale(columnIndex)

      if (scale >= precision || precision <= 0 || scale < 0 || precision > 38 || (precision + scale) > 38) {
        VARCHAR
      } else {
        NUMERIC
      }
    } else {
      NUMERIC
    }
  }

  private[core] def sanitizeDate(date: Date): Date = {
    // This check against null is important since date=null is a valid value.
    if (sanitizeDateTime && date != null) {
      val timeMilli = date.getTime
      if (timeMilli > MAX_SAFE_DATE_MILLI)
        MAX_SAFE_DATE
      else if (timeMilli < MIN_SAFE_DATE_MILLI)
        MIN_SAFE_DATE
      else
        date
    } else {
      date
    }
  }

  private[core] def sanitizeTimestamp(timestamp: Timestamp): Timestamp = {
    // This check against null is important since timestamp=null is a valid value.
    if (sanitizeDateTime && timestamp != null) {
      val timeMilli = timestamp.getTime
      if (timeMilli > MAX_SAFE_TIMESTAMP_MILLI)
        MAX_SAFE_TIMESTAMP
      else if (timeMilli < MIN_SAFE_TIMESTAMP_MILLI)
        MIN_SAFE_TIMESTAMP
      else
        timestamp
    } else {
      timestamp
    }
  }
}

object ResultSetToRowIterator {
  // Constants are aligned with Spark implementation
  val MAX_SAFE_DATE_MILLI: Long = LocalDateTime.of(9999, 12, 31, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  val MAX_SAFE_DATE = new Date(MAX_SAFE_DATE_MILLI)

  val MAX_SAFE_TIMESTAMP_MILLI: Long = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999).toInstant(ZoneOffset.UTC).toEpochMilli
  val MAX_SAFE_TIMESTAMP = new Timestamp(MAX_SAFE_TIMESTAMP_MILLI)

  val MIN_SAFE_DATE_MILLI: Long = LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  val MIN_SAFE_DATE = new Date(MIN_SAFE_DATE_MILLI)

  val MIN_SAFE_TIMESTAMP_MILLI: Long = LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  val MIN_SAFE_TIMESTAMP = new Timestamp(MIN_SAFE_TIMESTAMP_MILLI)
}
