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
    val metadata = rs.getMetaData
    val columnName = metadata.getColumnName(columnIndex)
    val dataType = metadata.getColumnType(columnIndex)
    val size = metadata.getPrecision(columnIndex)

    dataType match {
      case BIT if size > 1 => StructField(columnName, BinaryType)
      case BIT | BOOLEAN   => StructField(columnName, BooleanType)
      case BLOB            => StructField(columnName, BinaryType)
      case VARBINARY       => StructField(columnName, BinaryType)
      case LONGVARBINARY   => StructField(columnName, BinaryType)
      case TINYINT         => StructField(columnName, ByteType)
      case SMALLINT        => StructField(columnName, ShortType)
      case INTEGER         => StructField(columnName, IntegerType)
      case BIGINT          => StructField(columnName, LongType)
      case FLOAT           => StructField(columnName, FloatType)
      case DOUBLE          => StructField(columnName, DoubleType)
      case REAL            => StructField(columnName, getDecimalSparkSchema(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case NUMERIC         => StructField(columnName, getDecimalSparkSchema(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case DATE            => StructField(columnName, DateType)
      case TIMESTAMP       => StructField(columnName, TimestampType)
      case ARRAY           => StructField(columnName, getArrayDataType(columnIndex))
      case _               => StructField(columnName, StringType)
    }
  }

  private[core] def getCell(columnIndex: Int): Any = {
    if (columnIndexToTypeMap.isEmpty) {
      setupColumnTypes()
    }

    val dataType = columnIndexToTypeMap(columnIndex)

    // WARNING. Do not forget that `null` is a valid value returned by RecordSet methods that return a reference type objects.
    dataType match {
      case BIT if size > 1                  =>
        val v = rs.getBytes(columnIndex)
        if (rs.wasNull()) null else v
      case BLOB | VARBINARY | LONGVARBINARY =>
        val v = rs.getBytes(columnIndex)
        if (rs.wasNull()) null else v
      case BIT | BOOLEAN                    =>
        val v = rs.getBoolean(columnIndex)
        if (rs.wasNull()) null else v
      case TINYINT                          =>
        val v = rs.getByte(columnIndex)
        if (rs.wasNull()) null else v
      case SMALLINT                         =>
        val v = rs.getShort(columnIndex)
        if (rs.wasNull()) null else v
      case INTEGER                          =>
        val v = rs.getInt(columnIndex)
        if (rs.wasNull()) null else v
      case BIGINT                           =>
        val v = rs.getLong(columnIndex)
        if (rs.wasNull()) null else v
      case FLOAT                            =>
        val v = rs.getFloat(columnIndex)
        if (rs.wasNull()) null else v
      case DOUBLE                           =>
        val v = rs.getDouble(columnIndex)
        if (rs.wasNull()) null else v
      case REAL                             =>
        rs.getBigDecimal(columnIndex)
      case NUMERIC                          =>
        rs.getBigDecimal(columnIndex)
      case DATE                             =>
        sanitizeDate(rs.getDate(columnIndex))
      case TIMESTAMP                        =>
        sanitizeTimestamp(rs.getTimestamp(columnIndex))
      case ARRAY_STRING                     =>
        getJdbcArray(columnIndex).asInstanceOf[Array[String]]
      case ARRAY_BINARY                     =>
        getJdbcArray(columnIndex).asInstanceOf[Array[Array[Byte]]]
      case ARRAY_BOOL                       =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Boolean]]
      case ARRAY_SHORT                      =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Short]]
      case ARRAY_INT                        =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Integer]]
      case ARRAY_LONG                       =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Long]]
      case ARRAY_FLOAT                      =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Float]]
      case ARRAY_DOUBLE                     =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.lang.Double]]
      case ARRAY_DECIMAL                    =>
        getJdbcArray(columnIndex).asInstanceOf[Array[java.math.BigDecimal]]
      case ARRAY_DATE                       =>
        getJdbcArray(columnIndex).asInstanceOf[Array[Date]]
      case ARRAY_TIMESTAMP                  =>
        getJdbcArray(columnIndex).asInstanceOf[Array[Timestamp]]
      case _                                =>
        rs.getString(columnIndex)
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
        case BIT if size > 1 => BLOB
        case BIT | BOOLEAN   => BOOLEAN
        case BLOB            => BLOB
        case VARBINARY       => BLOB
        case LONGVARBINARY   => BLOB
        case BIT | BOOLEAN   => BOOLEAN
        case TINYINT         => TINYINT
        case SMALLINT        => SMALLINT
        case INTEGER         => INTEGER
        case BIGINT          => BIGINT
        case FLOAT           => FLOAT
        case DOUBLE          => DOUBLE
        case REAL            => getDecimalDataType(i)
        case NUMERIC         => getDecimalDataType(i)
        case DATE            => DATE
        case TIMESTAMP       => TIMESTAMP
        case ARRAY           => getArrayJdbcDataType(i)
        case _               => VARCHAR
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

  private[core] def getArrayDataType(columnIndex: Int): DataType = {
    val metadata = rs.getMetaData
    val typeName = metadata.getColumnTypeName(columnIndex)

    // Some drivers, like PostgreSQL adds '_' to array data type names.
    val fixedTypeName = if (typeName.startsWith("_")) typeName.drop(1) else typeName

    val columnTypeOpt = fixedTypeName match {
      case "bool"                                          => Some(BooleanType)
      case "bit"                                           => Some(BinaryType)
      case "int2"                                          => Some(ShortType)
      case "int4"                                          => Some(IntegerType)
      case "int8" | "oid"                                  => Some(LongType)
      case "float4"                                        => Some(FloatType)
      case "float8"                                        => Some(DoubleType)
      case "text" | "varchar" | "char" | "bpchar" | "cidr" | "inet" |
           "json" | "jsonb" | "uuid" | "xml" | "macaddr" | "macaddr8" |
           "txid_snapshot" | "path" | "varbit" |
           "interval"                                      => Some(StringType)
      case "bytea"                                         => Some(BinaryType)
      case "timestamp" | "timestamptz" | "time" | "timetz" => Some(TimestampType)
      case "date"                                          => Some(DateType)
      case "numeric" | "decimal"                           => Option(getDecimalSparkSchema(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case _                                               => None
    }

    columnTypeOpt match {
      case Some(dt) => ArrayType(dt)
      case None     => StringType // Make the driver stringify the array.
    }
  }

  private[core] def getArrayJdbcDataType(columnIndex: Int): Int = {
    val sparkType = getArrayDataType(columnIndex)

    sparkType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case StringType     => ARRAY_STRING
          case BooleanType    => ARRAY_BOOL
          case BinaryType     => ARRAY_BINARY
          case IntegerType    => ARRAY_INT
          case LongType       => ARRAY_LONG
          case FloatType      => ARRAY_FLOAT
          case DoubleType     => ARRAY_DOUBLE
          case d: DecimalType => ARRAY_DECIMAL
          case DateType       => ARRAY_DATE
          case TimestampType  => ARRAY_TIMESTAMP
          case _              => ARRAY_STRING
        }
      case _                    =>
        VARCHAR
    }
  }

  private[core] def getJdbcArray(columnIndex: Int): AnyRef = {
    val v = rs.getArray(columnIndex)
    if (rs.wasNull()) null else {
      v.getArray
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
  // Special data types
  val ARRAY_STRING = 10000
  val ARRAY_BINARY = 10001
  val ARRAY_BOOL = 10002
  val ARRAY_SHORT = 10003
  val ARRAY_INT = 10004
  val ARRAY_LONG = 10005
  val ARRAY_FLOAT = 10006
  val ARRAY_DOUBLE = 10007
  val ARRAY_DECIMAL = 10008
  val ARRAY_DATE = 10009
  val ARRAY_TIMESTAMP = 10010

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
