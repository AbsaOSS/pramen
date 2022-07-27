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

package za.co.absa.pramen.framework.utils.impl

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import java.sql.ResultSet
import java.sql.Types._

class ResultSetToRowIterator(rs: ResultSet) extends Iterator[Row] {
  private var didHasNext = false
  private var item: Option[Row] = None
  private val columnCount = rs.getMetaData.getColumnCount

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

  private def fetchNext(): Unit = {
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

  private def getStructField(columnIndex: Int): StructField = {
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
      case REAL          => StructField(columnName, DecimalType(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case NUMERIC       => StructField(columnName, DecimalType(rs.getMetaData.getPrecision(columnIndex), rs.getMetaData.getScale(columnIndex)))
      case DATE          => StructField(columnName, DateType)
      case TIMESTAMP     => StructField(columnName, TimestampType)
      case _             => StructField(columnName, StringType)
    }
  }

  private def getCell(columnIndex: Int): Any = {
    val dataType = rs.getMetaData.getColumnType(columnIndex)

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
      case DATE          => rs.getDate(columnIndex)
      case TIMESTAMP     => rs.getTimestamp(columnIndex)
      case _             => rs.getString(columnIndex)
    }
  }

}
