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

package za.co.absa.pramen.core.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DecimalType, MetadataBuilder, StringType, StructType, TimestampType}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.SparkUtils.MAX_LENGTH_METADATA_KEY
import za.co.absa.pramen.core.utils.impl.JdbcFieldMetadata

import java.sql.{Connection, ResultSet, ResultSetMetaData}
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object JdbcSparkUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  val MAXIMUM_VARCHAR_LENGTH = 8192

  def addMetadataFromJdbc(schema: StructType, jdbcMetadata: ResultSetMetaData): StructType = {
    val fieldToMetadataMap: Map[String, JdbcFieldMetadata] =
      {
        for (index <- Range.inclusive(1, jdbcMetadata.getColumnCount))
          yield {
            val fieldMetadata = getFieldMetadata(jdbcMetadata, index)
            val name = fieldMetadata.name.toLowerCase
            name -> fieldMetadata
          }
      }.toMap

    StructType(
      schema.fields.map {
        field =>
          val fieldNameLowerCase = field.name.toLowerCase
          field.dataType match {
            case StringType if fieldToMetadataMap.contains(fieldNameLowerCase) =>
              val jdbcMetadata = fieldToMetadataMap(fieldNameLowerCase)
              val maxLength = Math.max(jdbcMetadata.displaySize, jdbcMetadata.precision)
              if (maxLength > 0 && maxLength < MAXIMUM_VARCHAR_LENGTH) {
                val metadata = new MetadataBuilder
                metadata.withMetadata(field.metadata)
                metadata.putLong(MAX_LENGTH_METADATA_KEY, maxLength)
                field.copy(metadata = metadata.build())
              } else {
                field
              }
            case _ =>
              field
          }
      }
    )
  }

  def withJdbcMetadata(jdbcConfig: JdbcConfig,
                       nativeQuery: String)
                      (action: ResultSetMetaData => Unit): Unit = {
    val (url, connection) = JdbcNativeUtils.getConnection(jdbcConfig)

    log.info(s"Successfully connected to JDBC URL: $url")

    try {
      withResultSet(connection, nativeQuery) { rs =>
        action(rs.getMetaData)
      }
    } finally {
      connection.close()
    }
  }

  def withResultSet(connection: Connection,
                           query: String)
                           (action: ResultSet => Unit): Unit = {
    val statement = try {
      connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    } catch {
      case _: java.sql.SQLException =>
        // Fallback with more permissible result type.
        // JDBC sources should automatically downgrade result type, but Denodo driver doesn't do that.
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      case NonFatal(ex) =>
        throw ex
    }

    try {
      val resultSet = statement.executeQuery(query)
      try {
        action(resultSet)
      } finally {
        resultSet.close()
      }
    } finally {
      statement.close()
    }
  }

  def convertTimestampToDates(df: DataFrame): DataFrame = {
    val dateColumns = new ListBuffer[String]

    val newFields = df.schema.fields.map(fld => {
      fld.dataType match {
        case TimestampType =>
          dateColumns += fld.name
          col(fld.name).cast(DateType).as(fld.name)
        case _ =>
          col(fld.name)
      }
    })

    if (dateColumns.nonEmpty) {
      log.info(s"The following fields have been converted to Date: ${dateColumns.mkString(", ")}")
      df.select(newFields: _*)
    } else {
      log.debug("No timestamp fields found in the dataset.")
      df
    }
  }

  def getCorrectedDecimalsSchema(df: DataFrame, fixPrecision: Boolean): Option[String] = {
    val newSchema = new ListBuffer[String]

    df.schema.fields.foreach(field => {
      field.dataType match {
        case t: DecimalType if t.scale == 0 && t.precision <= 9 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to int")
          newSchema += s"${field.name} integer"
        case t: DecimalType if t.scale == 0 && t.precision <= 18 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to long")
          newSchema += s"${field.name} long"
        case t: DecimalType if t.scale >= 18 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to decimal(38, 18)")
          newSchema += s"${field.name} decimal(38, 18)"
        case t: DecimalType if fixPrecision && t.scale > 0 =>
          val fixedPrecision = if (t.precision + t.scale > 38) 38 else t.precision + t.scale
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to decimal($fixedPrecision, ${t.scale})")
          newSchema += s"${field.name} decimal($fixedPrecision, ${t.scale})"
        case _ =>
          field
      }
    })

    if (newSchema.nonEmpty) {
      val customSchema = newSchema.mkString(", ")
      log.info(s"Custom schema: $customSchema")
      Some(customSchema)
    } else {
      None
    }
  }

  def getFieldMetadata(jdbcMetadata: ResultSetMetaData, fieldIndex: Int): JdbcFieldMetadata = {
    val name = jdbcMetadata.getColumnName(fieldIndex).trim
    val label = jdbcMetadata.getColumnLabel(fieldIndex).trim
    val sqlType = jdbcMetadata.getColumnType(fieldIndex)
    val sqlTypeName = jdbcMetadata.getColumnTypeName(fieldIndex)
    val displaySize = jdbcMetadata.getColumnDisplaySize(fieldIndex)
    val precision = jdbcMetadata.getPrecision(fieldIndex)
    val scale = jdbcMetadata.getScale(fieldIndex)
    val nullable = jdbcMetadata.isNullable(fieldIndex) != ResultSetMetaData.columnNoNulls

    val effectiveName = if (name.isEmpty) label else name

    JdbcFieldMetadata(effectiveName, label, sqlType, sqlTypeName, displaySize, precision, scale, nullable)
  }

  def getJdbcOptions(url: String,
                     jdbcConfig: JdbcConfig,
                     nativeQuery: String,
                     extraSparkOptions: Map[String, String] = Map.empty): Map[String, String] = {
    def getOptions(optionName: String, optionValue: Option[Any]): Map[String, String] = {
      optionValue match {
        case Some(value) =>
          Map[String, String](optionName -> value.toString)
        case None =>
          Map[String, String]()
      }
    }

    val databaseOptions = getOptions("database", jdbcConfig.database)
    val userOptions = getOptions("user", jdbcConfig.user)
    val passwordOptions = getOptions("password", jdbcConfig.password)

    Map[String, String](
      "url" -> url,
      "driver" -> jdbcConfig.driver,
      "dbtable" -> nativeQuery
    ) ++
      userOptions ++
      passwordOptions ++
      databaseOptions ++
      jdbcConfig.extraOptions ++
      extraSparkOptions
  }
}
