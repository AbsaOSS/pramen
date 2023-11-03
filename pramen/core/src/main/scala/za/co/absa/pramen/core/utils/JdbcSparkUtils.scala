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

  /**
    * Adds metadata to Spark fields based on JDBC metadata.
    *
    * Currently, the only metadata it adds is 'maxLength' for VARCHAR fields.
    *
    * All existing metadata fields stay the same.
    *
    * @param schema        A schema.
    * @param jdbcMetadata  The metadata obtained for the same query using native JDBC connection.
    * @return The schema with new metadata items added.
    */
  def addMetadataFromJdbc(schema: StructType, jdbcMetadata: ResultSetMetaData): StructType = {
    val fieldToMetadataMap: Map[String, JdbcFieldMetadata] = {
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

  /**
    * Connects to a database and executes a raw SQL query using Java JDBC, and allows running a custom action on the
    * metadata of the query.
    *
    * @param jdbcConfig  a JDBC configuration.
    * @param nativeQuery a SQL query in the dialect native to the database.
    * @param action      the action to execute.
    */
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

  /**
    * Executes a query against a JDBC connection and allows running an action on the result set.
    * Handles the closure of created objects.
    *
    * @param connection  a JDBC connection.
    * @param query       a SQL query in the dialect native to the database.
    * @param action      the action to execute on the result set.
    */
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

  /**
    * Converts all timestamp fields to dates in a data frame.
    */
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

  /**
    * Fixes issues found with decimal numbers due to various compatibility issues between relational database
    * systems and Spark type model.
    *
    * Fix precision flag handles the case when the database sends precision as the number integral of digits
    * instead of total digits. For example,
    * {{{
    *   # Precision cannot be smaller that scale, but due to different interpretations of various dbs this can happen.
    *   precision=5, scale=8 converts to precition=5+8=13, scale = 8
    * }}}
    *
    * @param df an input dataframe.
    * @param fixPrecision  if true, the source database interprets precision as integral part and scale as fractional part.
    * @return An optional custom schema string that can be applied when reading the JDBC source.
    */
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

  /**
    * Converts JDBC metadata of a specific field to the case class representation.
    *
    * @param jdbcMetadata a query metadata object that you can get from a result set (rs.getMetaData()).
    * @param fieldIndex   a field index (warning! the index starts from 1, not from 0).
    * @return an object defining the field.
    */
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

  /**
    * Converts JDBC connection parameters to Spark options that you can pass to Spark reader.
    *
    * @param url               a JDBC URL.
    * @param jdbcConfig        a JDBC configuration.
    * @param nativeQuery       a SQL query in the dialect native to the database.
    * @param extraSparkOptions extra Spark options to add to the result (e.g. 'fetchsize', 'batchsize' etc).
    * @return
    */
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
