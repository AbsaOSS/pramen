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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.jobdef.TransformExpression
import za.co.absa.pramen.api.{CatalogTable, FieldChange}
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.utils.SparkMaster.Databricks

import java.io.ByteArrayOutputStream
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object SparkUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  val MAX_LENGTH_METADATA_KEY = "maxLength"
  val COMMENT_METADATA_KEY = "comment"

  // This seems to be limitation for multiple catalogs, like Glue and Hive.
  val MAX_COMMENT_LENGTH = 255

  /** Get Spark StructType from a case class. */
  def getStructType[T: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  /**
    * Converts a given DataFrame into a formatted JSON string.
    *
    * @param df The DataFrame to be converted to JSON format.
    * @return A JSON string representation of the input DataFrame, formatted for readability.
    */
  def dataFrameToJson(df: DataFrame): String = {
    prettyJSON(df.toJSON
      .collect()
      .mkString("[", ",", "]"))
  }

  /**
    * Converts a DataFrame to a formatted JSON string with pretty-printing.
    *
    * @param df    The input DataFrame to be converted into JSON format.
    * @param takeN The number of rows to include in the JSON output.
    *              If set to 0 or less, all rows are included.
    * @return A formatted JSON string representing the DataFrame content.
    */
  def convertDataFrameToPrettyJSON(df: DataFrame, takeN: Int = 0): String = {
    val collected = if (takeN <= 0) {
      df.toJSON.collect().mkString("\n")
    } else {
      df.toJSON.take(takeN).mkString("\n")
    }

    val json = "[" + "}\n".r.replaceAllIn(collected, "},\n") + "]"

    prettyJSON(json)
  }

  /**
    * Formats a given JSON string into a more human-readable, pretty-printed format.
    *
    * @param jsonIn the input JSON string to be formatted. Must be a valid JSON.
    * @return a pretty-printed JSON string, with consistent indentation and line breaks.
    */
  def prettyJSON(jsonIn: String): String = {
    val mapper = new ObjectMapper()

    val jsonUnindented = mapper.readValue(jsonIn, classOf[Any])
    val indented = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(jsonUnindented)
    indented.replace("\r\n", "\n")
  }

  /**
    * Renames space characters in column names and removes uniform table prefix from all columns.
    *
    * This can be potentially improved by adding support for other special characters
    *
    * @param df         A dataframe to sanitize columns of
    * @param characters A set of characters considered special
    */
  def sanitizeDfColumns(df: DataFrame, characters: String): DataFrame = {
    def replaceSpecialChars(s: String): String = {
      s.map(c => if (characters.contains(c)) '_' else c)
    }

    def removeTablePrefix(s: String): String = {
      if (s.contains('.')) {
        s.split('.').drop(1).mkString(".")
      } else {
        s
      }
    }

    def hasUniformTablePrefix(fields: Array[StructField]): Boolean = {
      fields.map {
        field => field.name.split('.').head
      }.distinct.length == 1
    }

    val hasTablePrefix = hasUniformTablePrefix(df.schema.fields)

    val fieldsToSelect = df.schema.fields.map(field => {
      val srcName = field.name
      val trgName = replaceSpecialChars(if(hasTablePrefix) removeTablePrefix(srcName) else srcName)
      if (srcName != trgName) {
        log.info(s"Renamed column: $srcName -> $trgName")
        col(s"`$srcName`").as(trgName)
      } else {
        col(s"`$srcName`")
      }
    })

    if (characters.isEmpty)
      df
    else
      df.select(fieldsToSelect: _*)
  }

  /**
    * Converts JSON-encoded Spark schema to the schema type `StructType`.
    */
  def schemaFromJson(json: String): Option[StructType] = {
    Try {
      DataType.fromJson(json).asInstanceOf[StructType]
    } match {
      case Success(schema) => Option(schema)
      case Failure(e)      =>
        log.error(s"Failed to parse schema from JSON: $json", e)
        None
    }
  }

  /**
    * Compares 2 schemas.
    */
  def compareSchemas(schema1: StructType, schema2: StructType): List[FieldChange] = {
    def dataTypeToString(dt: DataType, metadata: Metadata): String = {
      val maxLength = getLengthFromMetadata(metadata).getOrElse(0)

      dt match {
        case _: StructType | _: ArrayType    => dt.simpleString
        case _: StringType if maxLength > 0  => s"varchar($maxLength)"
        case _                               => dt.typeName
      }
    }

    val fields1 = schema1.fields.map(f => (f.name, f)).toMap
    val fields2 = schema2.fields.map(f => (f.name, f)).toMap

    val newColumns: Array[FieldChange] = schema2.fields
      .filter(f => !fields1.contains(f.name))
      .map(f => FieldChange.NewField(f.name, dataTypeToString(f.dataType, f.metadata)))

    val deletedColumns: Array[FieldChange] = schema1.fields
      .filter(f => !fields2.contains(f.name))
      .map(f => FieldChange.DeletedField(f.name, dataTypeToString(f.dataType, f.metadata)))

    val changedType: Array[FieldChange] = schema1.fields
      .filter(f => fields2.contains(f.name))
      .flatMap(f1 => {
        val dt1 = dataTypeToString(f1.dataType, f1.metadata)
        val f2 = fields2(f1.name)
        val dt2 = dataTypeToString(f2.dataType, f2.metadata)

        if (dt1 == dt2) {
          Seq.empty[FieldChange]
        } else {
          Seq(FieldChange.ChangedType(f1.name, dt1, dt2))
        }
      })

    (newColumns ++ deletedColumns ++ changedType).toList
  }

  /**
    * Applies transformations as custom Spark expression to the specified dataframe.
    *
    * @param df              The input dataframe
    * @param transformations The list of transformations
    * @return The dataframe with all transformations applied
    */
  def applyTransformations(df: DataFrame, transformations: Seq[TransformExpression]): DataFrame = {
    transformations.foldLeft(df)((accDf, tf) => {
      (tf.expression, tf.comment) match {
        case (Some(expression), _) if expression.isEmpty || expression.trim.equalsIgnoreCase("drop") =>
          log.info(s"Dropping: ${tf.column}")
          accDf.drop(tf.column)
        case (Some(expression), Some(comment)) =>
          log.info(s"Applying: ${tf.column} <- $expression ($comment)")
          val metadata = new MetadataBuilder()
          metadata.putString("comment", comment)
          accDf.withColumn(tf.column, expr(expression).as(tf.column, metadata.build()))
        case (Some(expression), None) =>
          log.info(s"Applying: ${tf.column} <- $expression")
          accDf.withColumn(tf.column, expr(expression))
        case (None, Some(comment)) =>
          log.debug(s"Adding comment '$comment' to ${tf.column}")
          val metadata = new MetadataBuilder()
          metadata.putString("comment", comment)
          accDf.withColumn(tf.column, col(tf.column).as(tf.column, metadata.build()))
        case (None, None) =>
          log.info(s"Dropping: ${tf.column}")
          accDf.drop(tf.column)
      }
    })
  }

  /**
    * Applies a series of string-based filters to a DataFrame within a specific date range.
    *
    * @param df       The input DataFrame to be filtered.
    * @param filters  A sequence of filter expressions to be applied.
    * @param infoDate The reference date to be used in filter expressions.
    * @param dateFrom The starting date of the date range for filtering.
    * @param dateTo   The ending date of the date range for filtering.
    * @return A DataFrame with the applied filters.
    */
  def applyFilters(df: DataFrame, filters: Seq[String], infoDate: LocalDate, dateFrom: LocalDate, dateTo: LocalDate): DataFrame = {
    filters.foldLeft(df)((df, filter) => {
      val exprEvaluator = new DateExprEvaluator()

      exprEvaluator.setValue("dateFrom", dateFrom)
      exprEvaluator.setValue("dateTo", dateTo)
      exprEvaluator.setValue("date", infoDate)

      val withInfoDate = filter.replaceAll("@infoDate", s"date'${infoDate.toString}'")
      val actualFilter = StringUtils.replaceFormattedDateExpression(withInfoDate, exprEvaluator)

      log.info(s"Applying filter: $actualFilter")
      df.filter(expr(actualFilter))
    })
  }

  /**
    * Transforms the schema to the format compatible with Hive-like catalogs.
    * - Removes non-nullable flag since it is not compatible with catalogs
    * - Switches from string to varchar(n) when the maximum field length is known
    *
    * @param schema The input schema.
    * @return The transformed schema.
    */
  def transformSchemaForCatalog(schema: StructType): StructType = {
    def transformField(field: StructField): StructField = {
      val metadata = if (field.metadata.contains("comment")) {
        val comment = field.metadata.getString("comment")
        val fixedComment = sanitizeCommentForHiveDDL(comment)
        new MetadataBuilder()
          .withMetadata(field.metadata)
          .putString("comment", fixedComment)
          .build()
      } else {
        field.metadata
      }

      field.dataType match {
        case struct: StructType => StructField(field.name, transformStruct(struct), nullable = true, metadata)
        case arr: ArrayType => StructField(field.name, transformArray(arr, field), nullable = true, metadata)
        case dataType: DataType => StructField(field.name, transformPrimitive(dataType, field), nullable = true, metadata)
      }
    }

    def transformPrimitive(dataType: DataType, field: StructField): DataType = {
      dataType match {
        case _: StringType =>
          getLengthFromMetadata(field.metadata) match {
            case Some(n) => VarcharType(n)
            case None => StringType
          }
        case _ =>
          dataType
      }
    }

    def transformArray(arr: ArrayType, field: StructField): ArrayType = {
      arr.elementType match {
        case struct: StructType => ArrayType(transformStruct(struct), arr.containsNull)
        case arr: ArrayType     => ArrayType(transformArray(arr, field))
        case dataType: DataType => ArrayType(transformPrimitive(dataType, field), arr.containsNull)
      }
    }

    def transformStruct(struct: StructType): StructType = {
      StructType(struct.fields.map(transformField))
    }

    transformStruct(schema)
  }

  /**
    * Extracts the maximum length value from the provided metadata if it exists.
    *
    * @param metadata the metadata object from which the length value should be retrieved
    * @return an `Option` containing the length as an `Int` if the key exists and can be parsed,
    *         otherwise `None`
    */
  def getLengthFromMetadata(metadata: Metadata): Option[Int] = {
    if (metadata.contains(MAX_LENGTH_METADATA_KEY)) {
      val try1 = Try {
        val length = metadata.getLong(MAX_LENGTH_METADATA_KEY).toInt
        Option(length)
      }
      val try2 = if (try1.isFailure) {
        Try {
          val length = metadata.getString(MAX_LENGTH_METADATA_KEY).toInt
          Option(length)
        }
      } else {
        try1
      }
      try2.getOrElse(None)
    } else {
      None
    }
  }

  /**
    * Sanitizes a comment for Hive DDL. Ideally this should be done by Spark, but because there are meny versions
    * of Hive and other catalogs, it is sometimes hard to have an general solution.
    *
    * These transformations are tested for Hive 1.0.
    *
    * @param comment The comment (description) of a column or a table.
    * @return
    */
  def sanitizeCommentForHiveDDL(comment: String): String = {
    val escapedComment = comment
      .replace("\n", " ") // This breaks DBeaver (shows no columns)
      .replace("\\n", " ") // This breaks DBeaver (shows no columns)

    if (escapedComment.length > MAX_COMMENT_LENGTH) {
      s"${escapedComment.take(MAX_COMMENT_LENGTH - 3)}..."
    } else {
      escapedComment
    }
  }

  /**
    * Removes metadata of nested fields to make DDL compatible with some Hive-like catalogs.
    * In addition, removes the nullability flag for all fields.
    *
    * This method is usually applied to make schemas comparable when reading a table from a data catalog.
    *
    * @param schema The input schema.
    * @return The transformed schema.
    */
  def removeNestedMetadata(schema: StructType): StructType = {
    def transformRootField(field: StructField): StructField = {
      field.dataType match {
        case struct: StructType => StructField(field.name, transformStruct(struct), nullable = true, field.metadata)
        case arr: ArrayType => StructField(field.name, transformArray(arr), nullable = true, field.metadata)
        case _: DataType => StructField(field.name, field.dataType, nullable = true, field.metadata)
      }
    }

    def transformNestedField(field: StructField): StructField = {
      field.dataType match {
        case struct: StructType => StructField(field.name, transformStruct(struct), nullable = true)
        case arr: ArrayType => StructField(field.name, transformArray(arr), nullable = true)
        case dataType: DataType => StructField(field.name, dataType, nullable = true)
      }
    }

    def transformStruct(struct: StructType): StructType = {
      StructType(struct.fields.map(transformNestedField))
    }

    def transformArray(array: ArrayType): ArrayType = {
      array.elementType match {
        case struct: StructType => ArrayType(transformStruct(struct), containsNull = true)
        case arr: ArrayType => ArrayType(transformArray(arr), containsNull = true)
        case dataType: DataType => ArrayType(dataType, containsNull = true)
      }
    }

    StructType(schema.fields.map(transformRootField))
  }

  /**
    * Checks whether there is valid data in the specified partition.
    * A partition exists and contains data files if it is neither empty
    * nor contains only hidden files (starting with "_" or ".").
    *
    * @param infoDate       The specific date for which partition data is checked.
    * @param infoDateColumn The name of the date column used to define the partition.
    * @param infoDateFormat The format of the date in the partition path.
    * @param basePath       The base path of the data being checked for the partition.
    * @param spark          The implicit SparkSession needed for filesystem operations.
    * @return `true` if the partition exists and contains at least one valid data file; `false` otherwise.
    */
  def hasDataInPartition(infoDate: LocalDate,
                         infoDateColumn: String,
                         infoDateFormat: String,
                         basePath: String)(implicit spark: SparkSession): Boolean = {
    val path = getPartitionPath(infoDate, infoDateColumn, infoDateFormat, basePath)

    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val hasPartition = fs.exists(path)
    if (hasPartition) {
      val pathFilter = new PathFilter {
        override def accept(path: Path): Boolean = {
          val name = path.getName
          !name.startsWith("_") && !name.startsWith(".")
        }
      }
      val filesInTheFolder = fs.globStatus(new Path(path, "*"), pathFilter).nonEmpty
      if (filesInTheFolder) {
        true
      } else {
        log.warn(s"No content in: $path")
        false
      }
    } else {
      log.warn(s"No partition: $path")
      false
    }
  }

  /**
    * Constructs a partition path based on the provided information date, column name, date format, and base path.
    *
    * @param infoDate       The information date used to generate the partition path.
    * @param infoDateColumn The name of the column that represents the information date.
    * @param infoDateFormat The date format of the information date, used for formatting.
    * @param basePath       The base path to which the partition path will be appended.
    * @return The constructed partition path as a Path object.
    */
  def getPartitionPath(infoDate: LocalDate,
                       infoDateColumn: String,
                       infoDateFormat: String,
                       basePath: String): Path = {
    val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

    val partition = s"$infoDateColumn=${dateFormatter.format(infoDate)}"
    new Path(basePath, partition)
  }

  /**
    * Adds a processing timestamp column to the given DataFrame.
    * If the specified timestamp column already exists in the DataFrame, no
    * changes are made, and a warning is logged.
    *
    * The processing time is determined at the executor node at the time the record
    * was actually processed, in comparison to `current_timestamp()` which is determined
    * at the driver node.
    *
    * @param df           The input DataFrame to which the timestamp column will be added.
    * @param timestampCol The name of the column to be added as the processing timestamp.
    * @return A new DataFrame with the processing timestamp column added, or the original DataFrame
    *         if the specified column already exists.
    */
  def addProcessingTimestamp(df: DataFrame, timestampCol: String): DataFrame = {
    if (df.schema.exists(_.name == timestampCol)) {
      log.warn(s"Column $timestampCol already exists. Won't add it.")
      df
    } else {
      val u = getActualProcessingTimeUdf
      df.withColumn(timestampCol, u(unix_timestamp()).cast(TimestampType))
    }
  }

  /**
    * This helper method returns output of `df.show()` to a string
    *
    * @param df      a dataframe to show
    * @param numRows the maximum number of rows to show
    * @return
    */
  def showString(df: DataFrame, numRows: Int = 20): String = {
    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(numRows, truncate = false)
    }
    new String(outCapture.toByteArray).replace("\r\n", "\n")
  }

  /**
    * Collects data from a DataFrame and returns it as a two-dimensional array of strings.
    * The first row contains the column names (headers) and the subsequent rows contain the data.
    * The number of rows collected can be limited by specifying a maximum number of records.
    *
    * @param df         The input DataFrame to collect data from.
    * @param maxRecords The maximum number of rows to collect. If set to 0 or less, all rows are collected. Default is 200.
    * @return A two-dimensional array of strings where the first row contains headers, and each subsequent row contains the data.
    */
  def collectTable(df: DataFrame, maxRecords: Int = 200): Array[Array[String]] = {
    val collected = if (maxRecords > 0) {
      df.take(maxRecords)
    } else {
      df.collect()
    }

    val headers = df.schema.fields.map(_.name)
    val rows = collected.map(row => {
      val arr = new ArrayBuffer[String]
      var i = 0
      val len = row.length

      while (i < len) {
        val v = row.get(i)
        val vs = if (v == null) "null" else v.toString

        arr.append(vs)
        i += 1
      }
      arr.toArray[String]
    })

    headers +: rows
  }

  /**
    * Escapes all columns in Spark DDL. Used to make sure column names are not reserved words.
    *
    * Example:
    * {{{
    *   Id INT NOT NULL,Name STRING,`System Date` ARRAY<STRING>
    * }}}
    * becomes
    * {{{
    *   `Id` INT NOT NULL,`Name` STRING,`System Date` ARRAY<STRING>
    * }}}
    *
    * @param sparkDdlExpr An expression as a Spark DDL (df.schema.toDDL)
    * @return The same DDL with all column names escaped.
    */
  def escapeColumnsSparkDDL(sparkDdlExpr: String): String = {
    val STATE_WHITESPACE_OR_ID = 0
    val STATE_POSSIBLE_ID = 1
    val STATE_DATA_TYPE = 2
    val STATE_PARENTHESIS = 3
    val STATE_QUOTES = 4
    val STATE_ESCAPE = 5

    val output = new StringBuilder()
    val token = new StringBuilder()

    var state = 0
    var depth = 0
    var i = 0
    val len = sparkDdlExpr.length

    while (i < len) {
      val c = sparkDdlExpr(i)
      if (state == STATE_WHITESPACE_OR_ID) {
        if (c == '<' || c == ':') {
          output.append(s"$token")
          token.clear()
          state = STATE_DATA_TYPE
        } else if (c == ' ') {
          token.append(c)
        } else {
          output.append(s"$token")
          token.clear()
          state = STATE_POSSIBLE_ID
        }
      }

      if (state == 1) {
        if (c == '(') {
          depth = 1
          state = STATE_PARENTHESIS
          token.append(c)
        } else if (c == '\'') {
          state = STATE_QUOTES
          token.append(c)
        } else if (c == '\\') {
          state = STATE_ESCAPE
          token.append(c)
        } else if (c == ' ' || c == ':') {
          state = STATE_DATA_TYPE
          val tokenStr = token.toString().trim
          if (tokenStr.isEmpty) {
            output.append(s"$tokenStr$c")
          } else if (tokenStr.head == '`') {
            output.append(s"$tokenStr$c")
          } else {
            output.append(s"`$tokenStr`$c")
          }
          token.clear()
        } else if (c == ',' || c == '<') {
          output.append(s"$token$c")
          token.clear()
          state = STATE_WHITESPACE_OR_ID
        } else if (c == '>') {
          output.append(s"$token$c")
          token.clear()
          state = STATE_DATA_TYPE
        } else {
          token.append(c)
        }
      } else if (state == STATE_DATA_TYPE) {
        if (c == '(') {
          depth = 1
          state = STATE_PARENTHESIS
        } else if (c == '\'') {
          state = STATE_QUOTES
        } else if (c == '\\') {
          state = STATE_ESCAPE
        }
        if (c == ',') {
          state = STATE_WHITESPACE_OR_ID
          output.append(s"$token$c")
          token.clear()
        } else if (c == '<') {
          state = STATE_WHITESPACE_OR_ID
          output.append(s"$token$c")
          token.clear()
        } else {
          token.append(c)
        }
      } else if (state == STATE_PARENTHESIS) {
        if (c == ')') {
          depth -= 1
          if (depth == 0)
            state = STATE_DATA_TYPE
        } else if (c == '(')
          depth += 1
        token.append(c)
      } else if (state == STATE_QUOTES) {
        if (c == '\'') {
          state = STATE_DATA_TYPE
        }
        token.append(c)
      } else if (state == STATE_ESCAPE) {
        state = STATE_DATA_TYPE
        token.append(c)
      }
      i += 1
    }
    if (token.nonEmpty) {
      output.append(token.toString())
    }
    output.toString()
  }

  /**
    * Determines whether a specified catalog table exists within the Spark session.
    * Takes into account Iceberg and Glue Catalog specifics.
    *
    * @param table The `CatalogTable` object representing the table to check for existence.
    * @param spark The implicit `SparkSession` in which the catalog and table existence checks will be performed.
    * @return `true` if the table exists in the specified catalog and database; otherwise, `false`.
    *         Throws an `IllegalArgumentException` if the database does not exist or if there are issues with the table's catalog context.
    */
  def doesCatalogTableExist(table: CatalogTable)(implicit spark: SparkSession): Boolean = {
    try {
      table.database match {
        case Some(_) if table.catalog.nonEmpty =>
          spark.catalog.tableExists(table.getFullTableName)
        case Some(db) =>
          if (spark.catalog.databaseExists(db)) {
            spark.catalog.tableExists(db, table.table)
          } else {
            throw new IllegalArgumentException(s"Database '$db' not found")
          }
        case None =>
          spark.catalog.tableExists(table.table)
      }
    } catch {
      case _: AnalysisException =>
        // Workaround for Iceberg tables stored in Glue
        // The error is:
        //   Caused by org.apache.spark.sql.AnalysisException: org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table my_test_table
        // Don't forget that Iceberg requires lowercase names as well.
        try {
          spark.read.table(table.getFullTableName)
          true
        } catch {
          // This is a common error
          case ex: AnalysisException if ex.getMessage().contains("Table or view not found") || ex.getMessage().contains("TABLE_OR_VIEW_NOT_FOUND") => false
          // This is the exception, needs to be re-thrown.
          case ex: AnalysisException if ex.getMessage().contains("TableType cannot be null for table:") =>
            throw new IllegalArgumentException("Attempt to use a catalog not supported by the file format. " +
              "Ensure you are using the iceberg/delta catalog and/or it is set as the default catalog with (spark.sql.defaultCatalog) " +
              "or the catalog is specified explicitly as the table name.", ex)
          // If the exception is not AnalysisException, something is wrong so the original exception is thrown.
          //case _: AnalysisException => false
        }
    }
  }

  /**
    * Retrieves a nested field from a given StructType schema based on the provided dot-separated field name.
    *
    * @param schema    The root StructType schema to search for the nested field.
    * @param fieldName A dot-separated string representing the hierarchical path to the desired field.
    * @return The StructField corresponding to the specified nested field name.
    * @throws IllegalArgumentException If the field name is invalid,
    *                                  the field cannot be found in the schema,
    *                                  or a non-struct field is encountered along the path.
    */
  def getNestedField(schema: StructType, fieldName: String): StructField = {
    def getNestedFieldInArray(schema: StructType, fieldNames: Array[String]): StructField = {
      val rootFieldName = fieldNames.head
      val rootFieldOpt = schema.fields.find(_.name.equalsIgnoreCase(rootFieldName))

      rootFieldOpt match {
        case Some(field) =>
          if (fieldNames.length == 1) {
            field
          } else {
            field.dataType match {
              case struct: StructType =>
                getNestedFieldInArray(struct, fieldNames.drop(1))
              case other =>
                throw new IllegalArgumentException(s"Field '${field.name}' (of $fieldName) is of type '${other.typeName}', expected StructType.")
            }
          }
        case None =>
          val fields = schema.fields.map(_.name).mkString("[ ", ", ", " ]")
          throw new IllegalArgumentException(s"Field $rootFieldName (of '$fieldName') not found in the schema. Available fields: $fields")
      }
    }

    val fieldNames = fieldName.split('.')

    if (fieldNames.length < 1) {
      throw new IllegalArgumentException(s"Field name '$fieldName' is not valid.")
    }

    getNestedFieldInArray(schema, fieldNames)
  }

  /**
    * Determines if the Spark driver is running on an edge node.
    *
    * This method evaluates the current Spark master and deployment mode to check if the driver
    * is running locally or in a YARN client deployment mode. If the Spark master is `local` or
    * if the deployment mode is `client` in a YARN environment, the method returns true,
    * indicating the driver is running on an edge node. Otherwise, it returns false.
    *
    * @param master The Spark master definition.
    * @return A boolean value where `true` indicates the driver is running on an edge node
    *         and `false` otherwise.
    */
  def isDriverRunningOnEdgeNode(master: SparkMaster): Boolean = {
    master match {
      case _: SparkMaster.Local                                                 => true
      case m: SparkMaster.Yarn if m.deploymentMode == YarnDeploymentMode.Client => true
      case _                                                                    => false
    }
  }

  /**
    * Determines the type of Spark master and its deployment mode currently being used in the Spark session.
    *
    * The method examines the configuration of the provided Spark session to identify the Spark master
    * and its deployment mode, supporting various environments such as local, standalone, YARN, Kubernetes,
    * and Databricks. If none of these can be determined, the master type is categorized as unknown.
    *
    * @param spark The implicit Spark session whose configuration is used to determine the Spark master.
    * @return The Spark master, represented as a `SparkMaster` instance, describing the execution environment
    *         and potentially its deployment mode.
    */
  def getSparkMaster(implicit spark: SparkSession): SparkMaster = {
    val conf = spark.sparkContext.getConf

    val master = conf.getOption("spark.master").map(_.toLowerCase).getOrElse("unknown")
    val deployMode = conf.getOption("spark.submit.deployMode").getOrElse("client").toLowerCase

    val isDatabricks = sys.env.contains("DATABRICKS_RUNTIME_VERSION") ||
      conf.getOption("spark.databricks.clusterUsageTags.clusterName").isDefined

    if (isDatabricks) return Databricks

    val masterType =
      if (master.startsWith("local")) "local"
      else if (master.startsWith("spark://")) "standalone"
      else if (master == "yarn") "yarn"
      else if (master.startsWith("k8s://")) "kubernetes"
      else master

    (masterType, deployMode) match {
      case ("local", _) =>
        SparkMaster.Local(master)
      case ("standalone", _) =>
        SparkMaster.Standalone(master)
      case ("yarn", "cluster") =>
        SparkMaster.Yarn(YarnDeploymentMode.Cluster)
      case ("yarn", "client") =>
        SparkMaster.Yarn(YarnDeploymentMode.Client)
      case ("kubernetes", _) =>
        SparkMaster.Kubernetes(master)
      case _ =>
        SparkMaster.Unknown(master)
    }
  }

  private def getActualProcessingTimeUdf: UserDefinedFunction = {
    udf((_: Long) => Instant.now().getEpochSecond)
  }

}
