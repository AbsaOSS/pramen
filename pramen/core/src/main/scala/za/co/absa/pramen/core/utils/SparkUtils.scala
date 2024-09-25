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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.FieldChange
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.pipeline.TransformExpression

import java.io.ByteArrayOutputStream
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object SparkUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  val MAX_LENGTH_METADATA_KEY = "maxLength"
  val COMMENT_METADATA_KEY = "comment"

  /** Get Spark StructType from a case class. */
  def getStructType[T: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  def dataFrameToJson(df: DataFrame): String = {
    prettyJSON(df.toJSON
      .collect()
      .mkString("[", ",", "]"))
  }

  // This is taken from
  // https://github.com/AbsaOSS/cobrix/blob/master/spark-cobol/src/main/scala/za/co/absa/cobrix/spark/cobol/utils/SparkUtils.scala */
  def convertDataFrameToPrettyJSON(df: DataFrame, takeN: Int = 0): String = {
    val collected = if (takeN <= 0) {
      df.toJSON.collect().mkString("\n")
    } else {
      df.toJSON.take(takeN).mkString("\n")
    }

    val json = "[" + "}\n".r.replaceAllIn(collected, "},\n") + "]"

    prettyJSON(json)
  }

  def prettyJSON(jsonIn: String): String = {
    val mapper = new ObjectMapper()

    val jsonUnindented = mapper.readValue(jsonIn, classOf[Any])
    val indented = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(jsonUnindented)
    indented.replace("\r\n", "\n")
  }

  /**
    * Renames space characters in column names.
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

    val fieldsToSelect = df.schema.fields.map(field => {
      val srcName = field.name
      val trgName = replaceSpecialChars(removeTablePrefix(srcName))
      if (srcName != trgName) {
        log.info(s"Renamed column: $srcName -> $trgName")
        col(s"`$srcName`").as(trgName)
      } else {
        col(srcName)
      }
    })

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
      val maxLength = if (metadata.contains(MAX_LENGTH_METADATA_KEY)) {
        Try {
          metadata.getLong(MAX_LENGTH_METADATA_KEY).toInt
        }.getOrElse(0)
      } else {
        0
      }

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
      field.dataType match {
        case struct: StructType =>  StructField(field.name, transformStruct(struct), nullable = true, field.metadata)
        case arr: ArrayType     =>  StructField(field.name, transformArray(arr, field), nullable = true, field.metadata)
        case dataType: DataType =>  StructField(field.name, transformPrimitive(dataType, field), nullable = true, field.metadata)
      }
    }

    def transformPrimitive(dataType: DataType, field: StructField): DataType = {
      dataType match {
        case _: StringType =>
          if (field.metadata.contains(MAX_LENGTH_METADATA_KEY)) {
            val length = field.metadata.getLong(MAX_LENGTH_METADATA_KEY).toInt
            VarcharType(length)
          } else {
            StringType
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

  def getPartitionPath(infoDate: LocalDate,
                       infoDateColumn: String,
                       infoDateFormat: String,
                       basePath: String): Path = {
    val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

    val partition = s"$infoDateColumn=${dateFormatter.format(infoDate)}"
    new Path(basePath, partition)
  }

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

  private def getActualProcessingTimeUdf: UserDefinedFunction = {
    udf((_: Long) => Instant.now().getEpochSecond)
  }

}
