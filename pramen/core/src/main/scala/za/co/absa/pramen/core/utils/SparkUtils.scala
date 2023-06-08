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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType, TimestampType}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.notify.pipeline.FieldChange
import za.co.absa.pramen.core.pipeline.TransformExpression

import java.io.ByteArrayOutputStream
import java.time.{Instant, LocalDate}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object SparkUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

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
    def dataTypeToString(dt: DataType): String = {
      dt match {
        case _: StructType | _: ArrayType => dt.simpleString
        case _                            => dt.typeName
      }
    }

    val fields1 = schema1.fields.map(f => (f.name, f)).toMap
    val fields2 = schema2.fields.map(f => (f.name, f)).toMap

    val newColumns: Array[FieldChange] = schema2.fields
      .filter(f => !fields1.contains(f.name))
      .map(f => FieldChange.NewField(f.name, dataTypeToString(f.dataType)))

    val deletedColumns: Array[FieldChange] = schema1.fields
      .filter(f => !fields2.contains(f.name))
      .map(f => FieldChange.DeletedField(f.name, dataTypeToString(f.dataType)))

    val changedType: Array[FieldChange] = schema1.fields
      .filter(f => fields2.contains(f.name))
      .flatMap(f => {
        val dt1 = dataTypeToString(f.dataType)
        val dt2 = dataTypeToString(fields2(f.name).dataType)

        if (dt1 == dt2) {
          Seq.empty[FieldChange]
        } else {
          Seq(FieldChange.ChangedType(f.name, dt1, dt2))
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
      if (tf.expression.isEmpty || tf.expression == "drop") {
        log.info(s"Dropping: ${tf.column}")
        accDf.drop(tf.column)
      } else {
        log.info(s"Applying: ${tf.column} <- ${tf.expression}")
        accDf.withColumn(tf.column, expr(tf.expression))
      }
    })
  }

  def applyFilters(df: DataFrame, filters: Seq[String], infoDate: LocalDate, dateFrom: LocalDate, dateTo: LocalDate): DataFrame = {
    filters.foldLeft(df)((df, filter) => {
      val actualFilter = filter
        .replaceAll("@dateFrom", s"${dateFrom.toString}")
        .replaceAll("@dateTo", s"${dateTo.toString}")
        .replaceAll("@date", s"${infoDate.toString}")
        .replaceAll("@infoDate", s"date'${infoDate.toString}'")
      log.info(s"Applying filter: $actualFilter")
      df.filter(expr(actualFilter))
    })
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

  private def getActualProcessingTimeUdf: UserDefinedFunction = {
    udf((_: Long) => Instant.now().getEpochSecond)
  }

}
