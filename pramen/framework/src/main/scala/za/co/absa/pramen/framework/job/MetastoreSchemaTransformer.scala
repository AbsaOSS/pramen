/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.job

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.metastore.MetastoreReader

import java.time.LocalDate

class MetastoreSchemaTransformer(metastoreReader: MetastoreReader,
                                 schemaTransformers: Map[String, Seq[TransformExpression]]) extends MetastoreReader {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame =
    transformDf(metastoreReader.getTable(tableName, infoDateFrom, infoDateTo), tableName)

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame =
    transformDf(metastoreReader.getLatest(tableName, until), tableName)

  override def getLatestAvailableDate(tableName: String, until: Option[LocalDate]): Option[LocalDate] =
    metastoreReader.getLatestAvailableDate(tableName, until)

  override def isDataAvailable(tableName: String, from: Option[LocalDate], until: Option[LocalDate]): Boolean =
    metastoreReader.isDataAvailable(tableName, from, until)

  private def transformDf(df: DataFrame, tableName: String): DataFrame = {
    val transformations = schemaTransformers.get(tableName)

    transformations match {
      case Some(t) => applyTransformations(df, t)
      case None => df
    }
  }

  private def applyTransformations(inputDf: DataFrame, transformExpressions: Seq[TransformExpression]): DataFrame = {
    transformExpressions.foldLeft(inputDf)((accDf, tf) => {
      if (tf.expression.isEmpty || tf.expression == "drop") {
        log.info(s"Dropping: ${tf.column}")
        accDf.drop(tf.column)
      } else {
        log.info(s"Applying: ${tf.column} <- ${tf.expression}")
        accDf.withColumn(tf.column, expr(tf.expression))
      }
    })
  }
}
