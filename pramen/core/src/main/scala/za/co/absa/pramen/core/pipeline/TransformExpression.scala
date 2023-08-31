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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

import scala.collection.JavaConverters._

case class TransformExpression(
                              column: String,
                              expression: Option[String],
                              comment: Option[String]
                              )

object TransformExpression {
  val COLUMN_KEY = "col"
  val EXPRESSION_KEY = "expr"
  val COMMENT_KEY = "comment"

  def fromConfigSingleEntry(conf: Config, parentPath: String): TransformExpression = {
    if (!conf.hasPath(COLUMN_KEY)) {
      throw new IllegalArgumentException(s"'$COLUMN_KEY' not set for the transformation in $parentPath' in the configuration.")
    }
    val col = conf.getString(COLUMN_KEY)

    if (!conf.hasPath(EXPRESSION_KEY) && !conf.hasPath(COMMENT_KEY)) {
      throw new IllegalArgumentException(s"Either '$EXPRESSION_KEY' or '$COMMENT_KEY' should be defined for for the transformation of '$col' in $parentPath' in the configuration.")
    }

    val expr = ConfigUtils.getOptionString(conf, EXPRESSION_KEY)
    val comment = ConfigUtils.getOptionString(conf, COMMENT_KEY)

    TransformExpression(col, expr, comment)
  }

  def fromConfig(conf: Config, arrayPath: String, parentPath: String): Seq[TransformExpression] = {
    if (conf.hasPath(arrayPath)) {
      val transformationConfigs = conf.getConfigList(arrayPath).asScala

      val transformations = transformationConfigs
        .zipWithIndex
        .map { case (transformationConfig, idx) => fromConfigSingleEntry(transformationConfig, s"$parentPath.$arrayPath[$idx]") }

      transformations.toSeq
    } else {
      Seq.empty[TransformExpression]
    }
  }
}