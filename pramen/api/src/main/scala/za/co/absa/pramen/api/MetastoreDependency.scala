/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.api

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

case class MetastoreDependency(
                                tables: Seq[String],
                                dateFromExpr: String,
                                dateUntilExpr: Option[String],
                                triggerUpdates: Boolean,
                                isOptional: Boolean
                              )

object MetastoreDependency {
  val TABLES_KEY = "tables"
  val DATE_FROM_EXPR_KEY = "date.from"
  val DATE_UNTIL_EXPR_KEY = "date.to"
  val TRIGGER_UPDATES_KEY = "trigger.updates"
  val OPTIONAL_KEY = "optional"

  def fromConfig(conf: Config, path: String): MetastoreDependency = {
    if (!conf.hasPath(TABLES_KEY)) {
      throw new IllegalArgumentException(s"Missing required key '$path.$TABLES_KEY' in config")
    }
    if (!conf.hasPath(DATE_FROM_EXPR_KEY)) {
      throw new IllegalArgumentException(s"Missing required key '$path.$DATE_FROM_EXPR_KEY' in config")
    }

    val tables = conf.getStringList(TABLES_KEY).asScala
    val dateFromExpr = conf.getString(DATE_FROM_EXPR_KEY)
    val triggerUpdates = Try(conf.getBoolean(TRIGGER_UPDATES_KEY)).getOrElse(false)
    val isOptional = Try(conf.getBoolean(OPTIONAL_KEY)).getOrElse(false)

    val dateUntilExpr = Try {
      conf.getString(DATE_UNTIL_EXPR_KEY)
    }.toOption

    MetastoreDependency(tables, dateFromExpr, dateUntilExpr, triggerUpdates, isOptional)
  }
}


