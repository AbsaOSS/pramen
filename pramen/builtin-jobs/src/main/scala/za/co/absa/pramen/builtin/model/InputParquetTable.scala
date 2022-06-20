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

package za.co.absa.pramen.builtin.model

import com.typesafe.config.Config
import za.co.absa.pramen.framework.utils.ConfigUtils

case class InputParquetTable(
                              name: String,
                              path: String,
                              isOptional: Boolean,
                              infoDate: InfoDateColumn,
                              beginInfoDateExpression: Option[String],
                              endInfoDateExpression: Option[String]
                            )

object InputParquetTable {
  val NAME = "name"
  val PATH = "path"
  val OPTIONAL = "optional"
  val INFO_DATE_BEGIN = "input.date.begin"
  val INFO_DATE_END = "input.date.end"
  val INFO_COLUMN_NAME = "information.date.column"
  val INFO_COLUMN_FORMAT = "information.date.format"

  def load(conf: Config, parent: String, defaultInfoColumnName: String, defaultInfoColumnFormat: String): InputParquetTable = {
    ConfigUtils.validatePathsExistence(conf, parent, NAME :: PATH :: Nil)

    val isOptional = ConfigUtils.getOptionBoolean(conf, OPTIONAL).getOrElse(false)
    val infoDateBegin = ConfigUtils.getOptionString(conf, INFO_DATE_BEGIN)
    val infoDateEnd = ConfigUtils.getOptionString(conf, INFO_DATE_END)

    val infoDateColumnName = ConfigUtils.getOptionString(conf, INFO_COLUMN_NAME).getOrElse(defaultInfoColumnName)
    val infoDateColumnFormat = ConfigUtils.getOptionString(conf, INFO_COLUMN_FORMAT).getOrElse(defaultInfoColumnFormat)

    InputParquetTable(
      conf.getString(NAME),
      conf.getString(PATH),
      isOptional,
      InfoDateColumn(infoDateColumnName, infoDateColumnFormat),
      infoDateBegin,
      infoDateEnd
    )
  }
}