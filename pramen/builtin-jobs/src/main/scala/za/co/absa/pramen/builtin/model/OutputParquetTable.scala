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

package za.co.absa.pramen.builtin.model

import com.typesafe.config.Config
import za.co.absa.pramen.framework.utils.ConfigUtils

case class OutputParquetTable(
                               name: String,
                               path: String,
                               recordsPerPartition: Long,
                               infoDate: InfoDateColumn,
                               outputInfoDateExpression: Option[String]
                             )

object OutputParquetTable {
  val NAME = "name"
  val PATH = "path"
  val RPP = "records.per.partition"
  val INFO_COLUMN_NAME = "information.date.column"
  val INFO_COLUMN_FORMAT = "information.date.format"

  def load(conf: Config, parent: String, defaultInfoColumnName: String, defaultInfoColumnFormat: String, outputInfoDateExpr: Option[String]): OutputParquetTable = {
    ConfigUtils.validatePathsExistence(conf, parent, NAME :: PATH :: RPP :: Nil)

    val infoDateColumnName = ConfigUtils.getOptionString(conf, INFO_COLUMN_NAME).getOrElse(defaultInfoColumnName)
    val infoDateColumnFormat = ConfigUtils.getOptionString(conf, INFO_COLUMN_FORMAT).getOrElse(defaultInfoColumnFormat)

    OutputParquetTable(
      conf.getString(NAME),
      conf.getString(PATH),
      conf.getLong(RPP),
      InfoDateColumn(infoDateColumnName, infoDateColumnFormat),
      outputInfoDateExpr
    )
  }
}