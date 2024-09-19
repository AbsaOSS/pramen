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

package za.co.absa.pramen.core.reader.model

import com.typesafe.config.Config
import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetValue}
import za.co.absa.pramen.core.utils.ConfigUtils

object OffsetInfoParser {
  val OFFSET_COLUMN_NAME_KEY = "offset.column.name"
  val OFFSET_COLUMN_TYPE_KEY = "offset.column.type"

  def fromConfig(conf: Config): Option[OffsetInfo] = {
    for {
      columnName <- ConfigUtils.getOptionString(conf, OFFSET_COLUMN_NAME_KEY)
      columnType <- ConfigUtils.getOptionString(conf, OFFSET_COLUMN_TYPE_KEY)
    } yield OffsetInfo(columnName, OffsetValue.getMinimumForType(columnType))
  }

}
