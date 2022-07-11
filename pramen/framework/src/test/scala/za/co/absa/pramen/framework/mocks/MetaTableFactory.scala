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

package za.co.absa.pramen.framework.mocks

import za.co.absa.pramen.framework.metastore.model.DataFormat._
import za.co.absa.pramen.framework.metastore.model.{DataFormat, MetaTable}

import java.time.LocalDate

object MetaTableFactory {
  def getDummyMetaTable(name: String = "dummy",
                        description: String = "description",
                        format: DataFormat = Parquet("/tmp/dummy", None),
                        infoDateColumn: String = "INFO_DATE",
                        infoDateFormat: String = "yyyy-MM-dd",
                        hiveTable: Option[String] = None,
                        infoDateExpression: Option[String] = None,
                        infoDateStart: LocalDate = LocalDate.of(2020, 1, 31),
                        trackDays: Int = 0,
                        readOptions: Map[String, String] = Map.empty[String, String],
                        writeOptions: Map[String, String] = Map.empty[String, String]
                       ): MetaTable = {
    MetaTable(name,
      description,
      format,
      infoDateColumn,
      infoDateFormat,
      hiveTable,
      infoDateExpression,
      infoDateStart,
      trackDays,
      readOptions,
      writeOptions)
  }
}
