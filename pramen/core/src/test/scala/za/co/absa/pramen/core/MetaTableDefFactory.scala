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

package za.co.absa.pramen.core

import za.co.absa.pramen.api.{DataFormat, MetaTableDef, PartitionScheme}

import java.time.LocalDate

object MetaTableDefFactory {

  def getDummyMetaTableDef(
                            name: String = "table",
                            description: String = "",
                            format: DataFormat = DataFormat.Null(),
                            infoDateColumn: String = "info_date",
                            infoDateFormat: String = "yyyy-MM-dd",
                            partitionScheme: PartitionScheme = PartitionScheme.PartitionByDay,
                            batchIdColumn: String = "pramen_batchid",
                            hiveTable: Option[String] = None,
                            hivePath: Option[String] = None,
                            infoDateStart: LocalDate = LocalDate.of(2022, 1, 1),
                            tableProperties: Map[String, String] = Map.empty[String, String],
                            readOptions: Map[String, String] = Map.empty[String, String],
                            writeOptions: Map[String, String] = Map.empty[String, String]
                          ): MetaTableDef = {
    MetaTableDef(
      name,
      description,
      format,
      infoDateColumn,
      infoDateFormat,
      partitionScheme,
      batchIdColumn,
      hiveTable,
      hivePath,
      infoDateStart,
      tableProperties,
      readOptions,
      writeOptions)
  }
}
