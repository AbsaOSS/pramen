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

package za.co.absa.pramen.core.mocks

import org.apache.spark.sql.SaveMode
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.core.metastore.model.{HiveConfig, MetaTable}

import java.time.LocalDate

object MetaTableFactory {
  def getDummyMetaTable(name: String = "dummy",
                        description: String = "description",
                        format: DataFormat = DataFormat.Parquet("/tmp/dummy", None),
                        infoDateColumn: String = "INFO_DATE",
                        infoDateFormat: String = "yyyy-MM-dd",
                        hiveConfig: HiveConfig = HiveConfig.getNullConfig,
                        hiveTable: Option[String] = None,
                        hivePath: Option[String] = None,
                        hivePreferAddPartition: Boolean = true,
                        infoDateExpression: Option[String] = None,
                        infoDateStart: LocalDate = LocalDate.of(2020, 1, 31),
                        trackDays: Int = 0,
                        trackDaysExplicitlySet: Boolean = false,
                        saveModeOpt: Option[SaveMode] = None,
                        readOptions: Map[String, String] = Map.empty[String, String],
                        writeOptions: Map[String, String] = Map.empty[String, String],
                        sparkConfig: Map[String, String] = Map.empty[String, String]
                       ): MetaTable = {
    MetaTable(name,
      description,
      format,
      infoDateColumn,
      infoDateFormat,
      hiveConfig,
      hiveTable,
      hivePath,
      hivePreferAddPartition,
      infoDateExpression,
      infoDateStart,
      trackDays,
      trackDaysExplicitlySet,
      saveModeOpt,
      readOptions,
      writeOptions,
      sparkConfig)
  }
}
