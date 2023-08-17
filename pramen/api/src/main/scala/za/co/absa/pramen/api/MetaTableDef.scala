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

package za.co.absa.pramen.api

import java.time.LocalDate

/**
  * This is metatable details available to read from the metastore.
  *
  * @param name            The name of the table.
  * @param description     The description of the table.
  * @param format          The format of the table.
  * @param infoDateColumn  The name of the column that contains the information date (partitioned by).
  * @param infoDateFormat  The format of the information date.
  * @param hiveTable       The name of the Hive table.
  * @param hivePath        The path of the Hive table (if it differs from the path in the underlying format).
  * @param infoDateStart   The start date of the information date.
  * @param readOptions     The read options for the table.
  * @param writeOptions    The write options for the table.
  */
case class MetaTableDef(
                         name: String,
                         description: String,
                         format: DataFormat,
                         infoDateColumn: String,
                         infoDateFormat: String,
                         hiveTable: Option[String],
                         hivePath: Option[String],
                         infoDateStart: LocalDate,
                         readOptions: Map[String, String],
                         writeOptions: Map[String, String]
                       )
