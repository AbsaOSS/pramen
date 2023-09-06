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

package za.co.absa.pramen.core.bookkeeper

import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate

/**
  * The implementation of bookkeeper that does nothing. The idea is as follows. When bookkeeping is disabled,
  * this bookkeeper implementation is used. It always returns the state as if every table is new and no information is
  * available.
  */
class BookkeeperNull() extends BookkeeperBase(false) {
  override val bookkeepingEnabled: Boolean = false

  override def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate]): Option[LocalDate] = None

  override def getLatestDataChunkFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = None

  override def getDataChunksFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = Nil

  override def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = 0

  private[pramen] override def saveRecordCountToStorage(table: String,
                                                        infoDate: LocalDate,
                                                        infoDateBegin: LocalDate,
                                                        infoDateEnd: LocalDate,
                                                        inputRecordCount: Long,
                                                        outputRecordCount: Long,
                                                        jobStarted: Long,
                                                        jobFinished: Long): Unit = {}

  override def getLatestSchema(table: String, until: LocalDate): Option[(StructType, LocalDate)] = None

  override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {}
}
