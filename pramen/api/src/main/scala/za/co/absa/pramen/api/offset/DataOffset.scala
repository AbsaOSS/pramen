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

package za.co.absa.pramen.api.offset

import java.time.{Instant, LocalDate}

sealed trait DataOffset {
  val tableName: String

  val infoDate: LocalDate

  val batchId: Long

  val createdAt: Instant

  val isCommitted: Boolean
}

object DataOffset {
  case class CommittedOffset(
                              tableName: String,
                              infoDate: LocalDate,
                              batchId: Long,
                              minOffset: OffsetValue,
                              maxOffset: OffsetValue,
                              createdAt: Instant,
                              committedAt: Instant
                            ) extends DataOffset {

    override val isCommitted: Boolean = true
  }

  case class UncommittedOffset(
                                tableName: String,
                                infoDate: LocalDate,
                                batchId: Long,
                                offsetType: OffsetType,
                                createdAt: Instant
                              ) extends DataOffset {

    override val isCommitted: Boolean = false
  }
}
