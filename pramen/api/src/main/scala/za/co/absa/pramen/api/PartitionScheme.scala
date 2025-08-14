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

sealed trait PartitionScheme

object PartitionScheme {
  case object PartitionByDay extends PartitionScheme

  case class PartitionByMonth(generatedMonthColumn: String, generatedYearColumn: String) extends PartitionScheme

  case class PartitionByYearMonth(generatedMonthYearColumn: String) extends PartitionScheme

  case class PartitionByYear(generatedYearColumn: String) extends PartitionScheme

  /** Tables are not partitioned, but the information date column is present and all updates are scoped within an information date day. */
  case object NotPartitioned extends PartitionScheme

  /** The table is always overwritten with the latest data. No information date column is present.*/
  case object Overwrite extends PartitionScheme
}
