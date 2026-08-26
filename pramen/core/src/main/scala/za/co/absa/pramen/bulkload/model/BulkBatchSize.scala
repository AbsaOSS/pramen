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

package za.co.absa.pramen.bulkload.model

sealed trait BulkBatchSize

object BulkBatchSize {
  case object Monthly extends BulkBatchSize
  case object Quarterly extends BulkBatchSize
  case object Yearly extends BulkBatchSize

  def fromString(bulkBatchSizeStr: String): BulkBatchSize = {
    bulkBatchSizeStr.toLowerCase.trim match {
      case "monthly" => Monthly
      case "quarterly" => Quarterly
      case "yearly" => Yearly
      case _ => throw new IllegalArgumentException(s"Unknown bulk batch size: $bulkBatchSizeStr. Can be one of: 'monthly', 'quarterly', 'yearly'.")
    }
  }
}
