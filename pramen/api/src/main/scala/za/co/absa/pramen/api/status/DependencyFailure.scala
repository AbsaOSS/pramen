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

package za.co.absa.pramen.api.status

case class DependencyFailure(
                              dep: MetastoreDependency,
                              emptyTables: Seq[String],
                              failedTables: Seq[String],
                              failedDateRanges: Seq[String]
                            ) {
  def renderText: String = {
    val length = Math.min(failedTables.length, failedDateRanges.length)

    val emptyTablesRendered = emptyTables.map(t => s"$t (Empty table or wrong table name)")

    val failedTablesRendered = Range(0, length).map { i =>
      s"${failedTables(i)} (${failedDateRanges(i)})"
    }

    (emptyTablesRendered ++ failedTablesRendered).mkString(", ")
  }
}
