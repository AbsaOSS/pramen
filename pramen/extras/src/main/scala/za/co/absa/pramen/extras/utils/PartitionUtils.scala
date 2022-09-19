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

package za.co.absa.pramen.extras.utils

import java.time.LocalDate

object PartitionUtils {
  /**
    * Transforms a custom pattern template to the actual pattern.
    * Variable substitutions:
    * - {column} Information date column name
    * - {year}, {month}, {day} - elements of the information date
    *
    * Examples:
    * - Default template:    "{column}={year}-{month}-{day}"
    * - Enceladus template:  "{year}/{month}/{day}/v1"
    *
    * @param pattern        An input pattern
    * @param infoDateColumn Information date column name
    * @param infoDate       Information date
    * @param infoVersion    Information version
    * @return Partition folder path
    */
  def unpackCustomPartitionPattern(pattern: String, infoDateColumn: String, infoDate: LocalDate, infoVersion: Int): String = {
    val year = infoDate.getYear
    val month = infoDate.getMonthValue
    val day = infoDate.getDayOfMonth

    pattern.replace("{year}", year.toString)
      .replace("{month}", f"$month%02d")
      .replace("{day}", f"$day%02d")
      .replace("{version}", s"$infoVersion")
      .replace("{column}", infoDateColumn)
  }
}
