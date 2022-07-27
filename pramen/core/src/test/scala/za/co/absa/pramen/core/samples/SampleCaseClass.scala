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

package za.co.absa.pramen.core.samples

import java.time.LocalDate

case class SampleCaseClass(
                          strValue: String,
                          intValue: Int,
                          longValue: Long,
                          dateValue: LocalDate,
                          listStr: List[String]
                          )

object SampleCaseClass {
  def getDummy: SampleCaseClass = {
    SampleCaseClass("String1", 100000, 10000000000L, LocalDate.of(2020,8,10), "Str1" :: "Str2" :: Nil)
  }
}