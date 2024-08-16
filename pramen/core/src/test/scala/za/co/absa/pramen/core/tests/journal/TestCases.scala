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

package za.co.absa.pramen.core.tests.journal

import za.co.absa.pramen.core.journal.model
import za.co.absa.pramen.core.journal.model.TaskCompleted

import java.time.{Instant, LocalDate}

object TestCases {
  val infoDate1: LocalDate = LocalDate.of(2020, 8, 11)
  val infoDate2: LocalDate = LocalDate.of(2020, 8, 11)
  val infoDate3: LocalDate = LocalDate.of(2020, 8, 11)

  val instant1: Instant = Instant.ofEpochSecond(1597318830)
  val instant2: Instant = Instant.ofEpochSecond(1597318835)
  val instant3: Instant = Instant.ofEpochSecond(1597318839)

  val task1: TaskCompleted = model.TaskCompleted("job1", "table1", infoDate1, infoDate1, infoDate1, 100, 0, Some(100), None, None, 597318830, 1597318830, "New", Some("Test1"), Some("abc123"), Some("p_id_1"), Some("p_1"), Some("TEST"), Some("T1"))
  val task2: TaskCompleted = model.TaskCompleted("job1", "table1", infoDate2, infoDate2, infoDate2, 100, 0, Some(100), None, None, 1597318835, 1597318835, "Late", Some("Test2"), Some("abc123"), Some("p_id_2"), Some("p_2"), Some("TEST"), Some("T2"))
  val task3: TaskCompleted = model.TaskCompleted("job2", "table2", infoDate3, infoDate3, infoDate3, 100, 0, Some(100), None, None, 1597318839, 1597318839, "Fail", Some("Test3"), Some("abc123"), Some("p_id_3"), Some("p_2"), Some("TEST"), Some("T2"))
}
