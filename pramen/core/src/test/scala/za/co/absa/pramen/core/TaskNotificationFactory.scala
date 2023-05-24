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

package za.co.absa.pramen.core

import za.co.absa.pramen.api.{TaskNotification, TaskStatus}

import java.time.{Instant, LocalDate}

object TaskNotificationFactory {
  def getDummyTaskNotification(tableName: String = "dummy_table",
                               infoDate: LocalDate = LocalDate.of(2022, 2, 18),
                               started: Instant = Instant.ofEpochMilli(1613600000000L),
                               finished: Instant = Instant.ofEpochMilli(1672759508000L),
                               status: TaskStatus = TaskStatus.Succeeded(100, Seq.empty, Seq.empty, Seq.empty, Seq.empty),
                               applicationId: String = "app_12345",
                               options: Map[String, String] = Map.empty[String, String]): TaskNotification = {
    TaskNotification(tableName, infoDate, started, finished, status, applicationId, options)
  }

}
