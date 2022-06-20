/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.mocks

import za.co.absa.pramen.framework.notify.{Notification, SchemaDifference, TaskCompleted}

import java.time.Instant

object NotificationFactory {
  def getDummyNotification(exception: Option[Throwable] = None,
                           ingestionName: String = "DummyIngestion",
                           environmentName: String = "DummyEnvironment",
                           syncStarted: Instant = Instant.ofEpochSecond(1234567L),
                           syncFinished: Instant = Instant.ofEpochSecond(1234568L),
                           tasksCompleted: List[TaskCompleted] = List(TaskCompletedFactory.getTackCompleted()),
                           schemaDifferences: List[SchemaDifference] = List(SchemaDifferenceFactory.getDummySchemaDifference())
                          ): Notification = {
    Notification(
      exception,
      ingestionName,
      environmentName,
      syncStarted,
      syncFinished,
      tasksCompleted,
      schemaDifferences
    )
  }

}
