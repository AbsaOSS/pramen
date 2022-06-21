/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.runner.splitter

import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.v2.MetastoreDependency
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.pipeline.TaskPreDef

import java.time.LocalDate

trait ScheduleStrategy {

  def getDaysToRun(
                    outputTable: String,
                    dependencies: Seq[MetastoreDependency],
                    bookkeeper: SyncBookKeeper,
                    infoDateExpression: String,
                    schedule: Schedule,
                    params: ScheduleParams,
                    minimumDate: LocalDate
                  ): Seq[TaskPreDef]
}
