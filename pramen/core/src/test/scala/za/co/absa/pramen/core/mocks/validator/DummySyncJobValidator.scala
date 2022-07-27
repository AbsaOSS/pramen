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

package za.co.absa.pramen.core.mocks.validator

import za.co.absa.pramen.core.validator.{RunDecision, RunDecisionSummary, SyncJobValidator}

import java.time.LocalDate

class DummySyncJobValidator extends SyncJobValidator {
  def isEmpty: Boolean = true

  override def validateTask(infoDateBegin: LocalDate, infoDateEnd: LocalDate, infoDateOutput: LocalDate): Unit = {
  }

  override def decideRunTask(infoDateBegin: LocalDate,
                             infoDateEnd: LocalDate,
                             infoDateOutput: LocalDate,
                             outputTableName: String,
                             forceRun: Boolean): RunDecisionSummary = {
    RunDecisionSummary(RunDecision.RunNew,
      100L, None, None, None, Nil, Nil
    )
  }
}
