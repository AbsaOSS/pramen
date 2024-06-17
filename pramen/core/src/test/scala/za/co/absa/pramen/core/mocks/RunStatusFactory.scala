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

package za.co.absa.pramen.core.mocks

import za.co.absa.pramen.api.status.{RunStatus, TaskRunReason}

object RunStatusFactory {
  def getDummySuccess(recordCountOld: Option[Long] = None,
                      recordCount: Long = 1000,
                      sizeBytes: Option[Long] = None,
                      reason: TaskRunReason = TaskRunReason.New,
                      filesRead: Seq[String] = Nil,
                      filesWritten: Seq[String] = Nil,
                      hiveTablesUpdated: Seq[String] = Nil,
                      warnings: Seq[String] = Nil): RunStatus.Succeeded = {
    RunStatus.Succeeded(recordCountOld,
      recordCount,
      sizeBytes,
      reason,
      filesRead,
      filesWritten,
      hiveTablesUpdated,
      warnings)
  }

  def getDummyFailure(ex: Throwable = new RuntimeException("Dummy failure")): RunStatus.Failed = {
    RunStatus.Failed(ex)
  }
}
