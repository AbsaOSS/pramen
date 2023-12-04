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

import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.runner.splitter.RunMode

import java.time.LocalDate

object RuntimeConfigFactory {

  def getDummyRuntimeConfig(isDryRun: Boolean = false,
                            isRerun: Boolean = false,
                            runTables: Seq[String] = Seq.empty[String],
                            isUndercover: Boolean = false,
                            useLocks: Boolean = false,
                            checkOnlyLateData: Boolean = false,
                            checkOnlyNewData: Boolean = false,
                            emailIfNoChanges: Boolean = false,
                            runDate: LocalDate = LocalDate.of(2022, 2,18),
                            runDateTo: Option[LocalDate] = None,
                            isInverseOrder: Boolean = false,
                            parallelTasks: Int = 1,
                            stopSparkSession: Boolean = false,
                            allowEmptyPipeline: Boolean = false,
                            historicalRunMode: RunMode = RunMode.CheckUpdates): RuntimeConfig = {
    RuntimeConfig(isDryRun,
      isRerun,
      runTables,
      isUndercover,
      useLocks,
      checkOnlyLateData,
      checkOnlyNewData,
      emailIfNoChanges,
      runDate,
      runDateTo,
      isInverseOrder,
      parallelTasks,
      stopSparkSession,
      allowEmptyPipeline,
      historicalRunMode)
  }

}
