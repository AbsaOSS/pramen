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

import za.co.absa.pramen.api.RunMode

import java.time.LocalDate

case class RuntimeInfo(
                        runDateFrom: LocalDate, // Specifies the run date or the beginning of the run period.
                        runDateTo: Option[LocalDate] = None, // If set, the pipeline runs for a period between `runDateFrom` and `runDateTo` inclusive.
                        historicalRunMode: Option[RunMode] = None, // If set, specifies which historical mode the pipeline runs for.
                        isRerun: Boolean = false, // If true, the pipeline runs in rerun mode.
                        isDryRun: Boolean = false, // If true, the pipeline won't do any writes, just the list of jobs it would run.
                        isUndercover: Boolean = false, // If true, no bookkeeping will be done for the job.
                        isNewOnly: Boolean = false, // If true, the pipeline runs without catching up late data mode.
                        isLateOnly: Boolean = false, // If true, the pipeline runs in catching up late data only mode.
                        minRps: Int = 0, // Configured records per second that is considered bad if the actual rps is lower, ignored if 0.
                        goodRps: Int = 0 // Configured records per second that is considered very good if the actual rps is higher, ignored if 0.
                      )
