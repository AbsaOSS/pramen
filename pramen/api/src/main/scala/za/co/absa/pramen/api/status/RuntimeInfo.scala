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

case class RuntimeInfo(
                        isDryRun: Boolean = false,     // If true, the pipeline won't do any writes, just the list of jobs it would run
                        isUndercover: Boolean = false, // If true, no bookkeeping will be done for the job
                        minRps: Int = 0,               // Configured records per second that is considered bad if the actual rps is lower, ignored if 0
                        goodRps: Int = 0               // Configured records per second that is considered very good if the actual rps is higher, ignored if 0
                      )
