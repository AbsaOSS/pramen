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

package za.co.absa.pramen.framework.mocks.job

import java.time.LocalDate

import za.co.absa.pramen.api.{AggregationJob, JobDependency}

class AggregationJobSpy(val dependencies: Seq[JobDependency],
                        val numOfRecordsOpt: Option[Long]) extends AggregationJob {

  var runTaskCalled = 0

  override def name: String = "Dummy"

  override def getDependencies: Seq[JobDependency] = dependencies

  override def runTask(inputTables: Seq[String],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): Option[Long] = {
    runTaskCalled += 1
    numOfRecordsOpt
  }
}

