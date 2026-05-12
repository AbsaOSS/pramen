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

package za.co.absa.pramen.core.state

import za.co.absa.pramen.api.status.{PipelineStateSnapshot, TaskResult}
import za.co.absa.pramen.core.journal.Journal

trait PipelineState extends AutoCloseable {
  def getState: PipelineStateSnapshot

  def getBatchId: Long

  def setShutdownHookCanRun(): Unit

  def setSuccess(): Unit

  def setWarningFlag(): Unit

  def setFailure(stage: String, exception: Throwable): Unit

  def setSparkAppId(sparkAppId: String): Unit

  def setJournal(journal: Journal): Unit

  def setComputeEngineId(computeEngineId: String): Unit

  def setNumberOfExecutorsMin(n: Int): Unit

  def setNumberOfExecutorsMax(n: Int): Unit

  def setExecutorType(executorType: String): Unit

  def setExecutionAdditionalOption(key: String, value: String): Unit

  def setNumberOfRecordsIngested(count: Long): Unit

  def addNumberOfRecordsIngested(count: Long): Unit

  def setMaximumNumberOfColumns(count: Long): Unit

  def addTaskCompletion(statuses: Seq[TaskResult]): Unit

  def getExitCode: Int
}
