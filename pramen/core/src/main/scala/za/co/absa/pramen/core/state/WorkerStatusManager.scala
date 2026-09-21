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

import scala.collection.mutable

object WorkerStatusManager {
  private val workerStatus = new mutable.HashMap[Long, String]()

  def setStatus(status: String): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    workerStatus(threadId) = status
  }

  def setFinished(): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    workerStatus.remove(threadId)
  }

  def getStatuses: Seq[WorkerStatus] = synchronized {
    workerStatus.map { case (threadId, status) => WorkerStatus(threadId, status) }.toSeq
  }
}
