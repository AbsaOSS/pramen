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

package za.co.absa.pramen.core.runner.task

import org.slf4j.LoggerFactory

import java.util

object ThreadClosableRegistry {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val closables = new util.HashMap[Long, AutoCloseable]()

  def registerCloseable(closeable: AutoCloseable): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    closables.put(threadId, closeable)
  }

  def unregisterCloseable(closeable: AutoCloseable): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    closables.remove(threadId, closeable)
  }

  def cleanupThread(threadId: Long): Unit = synchronized {
    Option(closables.remove(threadId)).foreach { closeable =>
      try {
        closeable.close()
      } catch {
        case ex: Exception =>
          log.warn(s"Failed to close resource for thread $threadId", ex)
      }
    }
  }

}
