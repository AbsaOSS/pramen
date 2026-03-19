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
import scala.collection.JavaConverters._

object ThreadClosableRegistry {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val closables = new util.HashMap[Long, util.List[AutoCloseable]]()

  /**
    * Registers a closeable resource for the current thread.
    * The resource will be automatically closed when [[cleanupThread]] is called for this thread.
    *
    * @param closeable The AutoCloseable resource to register
    */
  def registerCloseable(closeable: AutoCloseable): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    val list = Option(closables.get(threadId)).getOrElse {
      val newList = new util.ArrayList[AutoCloseable]()
      closables.put(threadId, newList)
      newList
    }
    list.add(closeable)
  }

  /**
    * Unregisters a closeable resource from the current thread's registry.
    * If this was the last resource for the thread, the thread entry is removed from the registry.
    *
    * @param closeable The AutoCloseable resource to unregister
    */
  def unregisterCloseable(closeable: AutoCloseable): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    Option(closables.get(threadId)).foreach { list =>
      list.remove(closeable)
      if (list.isEmpty) {
        closables.remove(threadId)
      }
    }
  }

  /**
    * Closes all registered resources for the specified thread in LIFO (Last-In-First-Out) order.
    * This method is typically called when a thread times out or completes execution.
    * Any exceptions during closing are logged but do not prevent other resources from being closed.
    *
    * @param threadId The ID of the thread whose resources should be cleaned up
    */
  def cleanupThread(threadId: Long): Unit = synchronized {
    Option(closables.remove(threadId)).foreach { list =>
      // Ensure LIFO order
      val iterator = list.asScala.reverseIterator
      while (iterator.hasNext) {
        val closeable = iterator.next()
        try {
          closeable.close()
        } catch {
          case ex: Exception =>
            log.warn(s"Failed to close resource for thread $threadId", ex)
        }
      }
    }
  }
}
