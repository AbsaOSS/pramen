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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ThreadClosableRegistry {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val closeables = new java.util.LinkedList[(Long, AutoCloseable)]

  /**
    * Registers a closeable resource for the current thread.
    * The resource will be automatically closed when [[cleanupThread]] is called for this thread.
    *
    * @param closeable The AutoCloseable resource to register
    */
  def registerCloseable(closeable: AutoCloseable): Unit = synchronized {
    val threadId = Thread.currentThread().getId

    if (closeables.indexOf(closeable) < 0) {
      closeables.add((threadId, closeable))
    }
  }

  /**
    * Unregisters a closeable resource from the thread's registry.
    *
    * @param closeable The AutoCloseable resource to unregister
    */
  def unregisterCloseable(closeable: AutoCloseable): Unit = synchronized {
    val iterator = closeables.iterator()
    while (iterator.hasNext) {
      val (_, c) = iterator.next()
      if (c == closeable) {
        iterator.remove()
        return
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
    val threadCloseables = closeables.asScala.filter(_._1 == threadId).map(_._2).toList
    threadCloseables.reverse.foreach { closeable =>
      try {
        closeable.close()
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Error closing resource for thread $threadId.", ex)
      } finally {
        unregisterCloseable(closeable)
      }
    }
  }
}
