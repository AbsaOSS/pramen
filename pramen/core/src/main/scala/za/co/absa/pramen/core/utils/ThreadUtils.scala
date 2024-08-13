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

package za.co.absa.pramen.core.utils

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.impl.ThreadWithException

import scala.concurrent.duration.Duration

object ThreadUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Executes an action
    * @param taskName
    * @param timeout
    * @param action
    */
  def runWithTimeout(taskName: String, timeout: Duration)(action: => Unit): Unit = {
    def logStackTrace(thread: Thread): Unit = {
      val stackTrace = thread.getStackTrace.map(_.toString).mkString("\n")
      log.error(s"Thread ($taskName) stack trace:\n$stackTrace")
    }

    val thread = new ThreadWithException {
      override def run(): Unit = {
        action
      }
    }
    thread.setUncaughtExceptionHandler((_, ex) => {
      thread.setException(ex)
    })
    thread.start()
    thread.join(timeout.toMillis)

    if (thread.isAlive) {
      logStackTrace(thread)
      thread.interrupt()
      val prettyTimeout = TimeUtils.prettyPrintElapsedTimeShort(timeout.toMillis)
      throw new RuntimeException(s"Timeout ($prettyTimeout) expired executing: $taskName (stack trace logged).")
    }

    thread.getException.foreach(ex => throw ex)
  }
}
