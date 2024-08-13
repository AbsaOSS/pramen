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

import za.co.absa.pramen.core.exceptions.TimeoutException
import za.co.absa.pramen.core.utils.impl.ThreadWithException

import java.lang.Thread.UncaughtExceptionHandler
import scala.concurrent.duration.Duration

object ThreadUtils {
  /**
    * Executes an action with a timeout. If the timeout is breached the task is killed (using Thread.interrupt())
    *
    * If the task times out, an exception is thrown.
    *
    * Any exception is passed to the caller.
    *
    * @param timeout  The task timeout.
    * @param action   An action to execute.
    */
  @throws[TimeoutException]
  def runWithTimeout(timeout: Duration)(action: => Unit): Unit = {
    val thread = new ThreadWithException {
      override def run(): Unit = {
        action
      }
    }

    val handler = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, ex: Throwable): Unit = {
        thread.asInstanceOf[ThreadWithException].setException(ex)
      }
    }

    thread.setUncaughtExceptionHandler(handler)

    thread.start()
    thread.join(timeout.toMillis)

    if (thread.isAlive) {
      val stackTrace = thread.getStackTrace
      thread.interrupt()

      val prettyTimeout = TimeUtils.prettyPrintElapsedTimeShort(timeout.toMillis)
      val cause = new RuntimeException("The task has been interrupted by Pramen.")
      cause.setStackTrace(stackTrace)

      throw new TimeoutException(s"Timeout expired ($prettyTimeout).", cause)
    }

    thread.getException.foreach(ex => throw ex)
  }
}
