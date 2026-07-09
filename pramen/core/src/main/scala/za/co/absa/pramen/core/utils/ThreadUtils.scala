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
import za.co.absa.pramen.core.exceptions.TimeoutException
import za.co.absa.pramen.core.runner.task.ThreadClosableRegistry
import za.co.absa.pramen.core.utils.impl.{DetachedRunService, ThreadWithException}

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object ThreadUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Executes an action with a timeout. If the timeout is breached, cleanup is performed
    * (e.g. closing JDBC connections) and then the task is killed (using Thread.interrupt()).
    *
    * If the task times out, a [[TimeoutException]] is thrown.
    *
    * Any exception thrown by the action is re-thrown to the caller.
    *
    * @param timeout        The task timeout.
    * @param cleanupTimeout The timeout for the cleanup operation before interrupting the thread.
    * @param action         An action to execute.
    * @throws TimeoutException if the action does not complete within the specified timeout.
    */
  @throws[TimeoutException]
  def runWithTimeout(timeout: Duration, cleanupTimeout: Duration = Duration(5, TimeUnit.MINUTES))(action: => Unit): Unit = {
    val thread = new ThreadWithException {
      override def run(): Unit = {
        action
      }
    }

    val handler = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, ex: Throwable): Unit = {
        thread.setException(ex)
      }
    }

    thread.setUncaughtExceptionHandler(handler)

    thread.start()
    val waitMillis = timeout.toMillis
    if (waitMillis > 0) {
      thread.join(waitMillis)
    }

    if (thread.isAlive) {
      val stackTrace = thread.getStackTrace
      val threadId = thread.getId

      // Execute cleanup BEFORE interrupt - e.g. close the JDBC connection/statement
      val future = DetachedRunService.runWithTimeoutThenDetach[Unit](cleanupTimeout) {
        ThreadClosableRegistry.cleanupThread(threadId)
      }

      future.value match {
        case Some(Success(_))  =>
          log.info(s"Cleanup for thread $threadId completed successfully. All SQL queries were cancelled and connections closed.")
        case Some(Failure(ex)) =>
          log.warn(s"Exception during thread $threadId cleanup: ${ex.getMessage}")
        case None              =>
          log.info(s"Timeout cleanup for thread $threadId is still running. Setting it to continue in a detached thread...")
      }

      thread.interrupt()

      val prettyTimeout = TimeUtils.prettyPrintElapsedTimeShort(timeout.toMillis)
      val cause = new RuntimeException("The task has been interrupted by Pramen.")
      cause.setStackTrace(stackTrace)

      throw new TimeoutException(s"Timeout expired ($prettyTimeout).", cause)
    }

    thread.getException.foreach(ex => throw ex)
  }
}
