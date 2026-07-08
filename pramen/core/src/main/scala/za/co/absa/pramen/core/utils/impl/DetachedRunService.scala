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

package za.co.absa.pramen.core.utils.impl

import scala.concurrent.Future
import scala.concurrent.duration.Duration

object DetachedRunService {
  /**
    * Executes the given action on a separate daemon thread, returning a Future that will be
    * completed with the result of the action or failed with any exception thrown during execution.
    *
    * The thread is marked as a daemon thread, meaning it will not prevent the JVM from shutting down.
    *
    * @param action the by-name computation to execute on a detached daemon thread
    * @return a Future containing the result of the action, or a failed Future if the action throws an exception
    */
  def runDetached[T](action: => T): Future[T] = {
    val thread = new ThreadWithResult[T] {
      override def runWitResult(): T = action
    }

    val fut = thread.getFuture
    thread.setDaemon(true)
    thread.start()

    fut
  }

  /**
    * Executes the given action on a separate daemon thread, waiting up to the specified timeout
    * for the result. If the action completes within the timeout, the result is returned in a
    * completed Future. If the timeout elapses before the action completes, the thread continues
    * running in a detached manner (as a daemon thread) and the returned Future will eventually
    * be completed when the action finishes.
    *
    * @param timeout the maximum duration to wait for the action to complete before detaching
    * @param action  the by-name computation to execute on a detached daemon thread
    * @return a Future containing the result of the action, or a failed Future if the action throws an exception
    */
  def runWithTimeoutThenDetach[T](timeout: Duration)(action: => T): Future[T] = {
    val thread = new ThreadWithResult[T] {
      override def runWitResult(): T = action
    }

    val fut = thread.getFuture
    thread.setDaemon(true)
    thread.start()

    thread.join(timeout.toMillis)

    fut
  }
}
