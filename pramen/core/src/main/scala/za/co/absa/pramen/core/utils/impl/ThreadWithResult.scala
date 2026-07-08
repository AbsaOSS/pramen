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

import scala.concurrent.{Future, Promise}

/**
  * An abstract thread that computes a result of type T and makes it available as a Future.
  *
  * Subclasses must implement the `runWitResult` method to define the computation to be performed.
  * When the thread is started, it executes `runWitResult` and completes an internal Promise with
  * the result. If the computation throws an exception, the Promise is failed with that exception.
  *
  * The result of the computation can be obtained via the `getFuture` method, which returns a Future
  * that will be completed once the thread finishes execution.
  *
  * @tparam T the type of the result produced by this thread
  */
abstract class ThreadWithResult[T] extends Thread {
  private val resultPromise: Promise[T] = Promise[T]()

  def runWitResult(): T
  
  def getFuture: Future[T] = resultPromise.future

  override def run(): Unit = {
    try {
      val result = runWitResult()
      resultPromise.success(result)
    } catch {
      case ex: Throwable => resultPromise.failure(ex)
    }
  }
}
