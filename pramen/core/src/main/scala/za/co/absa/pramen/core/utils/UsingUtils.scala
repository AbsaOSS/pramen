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

object UsingUtils {
  /**
    * Executes the given action with a resource that implements the AutoCloseable interface, ensuring
    * proper closure of the resource. Any exception that occurs during the action or resource closure
    * is handled appropriately, with suppressed exceptions added where relevant.
    *
    * @param resource a lazily evaluated resource that implements AutoCloseable
    * @param action   a function to be executed using the provided resource
    * @tparam T the type of the resource, which must extend AutoCloseable
    * @throws Throwable if either the action or resource closure fails. If both fail, the action's exception
    *                   is thrown with the closure's exception added as suppressed
    */
  def using[T <: AutoCloseable, U](resource: => T)(action: T => U): U = {
    var actionException: Throwable = null
    val openedResource = resource

    try {
      action(openedResource)
    } catch {
      case t: Throwable =>
        actionException = t
        throw t
    } finally
      if (openedResource != null) {
        try
          openedResource.close()
        catch {
          case closeException: Throwable =>
            if (actionException != null) {
              actionException.addSuppressed(closeException)
            } else {
              throw closeException
            }
        }
      }
  }
}
