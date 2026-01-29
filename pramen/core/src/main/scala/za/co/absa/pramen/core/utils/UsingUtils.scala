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

import scala.util.control.NonFatal

object UsingUtils {
  /**
    * Executes the given action with a resource that implements the AutoCloseable interface, ensuring
    * proper closure of the resource. Any exception that occurs during the action or resource closure
    * is handled appropriately, with suppressed exceptions added where relevant. Null resources are not supported.
    *
    * @param resource a lazily evaluated resource that implements AutoCloseable
    * @param action   a function to be executed using the provided resource
    * @tparam T the type of the resource, which must extend AutoCloseable
    * @throws Throwable if either the action or resource closure fails. If both fail, the action's exception
    *                   is thrown with the closure's exception added as suppressed
    */
  def using[T <: AutoCloseable,U](resource: => T)(action: T => U): U = {
    var actionExceptionOpt: Option[Throwable] = None
    val openedResource = resource

    try {
      return action(openedResource)
    } catch {
      case NonFatal(ex) =>
        actionExceptionOpt = Option(ex)
    } finally
      if (openedResource != null) {
        try
          openedResource.close()
        catch {
          case NonFatal(closeException) =>
            actionExceptionOpt match {
              case Some(actionException) =>
                actionException.addSuppressed(closeException)
                throw actionException
              case None =>
                throw closeException
            }
        }
      }

    // It is not possible to return a valid value of type U at this point so the rest of code should return Nothing
    actionExceptionOpt match {
      case Some(ex) => throw ex
      case None     => throw new IllegalArgumentException("Unreachable code")
    }
  }
}
