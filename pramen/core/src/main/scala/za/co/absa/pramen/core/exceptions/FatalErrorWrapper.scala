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

package za.co.absa.pramen.core.exceptions

/**
  * This is the wrapper of fatal errors occurring in a thread so that the error doesn't kill the thread.
  * This exception is returned from the thread feature, propagated to the bottom ot the stack trace, and
  * is handled properly there.
  */
class FatalErrorWrapper(val msg: String, val cause: Throwable = null) extends RuntimeException(msg, cause)
