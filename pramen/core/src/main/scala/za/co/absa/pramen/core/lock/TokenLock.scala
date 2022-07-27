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

package za.co.absa.pramen.core.lock

/**
  * This class provides facilities to protect against multiple jobs writing
  * to the same table.
  *
  * To achieve this a lock must be acquired for a particular token that should
  * be the table name.
  *
  * The state is isolated in this class. The usage is like this:
  * {{{
  *   val lock = new TokenLockHdfs("mytable")
  *   try {
  *     if (lock.tryAcquire() {
  *       runJob(...)
  *     } else {
  *       log.error("Sorry cannot acquire a write lock for 'mytable'.")
  *     }
  *   } finally {
  *     lock.release()
  *   }
  * }}}
  */

trait TokenLock extends AutoCloseable {
  def tryAcquire(): Boolean

  def release(): Unit
}
