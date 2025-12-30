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

/**
  * This class provides facilities to protect against multiple jobs writing
  * to the same table based on a global lock held using a database.
  *
  * To achieve this a lock must be acquired for a particular token that should
  * be uniquely defined based on the table identifier, and sometimes information date.
  *
  * The implementation does not depend on tokens to be related to tables, it can be anything.
  * But as along as one process owns the token, other processes can't acquire the lock.
  *
  * The usage is like this:
  * {{{
  *   val lockFactory = new TokenLockFactory // One of available implementations
  *   val lock = lockFactory.getLock("my_token")
  *   try {
  *     if (lock.tryAcquire()) {
  *       runJob(...)
  *     } else {
  *       log.error("Sorry cannot acquire a write lock for 'my_token'.")
  *     }
  *   } finally {
  *     lock.release()
  *   }
  * }}}
  */
package za.co.absa.pramen.api.lock

trait TokenLockFactory {
  def getLock(token: String): TokenLock
}
