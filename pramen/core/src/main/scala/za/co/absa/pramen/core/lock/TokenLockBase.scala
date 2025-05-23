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

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.{JvmUtils, StringUtils}

import java.time.Instant
import scala.util.Random

abstract class TokenLockBase(token: String) extends TokenLock {
  import TokenLockBase._

  private val log = LoggerFactory.getLogger(this.getClass)

  protected val tokenExpiresSeconds: Long = TOKEN_EXPIRES_SECONDS
  protected val lockAcquireRetries: Int = LOCK_ACQUIRE_RETRIES

  val escapedToken: String = StringUtils.escapeNonAlphanumerics(token)
  val owner: String = JvmUtils.jvmName + "_" + Random.nextInt().toString

  // State
  private var lockAcquired = false

  def tryAcquireGuardLock(retries: Int, thisTry: Int): Boolean

  def releaseGuardLock(): Unit

  def updateTicket(): Unit

  /**
    * Attempts to acquire the lock for a specific token. If the lock is already acquired,
    * the method returns false. Otherwise, it tries to acquire the guard lock with a specified
    * number of retries, logs the acquisition, initializes the necessary resources, and returns true
    * if successful.
    *
    * This operation is thread-safe.
    *
    * @return true if the lock is successfully acquired; false if the lock is already held by this or another job.
    */
  override def tryAcquire(): Boolean = synchronized {
    if (lockAcquired) {
      false
    } else {
      if (tryAcquireGuardLock(lockAcquireRetries, 0)) {
        lockAcquired = true
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        startWatcherThread()
        log.info(s"Lock '$token' lock acquired: '$escapedToken'.")
        true
      } else {
        log.warn(s"Lock '$token' is acquired by another job.")
        false
      }
    }
  }

  override def release(): Unit = synchronized {
    if (lockAcquired) {
      lockAcquired = false
      releaseGuardLock()
      JvmUtils.safeRemoveShutdownHook(shutdownHook)
      log.info(s"Lock released: '$escapedToken'.")
    }
  }

  override def close(): Unit = {
    release()
  }

  protected def isAcquired: Boolean = synchronized {
    lockAcquired
  }

  protected def getNewTicket: Long = {
    val now = Instant.now.getEpochSecond
    now + tokenExpiresSeconds
  }

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (lockAcquired) {
        releaseGuardLock()
      }
    }
  }

  private def startWatcherThread(): Thread = {
    val thread = new Thread(s"TokenLockWatcher-$escapedToken") {
      override def run(): Unit = {
        lockWatcher()
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def lockWatcher(): Unit = {
    var lastUpdateTime = Instant.now
    while (isAcquired) {
      try {
        Thread.sleep(1000)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return
      }
      if (Instant.now().isAfter(lastUpdateTime.plusMillis((tokenExpiresSeconds * 1000) / 5))) {
        this.synchronized {
          if (isAcquired) {
            updateTicket()
            lastUpdateTime = Instant.now
          }
        }
      }
    }
  }
}

object TokenLockBase {
  val LOCK_ACQUIRE_RETRIES = 3
  val TOKEN_EXPIRES_SECONDS: Long = 10L * 60L
}
