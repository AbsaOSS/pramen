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

  def releaseGuardLock(owner: String): Unit

  def updateTicket(): Unit

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
      releaseGuardLock(owner)
      JvmUtils.safeRemoveShutdownHook(shutdownHook)
      log.info(s"Lock released: '$escapedToken'.")
    }
  }

  override def close(): Unit = {}

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
        releaseGuardLock(owner)
      }
    }
  }

  private def startWatcherThread(): Thread = {
    val thread = new Thread {
      override def run(): Unit = {
        lockWatcher()
      }
    }
    thread.start()
    thread
  }

  private def lockWatcher(): Unit = {
    var lastUpdateTime = Instant.now
    while (isAcquired) {
      Thread.sleep(1000)
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
