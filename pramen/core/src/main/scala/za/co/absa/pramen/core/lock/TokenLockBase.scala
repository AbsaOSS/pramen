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
import scala.util.control.NonFatal

/**
  * This is an abstract base class for token-based locks.
  *
  * The lock behavior is defined here. The actual storage used for the distributed locking is implemented in
  * subclasses.
  *
  * @param token the unique identifier for the lock (across multiple JVM processes and Spark jobs).
  */
abstract class TokenLockBase(token: String) extends TokenLock {
  import TokenLockBase._

  private val log = LoggerFactory.getLogger(this.getClass)

  protected val tokenExpiresSeconds: Long = TOKEN_EXPIRES_SECONDS
  protected val lockAcquireRetries: Int = LOCK_ACQUIRE_RETRIES

  val escapedToken: String = StringUtils.escapeNonAlphanumerics(token)
  val owner: String = s"${JvmUtils.jvmName}_${Math.abs(Random.nextInt())}"

  // State
  private var lockAcquired = false
  private var watcherThreadOpt: Option[Thread] = None

  protected def tryAcquireGuardLock(retries: Int, thisTry: Int): Boolean

  protected def releaseGuardLock(): Unit

  protected def updateTicket(): Unit

  /**
    * Attempts to acquire the lock for a specific token. If the lock is already acquired,
    * the method returns false. Otherwise, it tries to acquire the guard lock with a specified
    * number of retries, logs the acquisition, initializes the necessary resources, and returns true
    * if successful.
    *
    * This operation is thread-safe.
    *
    * @return true if the lock is successfully acquired; false if the lock is already held by this or another job.
    *         Note: Unlike standard lock implementations, this returns false even when the current instance already owns the lock.
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

  override def release(): Unit = {
    var wasAcquired = false

    this.synchronized {
      wasAcquired = lockAcquired
      if (lockAcquired) {
        lockAcquired = false
      }
    }

    if (wasAcquired) {
      watcherThreadOpt.foreach(_.interrupt())
      watcherThreadOpt = None
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
      // Same logic as for release() but without removal of the shutdown hook - too late for that.
      var wasAcquired = false
      this.synchronized {
        wasAcquired = lockAcquired
        if (lockAcquired) {
          lockAcquired = false
        }
      }
      if (wasAcquired) {
        watcherThreadOpt.foreach(_.interrupt())
        watcherThreadOpt = None
        releaseGuardLock()
      }
    }
  }

  /** This method starts a watcher thread that ensures locked token expiration time is updated. Invoke only from synchronized methods. */
  private def startWatcherThread(): Thread = {
    val thread = new Thread(s"TokenLockWatcher-$escapedToken") {
      override def run(): Unit = {
        lockWatcher()
      }
    }
    thread.setDaemon(true)
    thread.start()
    watcherThreadOpt = Some(thread)
    thread
  }

  /** This method runs in a separate thread and periodically updates the expiration time of the lock being held. */
  private def lockWatcher(): Unit = {
    var lastUpdateTime = Instant.now
    val waitMs = tokenExpiresSeconds * 200 // Convert to milliseconds, and divide by 5
    while (isAcquired) {
      try {
        Thread.sleep(waitMs)

        this.synchronized {
          if (isAcquired) {
            try {
              updateTicket()
            } catch {
              case NonFatal(ex) =>
                log.error(s"An error occurred when trying to update the lock: '$escapedToken'. Will be re-tried.", ex)
            }
            lastUpdateTime = Instant.now
          }
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return
      }
    }
  }
}

object TokenLockBase {
  val LOCK_ACQUIRE_RETRIES = 3
  val TOKEN_EXPIRES_SECONDS: Long = 10L * 60L
}
