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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.{FsUtils, StringUtils}

import java.time.Instant

class TokenLockHadoopPath(token: String,
                          hadoopConf: Configuration,
                          locksPath: String,
                          tokenExpireTimeSeconds: Long = 10L * 60L) extends TokenLock {
  // Dependencies
  private val log = LoggerFactory.getLogger(this.getClass)
  private val fsUtils = new FsUtils(hadoopConf, locksPath)

  // Configuration
  private var fileGuard: Option[Path] = None
  private val escapedToken = StringUtils.escapeNonAlphanumerics(token)

  // State
  private var lockAcquired = false

  init()

  override def tryAcquire(): Boolean = synchronized {
    if (lockAcquired) {
      false
    } else {
      if (tryAcquireGuardLock(escapedToken)) {
        lockAcquired = true
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        log.info(s"Lock '$token' lock acquired: '$fileGuard'.")
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
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
      log.info(s"Lock released: '${fileGuard.get}'.")
      fileGuard = None
    }
  }

  override def close(): Unit = {}

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (lockAcquired) {
        releaseGuardLock()
      }
    }
  }

  private def tryAcquireGuardLock(token: String): Boolean = synchronized {
    fileGuard = Some(new Path(locksPath, s"$token.lock"))

    if (fsUtils.isFileGuardOwned(fileGuard.get, tokenExpireTimeSeconds)) {
      startWatcherThread()
      true
    } else {
      false
    }
  }

  private def releaseGuardLock(): Unit = synchronized {
    fsUtils.deleteFile(fileGuard.get)
  }

  private def isAcquired: Boolean = synchronized {
    lockAcquired
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
      if (Instant.now().isAfter(lastUpdateTime.plusMillis((tokenExpireTimeSeconds * 1000) / 5))) {
        this.synchronized {
          if (isAcquired && fileGuard.isDefined) {
            fsUtils.updateFileGuard(fileGuard.get, tokenExpireTimeSeconds)
            lastUpdateTime = Instant.now
          }
        }
      }
    }
  }

  private def init(): Unit = {
    fsUtils.createDirectoryRecursive(new Path(locksPath))
  }
}
