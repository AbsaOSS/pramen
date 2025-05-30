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
import za.co.absa.pramen.core.utils.FsUtils

class TokenLockHadoopPath(token: String,
                          hadoopConf: Configuration,
                          locksPath: String) extends TokenLockBase(token) {
  private val fsUtils = new FsUtils(hadoopConf, locksPath)
  private var fileGuardOpt: Option[Path] = None

  init()

  /** Invoked from a synchronized block. */
  override def tryAcquireGuardLock(retries: Int, thisTry: Int): Boolean = {
    val fileGuard = new Path(locksPath, s"$escapedToken.lock")

    val lockAcquired = fsUtils.isFileGuardOwned(fileGuard, tokenExpiresSeconds)
    if (lockAcquired) {
      fileGuardOpt = Some(fileGuard)
    }
    lockAcquired
  }

  /** Invoked from a synchronized block. */
  override def releaseGuardLock(): Unit = {
    fileGuardOpt.foreach { fileGuard =>
      fsUtils.deleteFile(fileGuard)
      fileGuardOpt = None
    }
  }

  /** Invoked from a synchronized block. */
  override def updateTicket(): Unit = {
    fileGuardOpt.foreach { fileGuard =>
      fsUtils.updateFileGuard(fileGuard, tokenExpiresSeconds)
    }
  }

  private def init(): Unit = {
    fsUtils.createDirectoryRecursive(new Path(locksPath))
  }
}
