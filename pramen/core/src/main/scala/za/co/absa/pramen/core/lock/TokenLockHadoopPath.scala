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
  private var fileGuard: Option[Path] = None

  init()

  override def tryAcquireGuardLock(retries: Int, thisTry: Int): Boolean = {
    fileGuard = Some(new Path(locksPath, s"$token.lock"))

    fsUtils.isFileGuardOwned(fileGuard.get, tokenExpiresSeconds)
  }

  override def releaseGuardLock(owner: String): Unit = {
    if (fileGuard.isDefined) {
      fsUtils.deleteFile(fileGuard.get)
      fileGuard = None
    }
  }

  override def updateTicket(): Unit = {
    if (fileGuard.isDefined) {
      fsUtils.updateFileGuard(fileGuard.get, tokenExpiresSeconds)
    }
  }

  private def init(): Unit = {
    fsUtils.createDirectoryRecursive(new Path(locksPath))
  }
}
