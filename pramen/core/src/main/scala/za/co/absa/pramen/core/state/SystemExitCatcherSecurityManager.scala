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

package za.co.absa.pramen.core.state

import za.co.absa.pramen.core.exceptions.OsSignalException
import za.co.absa.pramen.core.utils.JvmUtils

import java.security.Permission

class SystemExitCatcherSecurityManager(stateImpl: PipelineStateImpl) extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}

  override def checkPermission(perm: Permission, context: Object): Unit = {}

  override def checkExit(status: Int): Unit = {
    val nonDaemonStackTraces = JvmUtils.getStackTraces
    stateImpl.setSignalException(OsSignalException("System.exit()", nonDaemonStackTraces))
  }
}
