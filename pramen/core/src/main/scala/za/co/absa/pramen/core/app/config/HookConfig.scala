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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ClassLoaderUtils

case class HookConfig(
                       startupHook: Option[Runnable],
                       shutdownHook: Option[Runnable]
                     )

object HookConfig {
  val STARTUP_HOOK_CLASS_KEY = "pramen.hook.startup.class"
  val SHUTDOWN_HOOK_CLASS_KEY = "pramen.hook.shutdown.class"

  def fromConfig(conf: Config): HookConfig = {
    val startupHook = if (conf.hasPath(STARTUP_HOOK_CLASS_KEY) && conf.getString(STARTUP_HOOK_CLASS_KEY).nonEmpty) {
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](conf.getString(STARTUP_HOOK_CLASS_KEY), conf))
    } else {
      None
    }

    val shutdownHook = if (conf.hasPath(SHUTDOWN_HOOK_CLASS_KEY) && conf.getString(SHUTDOWN_HOOK_CLASS_KEY).nonEmpty) {
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](conf.getString(SHUTDOWN_HOOK_CLASS_KEY), conf))
    } else {
      None
    }

    HookConfig(startupHook, shutdownHook)
  }
}
