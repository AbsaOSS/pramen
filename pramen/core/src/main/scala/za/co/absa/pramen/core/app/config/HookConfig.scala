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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.ClassLoaderUtils

case class HookConfig(
                       startupHook: Option[Runnable],
                       shutdownHook: Option[Runnable]
                     )

object HookConfig {
  val STARTUP_HOOK_CLASS_KEY = "pramen.hook.startup.class"
  val SHUTDOWN_HOOK_CLASS_KEY = "pramen.hook.shutdown.class"

  private val log = LoggerFactory.getLogger(this.getClass)

  def fromConfig(conf: Config): HookConfig = {
    val startupHook = if (conf.hasPath(STARTUP_HOOK_CLASS_KEY) && conf.getString(STARTUP_HOOK_CLASS_KEY).nonEmpty) {
      val hookClass = conf.getString(STARTUP_HOOK_CLASS_KEY)
      log.info(s"Loading the startup hook class '$hookClass'")
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](hookClass, conf))
    } else {
      log.info(s"Startup hook is not defined.")
      None
    }

    val shutdownHook = if (conf.hasPath(SHUTDOWN_HOOK_CLASS_KEY) && conf.getString(SHUTDOWN_HOOK_CLASS_KEY).nonEmpty) {
      val hookClass = conf.getString(SHUTDOWN_HOOK_CLASS_KEY)
      log.info(s"Loading the shutdown hook class '$hookClass'")
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](hookClass, conf))
    } else {
      log.info(s"Shutdown hook is not defined.")
      None
    }

    HookConfig(startupHook, shutdownHook)
  }
}
