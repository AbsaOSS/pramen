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
import za.co.absa.pramen.core.utils.{ClassLoaderUtils, ConfigUtils}

import scala.util.{Failure, Success, Try}

case class HookConfig(
                       startupHook: Option[Try[Runnable]],
                       shutdownHook: Option[Try[Runnable]]
                     )

object HookConfig {
  val STARTUP_HOOK_CLASS_KEY = "pramen.hook.startup.class"
  val SHUTDOWN_HOOK_CLASS_KEY = "pramen.hook.shutdown.class"

  private val log = LoggerFactory.getLogger(this.getClass)

  def fromConfig(conf: Config): HookConfig = {
    val startupHookClass = ConfigUtils.getOptionString(conf, STARTUP_HOOK_CLASS_KEY).filter(_.nonEmpty)
    val shutdownHookClass = ConfigUtils.getOptionString(conf, SHUTDOWN_HOOK_CLASS_KEY).filter(_.nonEmpty)

    val startupHook = startupHookClass.map(getRunnable(_, conf))
    val shutdownHook = shutdownHookClass.map(getRunnable(_, conf))

    startupHookClass.foreach(clazz => log.info(s"Using the startup hook class '$clazz'..."))
    shutdownHookClass.foreach(clazz => log.info(s"Using the shutdown hook class '$clazz'..."))

    HookConfig(startupHook, shutdownHook)
  }

  private[core] def getRunnable(clazz: String, conf: Config): Try[Runnable] = {
    // Cannot use Try{} here since it can't capture fatal errors, that include linkage errors
    try {
      Success(ClassLoaderUtils.loadConfigurableClass[Runnable](clazz, conf))
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }
}
