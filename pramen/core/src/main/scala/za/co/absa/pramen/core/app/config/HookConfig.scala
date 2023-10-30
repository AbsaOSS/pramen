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
                       initHook: Option[Runnable],
                       finalHook: Option[Runnable]
                     )

object HookConfig {
  val INIT_HOOK_CLASS_KEY = "pramen.hook.init.class"
  val FINAL_HOOK_CLASS_KEY = "pramen.hook.final.class"

  def fromConfig(conf: Config): HookConfig = {
    val initHook = if (conf.hasPath(INIT_HOOK_CLASS_KEY) && conf.getString(INIT_HOOK_CLASS_KEY).nonEmpty) {
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](conf.getString(INIT_HOOK_CLASS_KEY), conf))
    } else {
      None
    }

    val finalHook = if (conf.hasPath(FINAL_HOOK_CLASS_KEY) && conf.getString(FINAL_HOOK_CLASS_KEY).nonEmpty) {
      Option(ClassLoaderUtils.loadConfigurableClass[Runnable](conf.getString(FINAL_HOOK_CLASS_KEY), conf))
    } else {
      None
    }

    HookConfig(initHook, finalHook)
  }
}
