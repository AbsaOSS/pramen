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

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.PramenHookSpy

import java.time.LocalDate

class HookConfigSuite extends AnyWordSpec {
  "RuntimeConfig" should {
    "deserialize the config properly" in {
      val configStr =
        s"""pramen.hook {
           |  startup.class = "za.co.absa.pramen.core.PramenHookSpy"
           |  shutdown.class = "za.co.absa.pramen.core.PramenHookSpy"
           |}
           |""".stripMargin

      val conf = getUseCase(configStr)

      val hookConfig = HookConfig.fromConfig(conf)

      assert(hookConfig.startupHook.isDefined)
      assert(hookConfig.shutdownHook.isDefined)

      val hook = hookConfig.startupHook.get.get.asInstanceOf[PramenHookSpy]

      assert(hook.runCallCount == 0)
      assert(hook.conf == conf)
    }

    "have default values" in {
      val conf = ConfigFactory.load()

      val hookConfig = HookConfig.fromConfig(conf)

      assert(hookConfig.startupHook.isEmpty)
      assert(hookConfig.shutdownHook.isEmpty)
    }
  }

  "throw RuntimeException when the init hook can't be created" in {
    val conf = getUseCase("pramen.hook.startup.class = \"za.co.absa.pramen.core.NonExistentHook\"")

    val ex = intercept[ClassNotFoundException] {
      HookConfig.fromConfig(conf).startupHook.get.get
    }

    assert(ex.getMessage.contains("za.co.absa.pramen.core.NonExistentHook"))
  }

  "throw RuntimeException when the final hook can't be created" in {
    val conf = getUseCase("pramen.hook.shutdown.class = \"za.co.absa.pramen.core.NonExistentHook\"")

    val ex = intercept[ClassNotFoundException] {
      HookConfig.fromConfig(conf).shutdownHook.get.get
    }

    assert(ex.getMessage.contains("za.co.absa.pramen.core.NonExistentHook"))
  }

  def getUseCase(rawConfig: String = ""): Config = {
    ConfigFactory.parseString(rawConfig)
      .withFallback(ConfigFactory.load())
      .resolve()
  }

}
