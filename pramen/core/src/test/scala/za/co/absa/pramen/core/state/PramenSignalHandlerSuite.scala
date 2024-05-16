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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import sun.misc.{Signal, SignalHandler}

class PramenSignalHandlerSuite extends AnyWordSpec {
  private val conf = ConfigFactory.parseString("""pramen.pipeline.name = "test"""")
    .withFallback(ConfigFactory.load())

  "handle" should {
    "run the handler and the old handler" in {
      val notificationBuilder = new NotificationBuilderImpl
      val stateImpl = new PipelineStateImpl()(conf, notificationBuilder)
      var reached = false

      val oldSignalHandlerMock = new SignalHandler {
        override def handle(sig: Signal): Unit = reached = true
      }

      val signalHandler = new PramenSignalHandler(new Signal("PIPE"), "Interrupt", stateImpl)

      signalHandler.setOldSignalHandler(oldSignalHandlerMock)
      assert(signalHandler.getOldSignalHandler.get == oldSignalHandlerMock)
      assert(!reached)
      Signal.handle(new Signal("PIPE"), signalHandler)
      signalHandler.handle(null)
      signalHandler.unhandle()
      assert(reached)
      assert(stateImpl.getSignalException.nonEmpty)
      assert(stateImpl.getFailureException.isEmpty)
    }
  }

}
