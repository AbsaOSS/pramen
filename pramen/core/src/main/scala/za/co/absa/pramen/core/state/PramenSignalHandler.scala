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

import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}
import za.co.absa.pramen.core.exceptions.{OsSignalException, ThreadStackTrace}
import za.co.absa.pramen.core.state.PipelineStateImpl.EXIT_CODE_SIGNAL_RECEIVED

import scala.collection.JavaConverters._

class PramenSignalHandler(signal: Signal, signalName: String, pipelineState: PipelineStateImpl) extends SignalHandler {
  private val log = LoggerFactory.getLogger(this.getClass)

  var oldHandler: Option[SignalHandler] = None

  override def handle(sig: Signal): Unit = {
    val stackTraces = Thread.getAllStackTraces.asScala

    val nonDaemonStackTraces = stackTraces.flatMap { case (t: Thread, s: Array[StackTraceElement]) =>
      if (t.isDaemon) {
        None
      } else {
        Option(ThreadStackTrace(t.getName, s))
      }
    }.toSeq

    val ex = OsSignalException(signalName, nonDaemonStackTraces)
    pipelineState.setSignalException(ex)

    log.warn(s"Got signal from the operating system: $signalName", ex)

    oldHandler match {
      case Some(handler) =>
        handler.handle(sig)
      case None =>
        // In Pramen context this should not happen since the old handler should always be available
        val exitCode = pipelineState.getExitCode | EXIT_CODE_SIGNAL_RECEIVED
        System.exit(exitCode)
    }
  }

  def setOldSignalHandler(signalHandler: SignalHandler): Unit = oldHandler = Option(signalHandler)

  def getOldSignalHandler: Option[SignalHandler] = oldHandler

  def unhandle(): Unit = oldHandler.foreach(handler => Signal.handle(signal, handler))
}
