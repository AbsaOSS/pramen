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

package za.co.absa.pramen.core.utils

import za.co.absa.pramen.core.exceptions.ThreadStackTrace

import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._
import scala.compat.Platform.EOL
import scala.util.Try

object JvmUtils {
  // Caching JVM name. In some cases this method can take some time to complete.
  lazy val jvmName: String = ManagementFactory.getRuntimeMXBean.getName

  def getShortExceptionDescription(ex: Throwable): String = {
    if (ex.getCause == null) {
      ex.getMessage
    } else {
      val cause = ex.getCause
      if (cause.getCause == null) {
        s"${ex.getMessage} (${ex.getCause.getMessage})"
      } else {
        s"${ex.getMessage} (${cause.getMessage} caused by ${cause.getCause.getMessage})"
      }
    }
  }

  def safeRemoveShutdownHook(hook: Thread): Unit = {
    Try {
      // Ignore runtime exceptions, including "java.lang.IllegalStateException: Shutdown in progress"
      Runtime.getRuntime.removeShutdownHook(hook)
    }
  }

  /**
    * Returns stack traces of all current non-daemon threads of the JVM.
    *
    * Includes the stack trace that called this method even if it is inside a daemon thread.
    * @return Stack traces of all threads in the JVM.
    */
  def getStackTraces: Seq[ThreadStackTrace] = {
    val stackTraces = Thread.getAllStackTraces.asScala

    stackTraces.flatMap { case (t: Thread, s: Array[StackTraceElement]) =>
      val containsSignalSource = s.exists(e => e.toString.contains("za.co.absa.pramen.core.utils.JvmUtils$.getStackTraces"))

      if (!containsSignalSource && t.isDaemon) {
        None
      } else {
        Option(ThreadStackTrace(t.getName, s))
      }
    }.toSeq
  }

  /**
    * Renders stack traces of multiple threads into a readable string format.
    *
    * Each stack trace is prefixed with the thread index and thread name, and formatted
    * with appropriate indentation for improved readability.
    *
    * @param stackTraces A sequence of `ThreadStackTrace` instances, where each instance
    *                    contains the thread name and its corresponding stack trace.
    * @return A formatted string representation of the provided stack traces.
    */
  def renderStackTraces(stackTraces: Seq[ThreadStackTrace]): String = {
    val threadTitlePadding = "  "
    val stackTracePadding = "    "

    stackTraces.zipWithIndex.map {
      case (threadStackTrace, index) =>
        val threadTitle = s"${threadTitlePadding}Thread $index (${threadStackTrace.threadName}): \n"
        val stackTrace = threadStackTrace.stackTrace
        val stackTraceStr = s"""${stackTrace.map(s => s"$stackTracePadding$s").mkString("", EOL, EOL)}""".stripMargin
        threadTitle + stackTraceStr
    }.mkString("\n")
  }

  /**
    * Determines the major version of the JVM currently in use.
    *
    * The major version is derived from the JVM's system property "java.version".
    * For Java 8 and earlier, the version format follows the "1.x" scheme (e.g., "1.8").
    * For Java 9 and later, the version directly reflects the major version (e.g., "9", "11").
    *
    * @return The major version of the JVM as a number.
    */
  lazy val jvmMajorVersion: Int = {
    val raw = sys.props.get("java.specification.version")
      .orElse(sys.props.get("java.version"))
      .getOrElse("5")
    val majorPortion = if (raw.startsWith("1.")) raw.drop(2) else raw
    val digits = majorPortion.takeWhile(_.isDigit)
    if (digits.nonEmpty) digits.toInt else 5
  }
}
