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

package za.co.absa.pramen.core.tests.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.exceptions.ThreadStackTrace
import za.co.absa.pramen.core.utils.JvmUtils

import java.util.concurrent.locks.ReentrantLock

class JvmUtilsSuite extends AnyWordSpec {
  "getShortExceptionDescription()" should {
    "work with an exception that does not have a cause" in {
      val actual = JvmUtils.getShortExceptionDescription(new RuntimeException("Dummy1"))

      assert(actual == "Dummy1")
    }

    "work with an exception that has a cause" in {
      val actual = JvmUtils.getShortExceptionDescription(new RuntimeException("Dummy1", new RuntimeException("Dummy2")))

      assert(actual == "Dummy1 (Dummy2)")
    }

    "work with an exception that has a cause of a cause" in {
      val actual = JvmUtils.getShortExceptionDescription(new RuntimeException("Dummy1", new RuntimeException("Dummy2", new RuntimeException("Dummy3"))))

      assert(actual == "Dummy1 (Dummy2 caused by Dummy3)")
    }
  }

  "getStackTraces" should {
    "return all threads stack traces" in {
      val stackTraces = JvmUtils.getStackTraces

      val stackTracesStr = JvmUtils.renderStackTraces(stackTraces)

      assert(stackTracesStr.contains("za.co.absa.pramen.core.utils.JvmUtils$.getStackTraces"))
    }

    "include the daemon thread of the caller class" in {
      val lock = new ReentrantLock()
      var stackTraces: Seq[ThreadStackTrace] = null
      val daemonThread = new Thread("daemon-thread") {
        override def run(): Unit = {
          lock.lock()
          stackTraces = JvmUtils.getStackTraces
          lock.unlock()
        }
      }
      daemonThread.setDaemon(true)
      daemonThread.start()

      assert(daemonThread.isDaemon)

      daemonThread.join()

      assert(!daemonThread.isAlive)

      val stackTracesStr = JvmUtils.renderStackTraces(stackTraces)

      assert(stackTracesStr.contains("za.co.absa.pramen.core.utils.JvmUtils$.getStackTraces"))
      assert(stackTracesStr.contains("(daemon-thread)"))
    }
  }
}
