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

import com.github.yruslan.channel.{Channel, WriteChannel}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.runner.task.ThreadClosableRegistry
import za.co.absa.pramen.core.utils.ThreadUtils

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ThreadUtilsSuite extends AnyWordSpec {
  "runWithTimeout" should  {
    "run the action normally when the timeout is not breached" in {
      ThreadUtils.runWithTimeout(Duration(10, TimeUnit.SECONDS)) {
        Thread.sleep(1)
      }
    }

    "throw an exception when timeout is breached" in {
      val ex = intercept[RuntimeException] {
        ThreadUtils.runWithTimeout(Duration(1, TimeUnit.MILLISECONDS)) {
          Thread.sleep(2000)
        }
      }

      assert(ex.getMessage.contains("Timeout expired (instantly)."))
      assert(ex.getCause != null)
      assert(ex.getCause.isInstanceOf[RuntimeException])
      assert(ex.getCause.getStackTrace.nonEmpty)
    }

    "pass the thrown exception to the caller" in {
      val ex = intercept[IllegalStateException] {
        ThreadUtils.runWithTimeout(Duration(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test")
        }
      }

      assert(ex.getMessage == "test")
    }

    "closable is called when registered" in {
      val (closeable1, getCount1) = createCountingCloseable(0)

      val timeout = Duration(1, TimeUnit.SECONDS)
      val ex = intercept[RuntimeException] {
        ThreadUtils.runWithTimeout(timeout, timeout) {
          ThreadClosableRegistry.registerCloseable(closeable1)
          Thread.sleep(10000)
        }
      }

      assert(ex.getMessage.startsWith("Timeout expired"))
      assert(getCount1() == 1)
    }

    "closable continues to run in detached thread when timed out" in {
      val ch = Channel.make[Int]
      val (closeable1, _) = createCountingCloseable(1000, Some(ch))

      val start = Instant.now
      val timeout = Duration(1, TimeUnit.SECONDS)
      val closeTimeout = Duration(1, TimeUnit.MILLISECONDS)
      val ex = intercept[RuntimeException] {
        ThreadUtils.runWithTimeout(timeout, closeTimeout) {
          ThreadClosableRegistry.registerCloseable(closeable1)
          Thread.sleep(10000)
        }
      }

      assert(ex.getMessage.startsWith("Timeout expired"))

      val num = ch.recv()
      assert(num == 2)

      val finish = Instant.now
      assert(java.time.Duration.between(start, finish).getSeconds < 10)
    }
  }

  private def createCountingCloseable(waitTimeMs: Int, closingChannel: Option[WriteChannel[Int]] = None): (AutoCloseable, () => Int) = {
    var count = 0
    val closeable = new AutoCloseable {
      override def close(): Unit = {
        this.synchronized {
          count += 1
        }
        if (waitTimeMs > 0) {
          Thread.sleep(waitTimeMs)
          this.synchronized {
            count += 1
          }
          closingChannel.foreach(c => c.send(count) )
        }
      }
    }
    (closeable, () => count)
  }

}
