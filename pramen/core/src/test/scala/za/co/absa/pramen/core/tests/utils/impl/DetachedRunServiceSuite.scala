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

package za.co.absa.pramen.core.tests.utils.impl

import com.github.yruslan.channel.Channel
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.impl._

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

class DetachedRunServiceSuite extends AnyWordSpec {
  "runDetached" should {
    "execution should return the expected result" in {
      val fut = DetachedRunService.runDetached {
        42
      }

      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
    }

    "execution should be delayed" in {
      val channel = Channel.make[Int]
      val fut = DetachedRunService.runDetached {
        channel.recv()
      }

      assert(!fut.isCompleted)
      channel.send(42)

      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
    }

    "execution should propagate the exception" in {
      val fut = DetachedRunService.runDetached {
        throw new RuntimeException("Test")
        42
      }

      val ex = intercept[RuntimeException] {
        Await.result(fut, 5.seconds)
      }
      assert(ex.getMessage == "Test")
    }
  }

  "runWithTimeoutThenDetach" should {
    "execute normally when timeout is not breached" in {
      val timeout = Duration(1, TimeUnit.SECONDS)
      val ch = Channel.make[Int](1)

      val fut = DetachedRunService.runWithTimeoutThenDetach(timeout) {
        ch.send(1)
        42
      }

      val num = ch.recv()
      assert(num == 1)
      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
    }

    "execute in detached thread anyway when timeout is breached" in {
      val timeout = Duration(1, TimeUnit.MILLISECONDS)
      val ch = Channel.make[Int]
      var isDaemonThread = false

      val fut = DetachedRunService.runWithTimeoutThenDetach(timeout) {
        ch.send(1)
        this.synchronized {
          isDaemonThread = Thread.currentThread().isDaemon
        }
        42
      }

      val num = ch.recv()
      assert(num == 1)

      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
      assert(isDaemonThread)
    }
  }
}
