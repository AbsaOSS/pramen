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
import za.co.absa.pramen.core.utils.ThreadUtils

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
          Thread.sleep(1000)
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
  }

}
