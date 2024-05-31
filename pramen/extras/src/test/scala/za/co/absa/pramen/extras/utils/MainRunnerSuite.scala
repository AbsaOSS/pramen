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

package za.co.absa.pramen.extras.utils

import org.scalatest.wordspec.AnyWordSpec

class MainRunnerSuite extends AnyWordSpec {
  "runMain()" should {
    "run a main method of a given class" in {
      val args = Array("arg1", "arg2")
      val ex = intercept[RuntimeException] {
        MainRunner.runMain("za.co.absa.pramen.extras.mocks.AppMainMock", args)
      }

      assert(ex.getMessage.contains("Main reached"))
      assert(ex.getMessage.contains("arg1 arg2"))
    }

    "throw an exception if the object does not conform" in {
      intercept[NoSuchMethodException] {
        MainRunner.runMain("za.co.absa.pramen.extras.mocks.DummyKafkaConfigFactory", null)
      }
    }
  }
}
