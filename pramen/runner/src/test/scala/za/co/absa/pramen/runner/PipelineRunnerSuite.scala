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

package za.co.absa.pramen.runner

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.base.{ExitException, NoExitSecurityManager, SparkTestBase, TempDirFixture}

class PipelineRunnerSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {
  override def beforeAll(): Unit = System.setSecurityManager(new NoExitSecurityManager())

  override def afterAll(): Unit = System.setSecurityManager(null)

  "system.exit" should {
    "not exit" in {
      try {
        System.exit(1)
        fail("shouldn't read this code")
      } catch {
        case e: ExitException =>
          assert(e.status == 1)
      }
    }
  }

  "PipelineRunner.main" should {
    "exit with non-zero exit code on error" in {
      try {
        PipelineRunner.main(Array.empty[String])
        fail("shouldn't read this code")
      } catch {
        case e: ExitException =>
          assert(e.status != 0)
      }
    }
  }
}
