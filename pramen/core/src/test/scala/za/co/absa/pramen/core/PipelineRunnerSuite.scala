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

package za.co.absa.pramen.core

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.mocks.{ExitException, NoExitSecurityManager}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.runner.PipelineRunner

import java.nio.file.Paths

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

    "exit with zero exit code on a pipeline with minimal configuration" in {
      val conf = ConfigFactory.parseString(
        """pramen {
          |  pipeline.name = "Test pipeline"
          |  bookkeeping.enabled = false
          |  stop.spark.session = false
          |  allow.empty.pipeline = true
          |}
          |""".stripMargin)
      withTempDirectory("pramen_main") { tempDir =>
        val workflowPath = Paths.get(tempDir, "workflow.conf").toString

        ConfigUtils.writeConfigToFile(conf, workflowPath)

        try {
          PipelineRunner.main(Array("--workflow", workflowPath))
          assert(PipelineRunner.getExitCodes.length == 1)
          assert(PipelineRunner.getExitCodes.head == 0)
        } catch {
          case e: ExitException =>
            assert(e.status == 0)
        }
      }
    }
  }
}

