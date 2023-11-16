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

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture

import java.nio.file.{Files, Paths}
import scala.util.control.NonFatal

class RunnerCommonsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with BeforeAndAfterAll {
  private var tempDir: String = _
  private val configInTheCurrentDirectory = Paths.get("test_current.conf")

  override def afterAll(): Unit = {
    deleteDir(tempDir)

    if (Files.exists(configInTheCurrentDirectory)) {
      Files.delete(configInTheCurrentDirectory)
    }

    super.afterAll()
  }

  "copyFilesToLocal" should {
    "copy files from Hadoop locally" in {
      withTempDirectory("copy_to_local") { tempDir =>
        createTextFile(tempDir, "1.dat", "123")
        createTextFile(tempDir, "2.dat", "123")

        val files = Seq(Paths.get(tempDir, "1.dat").toString, Paths.get(tempDir, "2.dat").toString)
        val hadoopConfig = new Configuration

        RunnerCommons.copyFilesToLocal(files, hadoopConfig)

        val exists1 = Files.exists(Paths.get("1.dat"))
        val exists2 = Files.exists(Paths.get("2.dat"))

        silentDelete("1.dat")
        silentDelete("2.dat")
        silentDelete(".1.dat.crc")
        silentDelete(".2.dat.crc")

        assert(exists1)
        assert(exists2)
      }
    }
  }

  "getExistingWorkflowPath()" when {
    tempDir = createTempDir("workflow_test")
    val innedDir = Paths.get(tempDir, "inner")

    val absolute = Paths.get(innedDir.toString, "absolute.conf").toAbsolutePath

    if (Files.exists(configInTheCurrentDirectory)) {
      Files.delete(configInTheCurrentDirectory)
    }

    Files.createDirectory(innedDir)
    Files.createFile(configInTheCurrentDirectory)
    Files.createFile(absolute)

    "absolute path provided" should {
      "return the path if it is available" in {
        val fileName = RunnerCommons.getExistingWorkflowPath(absolute.toString)

        assert(fileName == absolute.toString)
      }

      "return the file name if the file is available at the current dir" in {
        val fileName = RunnerCommons.getExistingWorkflowPath(Paths.get("dummy", configInTheCurrentDirectory.toString).toString)

        assert(fileName == "test_current.conf")
      }

      "throw an exception if the file does not exist" in {
        assertThrows[IllegalArgumentException] {
          RunnerCommons.getExistingWorkflowPath(Paths.get("dummy", "dummy.conf").toString)
        }
      }
    }

    "just file name provided provided" should {
      "return the file name if it is available" in {
        val fileName = RunnerCommons.getExistingWorkflowPath(Paths.get("dummy", configInTheCurrentDirectory.toString).toString)

        assert(fileName == "test_current.conf")
      }

      "throw an exception if the file does not exist" in {
        assertThrows[IllegalArgumentException] {
          RunnerCommons.getExistingWorkflowPath(Paths.get("dummy.conf").toString)
        }
      }
    }
  }

  def silentDelete(file: String): Unit = {
    try {
      Files.delete(Paths.get(file))
    } catch {
      case NonFatal(_) => // do nothing
    }
  }

}
