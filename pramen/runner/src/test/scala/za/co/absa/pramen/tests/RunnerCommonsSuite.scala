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

package za.co.absa.pramen.tests

import org.apache.hadoop.conf.Configuration
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.base.{SparkTestBase, TempDirFixture}
import za.co.absa.pramen.runner.RunnerCommons

import java.nio.file.{Files, Paths}
import scala.util.control.NonFatal

class RunnerCommonsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  "copyFilesToLocal" should {
    "copy files from Hadoop locally" in {
      withTempDirectory("copy_to_local") {tempDir =>
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

  def silentDelete(file: String): Unit = {
    try {
      Files.delete(Paths.get(file))
    } catch {
      case NonFatal(_) => // do nothing
    }
  }

}
