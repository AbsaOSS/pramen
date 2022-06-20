/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.fixtures

import java.io.File
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.commons.io.FileUtils
import za.co.absa.pramen.DummyProcessRunner
import za.co.absa.pramen.builtin.process.ProcessRunner

import scala.util.Random

trait ScriptProcessRunnerFixture {

  /**
    * Creates a ProcessRunner that won't actually run the script.
    *
    * @param outputRecordCountRegEx A RegEx expression with groups to parse output record count.
    * @param outputFiltersRegEx     RegRx filters of the command line output
    * @return An instance of a ProcessRunner
    */
  def withDummyProcessRunner(outputRecordCountRegEx: String,
                             zeroRecordSuccessRegEx: Option[String],
                             noDataFailureRegEx: Option[String],
                             outputFiltersRegEx: Seq[String])(f: ProcessRunner => Unit): Unit = {
    val runner = new DummyProcessRunner(outputRecordCountRegEx, zeroRecordSuccessRegEx, noDataFailureRegEx, outputFiltersRegEx)

    f(runner)
  }

  /**
    * Creates a ProcessRunner from a content of a sh script and provides it to the unit test.
    *
    * @param script                 The content of the sh script to run.
    * @param outputRecordCountRegEx A RegEx expression with groups to parse output record count.
    * @param outputFiltersRegEx     RegRx filters of the command line output
    * @return An instance of a ProcessRunner
    */
  def withRealScript(script: String,
                     outputRecordCountRegEx: String,
                     zeroRecordSuccessRegEx: Option[String],
                     noDataFailureRegEx: Option[String],
                     outputFiltersRegEx: Seq[String])(f: ProcessRunner => Unit): Unit = {
    val tmpPath = Files.createTempDirectory("ProcessRunner")
    val pathStr = tmpPath.toAbsolutePath.toString

    val fileName = s"script_${Random.nextInt(10000)}.sh"

    val outputFilePath = Paths.get(pathStr, fileName)

    import PosixFilePermission._

    val permissions = new java.util.HashSet[PosixFilePermission]
    permissions.add(OWNER_EXECUTE)
    permissions.add(OWNER_READ)
    permissions.add(OWNER_WRITE)

    Files.write(outputFilePath, script.getBytes(), StandardOpenOption.CREATE)
    Files.setPosixFilePermissions(outputFilePath, permissions)

    val runner = new ProcessRunner(outputFilePath.toAbsolutePath.toString,
      outputRecordCountRegEx,
      zeroRecordSuccessRegEx,
      noDataFailureRegEx,
      outputFiltersRegEx,
      200)

    f(runner)

    FileUtils.deleteDirectory(new File(pathStr))
  }
}
