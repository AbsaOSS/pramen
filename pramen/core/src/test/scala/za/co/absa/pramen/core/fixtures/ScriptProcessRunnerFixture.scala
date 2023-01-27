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

package za.co.absa.pramen.core.fixtures

import org.apache.commons.io.FileUtils
import za.co.absa.pramen.core.mocks.DummyProcessRunner
import za.co.absa.pramen.core.process.{ProcessRunner, ProcessRunnerImpl}

import java.io.File
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.util.Random

trait ScriptProcessRunnerFixture {
  /**
    * Creates a ProcessRunner that won't actually run the script.
    *
    * @param includeOutputLines the number of stdout/stderr lines to remember.
    * @param logStdOut          is logging of stdout enabled
    * @param logStdErr          is logging of stderr enabled
    * @return An instance of a ProcessRunner
    */
  def withDummyProcessRunner(includeOutputLines: Int = 10,
                             logStdOut: Boolean = true,
                             logStdErr: Boolean = true,
                             redirectErrorStream: Boolean = false,
                             recordCountRegEx: Option[String] = None,
                             zeroRecordsSuccessRegEx: Option[String] = None,
                             failureRegEx: Option[String] = None,
                             outputFilterRegEx: Seq[String] = Nil
                            )(f: ProcessRunnerImpl => Unit): Unit = {
    val runner = new DummyProcessRunner(includeOutputLines,
      logStdOut,
      logStdErr,
      redirectErrorStream,
      recordCountRegEx,
      zeroRecordsSuccessRegEx,
      failureRegEx,
      outputFilterRegEx)

    f(runner)
  }

  /**
    * Creates a ProcessRunner from a content of a sh script and provides it to the unit test.
    *
    * @param script             The content of the sh script to run.
    * @param includeOutputLines the number of stdout/stderr lines to remember.
    * @param logStdOut          is logging of stdout enabled
    * @param logStdErr          is logging of stderr enabled
    * @return An instance of a ProcessRunner
    */
  def withRealScript(script: String,
                     includeOutputLines: Int = 10,
                     logStdOut: Boolean = true,
                     logStdErr: Boolean = true,
                     redirectErrorStream: Boolean = false,
                     zeroRecordSuccessRegEx: Option[String] = None,
                     failureRegEx: Option[String] = None
                    )(f: (ProcessRunnerImpl, String) => Unit): Unit = {
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

    val runner = ProcessRunner(includeOutputLines,
      logStdOut,
      logStdErr,
      redirectErrorStream = redirectErrorStream,
      zeroRecordsSuccessRegEx = zeroRecordSuccessRegEx,
      failureRegEx = failureRegEx).asInstanceOf[ProcessRunnerImpl]

    f(runner, outputFilePath.toAbsolutePath.toString)

    FileUtils.deleteDirectory(new File(pathStr))
  }
}
