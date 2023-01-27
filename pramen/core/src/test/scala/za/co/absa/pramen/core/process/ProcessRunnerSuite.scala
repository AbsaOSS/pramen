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

package za.co.absa.pramen.core.process

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.ScriptProcessRunnerFixture
import za.co.absa.pramen.core.utils.CircularBuffer

import java.io.{BufferedReader, IOException, StringReader}

class ProcessRunnerSuite extends AnyWordSpec with ScriptProcessRunnerFixture {
  "processExecutionOutput()" should {
    "handle stdout separately from stderr" in {
      withDummyProcessRunner() { runner =>
        val stdout = new BufferedReader(new StringReader(
          """First line
            |Second line
            |""".stripMargin))

        val stderr = new BufferedReader(new StringReader(
          """Error line 1
            |""".stripMargin))

        runner.processExecutionOutput(stdout, stderr)

        assert(runner.getLastStdoutLines.length == 2)
        assert(runner.getLastStderrLines.length == 1)
      }
    }

    "handle long streams" in {
      def generateLongString(): String = {
        val line = "a" * 120
        val lines = new StringBuilder
        for (i <- Range(0, 100000)) {
          lines.append(s"$i $line\n")
        }
        lines.toString()
      }

      withDummyProcessRunner() { runner =>
        val str1 = generateLongString()

        val stdout = new BufferedReader(new StringReader(
          str1 +
            """First line
              |Second line
              |""".stripMargin))

        val stderr = new BufferedReader(new StringReader(
          str1 +
            """Error line 1
              |""".stripMargin))

        runner.processExecutionOutput(stdout, stderr)

        assert(runner.getLastStdoutLines.length == 10)
        assert(runner.getLastStderrLines.length == 10)
        assert(runner.getLastStdoutLines.last == "Second line")
        assert(runner.getLastStderrLines.last == "Error line 1")
      }
    }
  }

  "processReader()" should {
    "read everything from a reader" in {
      withDummyProcessRunner() { runner =>
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Records:10A
            |Last Line
            |""".stripMargin))

        val buffer = new CircularBuffer[String](10)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(buffer.get().length == 4)
      }
    }

    "read no more than the maximum number of records" in {
      withDummyProcessRunner() { runner =>
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Records:10A
            |Last Line
            |""".stripMargin))

        val buffer = new CircularBuffer[String](2)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(buffer.get().length == 2)
        assert(buffer.get().head == "Records:10A")
        assert(buffer.get()(1) == "Last Line")
        assert(runner.recordCount.isEmpty)
        assert(!runner.isFailureFound)
      }
    }

    "get record count via a regex" in {
      withDummyProcessRunner(recordCountRegEx = Some("RecordCount=(\\d+)")) { runner =>
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |RecordCount=1000
            |Records:10A
            |Last Line
            |""".stripMargin))

        val buffer = new CircularBuffer[String](2)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(runner.recordCount.contains(1000))
      }
    }

    "get failure via a regex" in {
      withDummyProcessRunner(failureRegEx = Some("FAILED")) { runner =>
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Job FAILED
            |Records:10A
            |Last Line
            |""".stripMargin))

        val buffer = new CircularBuffer[String](2)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(runner.isFailureFound)
      }
    }

    "filters output according to a list of regexp" in {
      withDummyProcessRunner(outputFilterRegEx = Seq("Filter\\d*", "Delete\\d+")) { runner =>
        val reader = new BufferedReader(new StringReader(
          """Filter0
            |Second line
            |Filter
            |Filter1
            |Delete22
            |Last Line
            |""".stripMargin))

        val buffer = new CircularBuffer[String](10)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(buffer.get().length == 2)
        assert(buffer.get().head == "Second line")
        assert(buffer.get()(1) == "Last Line")
      }
    }

    "handle exceptions" in {
      withDummyProcessRunner() { runner =>
        val reader: BufferedReader = null

        val buffer = new CircularBuffer[String](2)
        runner.processReader(reader, Some(buffer), "out", logEnabled = true)

        assert(buffer.get().isEmpty)
      }
    }
  }

  "line transformers" when {
    "doIf" should {
      "do the action when the condition is true" in {
        withDummyProcessRunner() { runner =>
          val line = "A"
          var result: String = null

          runner.doIf(line, actionNeeded = true)(a => result = s"${a}B")
          assert(result == "AB")
        }
      }
      "don't do the action when the condition is false" in {
        withDummyProcessRunner() { runner =>
          val line = "A"
          var result: String = "Original value"

          runner.doIf(line, actionNeeded = false)(a => result = s"${a}B")
          assert(result == "Original value")
        }
      }
    }

    "filterEmpty()" should {
      "filter out empty lines" in {
        withDummyProcessRunner() { runner =>
          val line = ""
          val filtered = runner.filterEmpty(line)
          assert(filtered.isEmpty)
        }
      }
      "keep non-empty lines" in {
        withDummyProcessRunner() { runner =>
          val line = "A"
          val filtered = runner.filterEmpty(line)
          assert(filtered.contains("A"))
        }
      }
    }

    "skipFilteredOutLines" should {
      "skip a line if it matches one of regexes" in {
        withDummyProcessRunner(outputFilterRegEx = Seq("Filter\\d*", "Delete\\d+")) { runner =>
          val line = "Filter0"
          val filtered = runner.skipFilteredOutLines(line)

          assert(filtered.isEmpty)
        }

      }

      "do not skip the line if it does not match any filters" in {
        withDummyProcessRunner(outputFilterRegEx = Seq("Filter\\d*", "Delete\\d+")) { runner =>
          val line = "A"
          val filtered = runner.skipFilteredOutLines(line)

          assert(filtered.contains("A"))
        }
      }

      "do not skip the line on an empty list" in {
        withDummyProcessRunner() { runner =>
          val line = "A"
          val filtered = runner.skipFilteredOutLines(line)

          assert(filtered.contains("A"))
        }
      }
    }

    "extractRecordCount" should {
      "extract record count from a line" in {
        withDummyProcessRunner() { runner =>
          val line = "RecordCount=1000"
          runner.extractRecordCount(line, "RecordCount=(\\d+)".r)

          assert(runner.recordCount.contains(1000))
        }
      }

      "return None if the line does not match the regex" in {
        withDummyProcessRunner() { runner =>
          val line = "A"
          val recordCount = runner.extractRecordCount(line, "RecordCount=(\\d+)".r)

          assert(runner.recordCount.isEmpty)
        }
      }

      "do not rethrow the number parsing expression" in {
        withDummyProcessRunner() { runner =>
          val line = "RecordCount=1000A"
          runner.extractRecordCount(line, "RecordCount=(.+)".r)

          assert(runner.recordCount.isEmpty)
        }
      }

      "failures do not supersede successes" in {
        withDummyProcessRunner() { runner =>
          val expr = "RecordCount=(.+)".r
          val line1 = "RecordCount=2000"
          val line2 = "RecordCount=1000A"
          runner.extractRecordCount(line1, expr)
          runner.extractRecordCount(line2, expr)

          assert(runner.recordCount.contains(2000L))
        }
      }

      "new updates supersede the old record count" in {
        withDummyProcessRunner() { runner =>
          val expr = "RecordCount=(.+)".r
          val line1 = "RecordCount=1000"
          val line2 = "RecordCount=2000A"
          val line3 = "RecordCount=3000"
          runner.extractRecordCount(line1, expr)
          runner.extractRecordCount(line2, expr)
          runner.extractRecordCount(line3, expr)

          assert(runner.recordCount.contains(3000L))
        }
      }
    }

    "extractZeroRecordSuccess" should {
      "extract success from a line" in {
        withDummyProcessRunner() { runner =>
          val line = "The job has succeeded with 0 records written."
          runner.extractZeroRecordSuccess(line, ".*succeeded.*".r)

          assert(runner.recordCount.contains(0))
        }
      }

      "return None if the line does not match the regex" in {
        withDummyProcessRunner() { runner =>
          val line = "The job has failed with 0 records written."
          runner.extractZeroRecordSuccess(line, ".*succeeded.*".r)

          assert(runner.recordCount.isEmpty)
        }
      }

    }

    "extractFailure" should {
      "return the failure state when the line matches the regex expression" in {
        withDummyProcessRunner() { runner =>
          val line = "The job has failed."
          runner.extractFailure(line, ".*failed.*".r)

          assert(runner.isFailureFound)
        }
      }

      "return None if the line does not match the regex" in {
        withDummyProcessRunner() { runner =>
          val line = "The job has succeeded."
          runner.extractFailure(line, ".*failed.*".r)

          assert(!runner.isFailureFound)
        }
      }

    }
  }

  "run()" should {
    val os = System.getProperty("os.name").toLowerCase

    "run a program and return exit status" when {
      val line = "a" * 120
      val script =
        s"""#!/bin/bash
           |for (( c=0; c<1000; c++ ))
           |do
           |   echo "$$c - $line"
           |done
           |for (( c=0; c<1000; c++ ))
           |do
           |   >&2 echo "E $$c - $line"
           |done
           |""".stripMargin

      "stdout and stderr are combined" in {
        if (!os.contains("windows")) {
          withRealScript(script)((runner, cmd) => {
            val exitStatus = runner.run(cmd)
            val stdOutLog = runner.getLastStdoutLines
            val stdErrLog = runner.getLastStderrLines

            assert(exitStatus == 0)
            assert(stdOutLog.length == 10)
            assert(stdErrLog.length == 10)
            assert(stdOutLog.last.contains("999 - "))
            assert(stdErrLog.last.contains("E 999 - "))
          })
        }
      }

      "stdout and stderr are not combined" in {
        if (!os.contains("windows")) {
          withRealScript(script, redirectErrorStream = true)((runner, cmd) => {
            val exitStatus = runner.run(cmd)

            val stdOutLog = runner.getLastStdoutLines
            val stdErrLog = runner.getLastStderrLines

            assert(exitStatus == 0)
            assert(stdOutLog.length == 10)
            assert(stdErrLog.isEmpty)
            assert(stdOutLog.last.contains("E 999 - "))
          })
        }
      }
    }

    if (!os.contains("windows")) {
      "run program with a failure but with 0 exit status" in {
        val script =
          s"""#!/bin/bash
             |echo "The job has failed."
             |""".stripMargin

        withRealScript(script, failureRegEx = Some(".*failed.*"))((runner, cmd) => {
          val exitStatus = runner.run(cmd)

          assert(exitStatus == 1)
        })
      }

      "run program with a success and zero records written" in {
        val script =
          s"""#!/bin/bash
             |echo "The job has succeeded."
             |""".stripMargin

        withRealScript(script, zeroRecordSuccessRegEx = Some(".*succeeded.*"), includeOutputLines = 0)((runner, cmd) => {
          val exitStatus = runner.run(cmd)

          assert(exitStatus == 0)
          assert(runner.recordCount.contains(0))
        })
      }
    }

    "throw an exception is the command is not found" in {
      val runner = ProcessRunner(10)

      val ex = intercept[IOException] {
        runner.run("/tmp/dummy_command12312.tmp")
      }
      assert(ex.getMessage.contains("Cannot run program"))
    }
  }
}
