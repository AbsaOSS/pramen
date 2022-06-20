/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.tests.builtin.process

import java.io.{BufferedReader, IOException, StringReader}

import org.scalatest.WordSpec
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.builtin.process.ProcessRunner
import za.co.absa.pramen.fixtures.ScriptProcessRunnerFixture

class ProcessRunnerSuite extends WordSpec with SparkTestBase with ScriptProcessRunnerFixture {
  "getFirstGroupTokenMatchLong()" should {
    "parse the output record count" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      withDummyProcessRunner(recordCountRegEx, None, None, Nil) (runner => {
        val recordCount1Opt = runner.getFirstGroupTokenMatchLong(recordCountRegEx.r, "None:100")
        val recordCount2Opt = runner.getFirstGroupTokenMatchLong(recordCountRegEx.r, "Records:100")

        assert(recordCount1Opt.isEmpty)
        assert(recordCount2Opt.nonEmpty)
        assert(recordCount2Opt.contains(100L))
      })
    }

    "throw an exception if there is a match, but no match groups" in {
      val recordCountRegEx = "Records\\:\\d*"
      withDummyProcessRunner(recordCountRegEx, None, None, Nil) (runner => {
        val ex = intercept[IllegalArgumentException] {
          runner.getFirstGroupTokenMatchLong(recordCountRegEx.r, "Records:100")
        }
        assert(ex.getMessage.contains("An output string matches the pattern"))
      })
    }

    "throw an exception if the number cannot be parsed" in {
      val recordCountRegEx = "Records\\:(.*)"
      withDummyProcessRunner(recordCountRegEx, None, None, Nil) (runner => {
        val ex = intercept[IllegalArgumentException] {
          runner.getFirstGroupTokenMatchLong(recordCountRegEx.r, "Records:10A")
        }
        assert(ex.getMessage.contains("Unable to parse '10A' as long."))
      })
    }
  }

  "getRecordCountFromOutput()" should {
    "parse the output record count from the output" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      withDummyProcessRunner(recordCountRegEx, None, None, Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Records:100
            |Last Line
            |""".stripMargin))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.nonEmpty)
        assert(recordCountOpt.contains(100L))
      })
    }

    "parse zero record success" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      val zeroRecordsSuccess = "No\\sRecords"
      withDummyProcessRunner(recordCountRegEx, Some(zeroRecordsSuccess), None, Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |No Records
            |Last Line
            |""".stripMargin))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.nonEmpty)
        assert(recordCountOpt.contains(0L))
      })
    }

    "parse actual records when it comes after zero record success" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      val zeroRecordsSuccess = "No\\sRecords"
      withDummyProcessRunner(recordCountRegEx, Some(zeroRecordsSuccess), None, Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |No Records
            |Records:100
            |Last Line
            |""".stripMargin))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.nonEmpty)
        assert(recordCountOpt.contains(100L))
      })
    }

    "parse failure" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      val failureRegEx = "^Fail"
      withDummyProcessRunner(recordCountRegEx, None, Some(failureRegEx), Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Fail to run
            |Last Line
            |""".stripMargin))
        runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(runner.isFailedNoData)
        assert(runner.getFailedLine == "Fail to run")
      })
    }

    "parse success after a failure" in {
      val recordCountRegEx = "Records\\:(\\d*)"
      val failureRegEx = "^Fail"
      withDummyProcessRunner(recordCountRegEx, None, Some(failureRegEx), Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Fail to execute
            |Records:100
            |Last Line
            |""".stripMargin))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.nonEmpty)
        assert(recordCountOpt.contains(100L))
        assert(!runner.isFailedNoData)
      })
    }

    "don't throw an exception on record count parsing errors" in {
      val recordCountRegEx = "Records\\:(.*)"
      withDummyProcessRunner(recordCountRegEx, None, None, Nil) (runner => {
        val reader = new BufferedReader(new StringReader(
          """First line
            |Second line
            |Records:10A
            |Last Line
            |""".stripMargin))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.isEmpty)
      })
    }

    "handle long outputs" in {
      def generateLongString(): String = {
        val line = "a"*120
        val lines = new StringBuilder
        for (i <- Range(0, 100000)) {
          lines.append(s"$i $line\n")
        }
        lines.toString()
      }

      val recordCountRegEx = "Records\\:(.*)"
      withDummyProcessRunner(recordCountRegEx, None, None, "aaaaaaa" :: Nil) (runner => {
        val str1 = generateLongString()

        val reader = new BufferedReader(new StringReader(
          str1 +
          """Records:50
            |Some line
            |Records:100
            |""".stripMargin + str1))

        val recordCountOpt = runner.getRecordCountFromOutput(reader, recordCountRegEx)

        assert(recordCountOpt.nonEmpty)
        assert(recordCountOpt.contains(100L))
      })
    }
  }

  "run()" should {
    "run a program and return exit status" in {
      val os = System.getProperty("os.name").toLowerCase
      if (!os.contains("windows")) {
        val recordCountRegEx = "Records\\:(.*)"
        val line = "a"*120
        val script =
          s"""#!/bin/bash
             |for (( c=0; c<100000; c++ ))
             |do
             |   echo "$$c - $line"
             |done
             |echo "Records:100"
             |for (( c=0; c<100000; c++ ))
             |do
             |   echo "$$c - $line"
             |done
             |""".stripMargin

        withRealScript(script, recordCountRegEx, None, None, "aaaaaaa" :: Nil) (runner => {
          val exitStatus = runner.run()

          val recordCountOpt = runner.getRecordCount
          assert(exitStatus == 0)
          assert(recordCountOpt.isDefined)
          assert(recordCountOpt.contains(100L))
        })
      }
    }

    "throw an exception is the command is not found" in {
      val runner = new ProcessRunner("/tmp/dummy_command12312.tmp", "", None, None, Nil, 200)

      val ex = intercept[IOException] {
        runner.run()
      }
      assert(ex.getMessage.contains("Cannot run program"))
    }
  }

}
