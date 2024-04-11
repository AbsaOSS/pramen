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

package za.co.absa.pramen.core.mocks.sink

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{DataFormat, MetastoreReader}
import za.co.absa.pramen.core.MetaTableDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.exceptions.CmdFailedException
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.mocks.process.ProcessRunnerSpy
import za.co.absa.pramen.core.process.ProcessRunnerImpl
import za.co.absa.pramen.core.sink.CmdLineSink
import za.co.absa.pramen.core.sink.CmdLineSink.CmdLineDataParams

import java.time.LocalDate

class CmdLineSinkSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2021, 12, 28)
  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "apply()" should {
    "construct a sink from config" in {
      val configStr =
        """{
          |  name = "cmd_line"
          |  factory.class = "za.co.absa.pramen.core.sink.CmdLineSink"
          |  temp.hadoop.path = "/tmp/cmd_line_sink"
          |  format = "csv"
          |  include.log.lines = 1000
          |  option {
          |    sep = "|"
          |    quoteAll = "false"
          |    header = "true"
          |  }
          |  record.count.regex = "a(.)"
          |  zero.records.success.regex = "b"
          |  failure.regex = "c"
          |  output.filter.regex = [ "d" ]
          |}""".stripMargin
      val conf = ConfigFactory.parseString(configStr)

      val sink = CmdLineSink(conf, "parent", spark)

      assert(sink.isInstanceOf[CmdLineSink])
      assert(sink.dataParams.exists(_.format == "csv"))
      assert(sink.dataParams.exists(_.tempHadoopPath == "/tmp/cmd_line_sink"))
      assert(sink.dataParams.exists(_.formatOptions("sep") == "|"))

      assert(sink.processRunner.isInstanceOf[ProcessRunnerImpl])
      val runner = sink.processRunner.asInstanceOf[ProcessRunnerImpl]
      assert(runner.includeOutputLines == 1000)
      assert(runner.recordCountRegEx.contains("a(.)"))
      assert(runner.zeroRecordsSuccessRegEx.contains("b"))
      assert(runner.failureRegEx.contains("c"))
      assert(runner.outputFilterRegEx == Seq("d"))
    }
  }

  "send()" should {
    "run the command in the temporary folder" in {
      withTempDirectory("cmd_sink") { tempDir =>
        var count = 0L

        val (sink, _) = getUseCase(tempDir, runFunction = () => {
          val df = spark.read.parquet(new Path(tempDir, "*").toString)

          count = df.count()
        })

        sink.send(exampleDf, "table1", null, infoDate, Map[String, String]("cmd.line" -> "dummy @infoDate"))

        assert(count == 3)
      }
    }

    "work without a temporary path" in {
      withTempDirectory("cmd_sink") { tempDir =>
        val metastoreReader = mock(classOf[MetastoreReader])
        val metatable = MetaTableDefFactory.getDummyMetaTableDef(name = "table1",
          format = DataFormat.Parquet(tempDir, None)
        )
        when(metastoreReader.getTableDef("table1")).thenReturn(metatable)

        val (sink, _) = getUseCase(null, recordCountToReturn = Some(5))

        val sinkResult = sink.send(exampleDf, "table1", metastoreReader, infoDate, Map[String, String]("cmd.line" -> "dummy @infoDate @partitionPath"))

        assert(sinkResult.recordsSent == 5)
      }
    }
  }

  "getCmdLine()" should {
    "replace variables with actual values" in {
      val (sink, _) = getUseCase()
      val dataPath = Some(new Path("/dummy/path"))
      val partitionPath = Some(new Path(s"/dummy/path/date=$infoDate"))

      val cmdTemplate = "--data-path @dataPath --data-uri @dataUri --partition-path @partitionPath --info-date @infoDate --infoMonth @infoMonth"

      assert(sink.getCmdLine(cmdTemplate, dataPath, partitionPath, infoDate) ==
        "--data-path /dummy/path --data-uri /dummy/path --partition-path /dummy/path/date=2021-12-28 --info-date 2021-12-28 --infoMonth 2021-12")
    }

    "replace s3 variables with actual values" in {
      val (sink, _) = getUseCase()
      val dataPath = Some(new Path("s3a://my_bucket1/dummy/path"))
      val partitionPath = Some(new Path("s3a://my_bucket2/dummy/path/enceladus_info_date=2023-12-30"))

      val cmdTemplate = "--bucket @bucket --prefix @prefix --partition-prefix @partitionPrefix"

      assert(sink.getCmdLine(cmdTemplate, dataPath, partitionPath, infoDate) ==
        "--bucket my_bucket1 --prefix dummy/path --partition-prefix dummy/path/enceladus_info_date=2023-12-30")
    }
  }

  "runCmd()" should {
    "handle normal run" in {
      val (sink, runner) = getUseCase()

      sink.runCmd("/dummy/cmd")

      assert(runner.runCommands.length == 1)
      assert(runner.runCommands.head == "/dummy/cmd")
    }

    "handle non-zero exit code" in {
      val (sink, _) = getUseCase(exitCode = 123)

      val ex = intercept[CmdFailedException] {
        sink.runCmd("/dummy/cmd")
      }

      assert(ex.getMessage == "The process has exited with error code 123.")
    }

    "handle an exception thrown by the runner" in {
      val (sink, _) = getUseCase(runException = new RuntimeException("dummy"))

      val ex = intercept[RuntimeException] {
        sink.runCmd("/dummy/cmd")
      }

      assert(ex.getMessage == "The process has exited with an exception.")
      assert(ex.getCause.getMessage == "dummy")
    }
  }

  def getUseCase(tempDir: String = null,
                 exitCode: Int = 0,
                 runException: Throwable = null,
                 format: String = "parquet",
                 options: Map[String, String] = Map.empty[String, String],
                 runFunction: () => Unit = () => {},
                 recordCountToReturn: Option[Long] = None): (CmdLineSink, ProcessRunnerSpy) = {
    val runner = new ProcessRunnerSpy(exitCode = exitCode, runException = runException, runFunction = runFunction, recordCountToReturn = recordCountToReturn)

    val dataParams = if (tempDir != null) {
      Some(CmdLineDataParams(tempDir, format, options))
    } else {
      None
    }

    (new CmdLineSink(ConfigFactory.empty(), runner, dataParams), runner)
  }

}
