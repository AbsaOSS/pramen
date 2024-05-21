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

package za.co.absa.pramen.core.sink

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.utils.FsUtils

import java.time.LocalDate

class SparkSinkSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def inputDf: DataFrame = List(
    ("A", 1, "2022-02-18"),
    ("B", 2, "2022-02-18"),
    ("C", 3, "2022-02-18")
  ).toDF("a", "b", "info_date")

  "apply()" should {
    "construct a sink from config" in {
      val conf = ConfigFactory.parseString(
        """  format = "csv"
          |  mode = "overwrite"
          |  records.per.partition = 1
          |  partition.by = [ info_date ]
          |  save.empty = false
          |
          |  options {
          |    compression = gzip
          |  }
          |""".stripMargin
      )
      val sink = SparkSink(conf, "parent", spark)

      assert(sink.isInstanceOf[SparkSink])
    }
  }

  def testWithDataFrameSentToSink(dataFrame: DataFrame, sink: SparkSink)(test: (FsUtils, Path) => Unit): Unit = {
    withTempDirectory("spark_sink_write_test") { tempDir =>
      val workingDirectory = new Path(tempDir)
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, workingDirectory.toUri.toString)

      sink.send(
        dataFrame,
        "dummy_table",
        null,
        infoDate,
        Map("path" -> workingDirectory.toUri.toString)
      )

      test(fsUtils, workingDirectory)
    }
  }

  "connect()" should {
    "do nothing" in {
      val sink = getUseCase()

      sink.connect()
    }
  }

  "close()" should {
    "do nothing" in {
      val sink = getUseCase()

      sink.close()
    }
  }

  "send()" should {
    "write data to the target directory with specified partition structure" in {
      val sink = getUseCase(partitionBy = Seq("info_date", "b"))

      testWithDataFrameSentToSink(inputDf, sink) { (fsUtils, outputDir) =>
        val partitionDirectory = new Path(outputDir, "info_date=2022-02-18/b=2")

        assert(fsUtils.exists(partitionDirectory))
        assert(fsUtils.getFilesRecursive(partitionDirectory, "*.parquet").nonEmpty)
      }
    }

    "write data based on specified format and format options" in {
      val sink = getUseCase(
        format = "csv",
        formatOptions = Map(
          "compression" -> "gzip"
        )
      )

      testWithDataFrameSentToSink(inputDf, sink) { (fsUtils, outputDir) =>
        assert(fsUtils.getFilesRecursive(outputDir, "*.csv.gz").nonEmpty)
      }
    }

    "write data based on specified mode" in {
      withTempDirectory("spark_sink") { tempDir =>
        val dfToAppend = Seq(
          (1, 2)
        ).toDF("a", "b")
        val sinkWithAppendMode = getUseCase(mode = "append")
        val outputDir = new Path(tempDir)
        val options = Map("path" -> outputDir.toUri.toString)

        sinkWithAppendMode.send(dfToAppend, "dummy_table", null, infoDate, options)
        sinkWithAppendMode.send(dfToAppend, "dummy_table", null, infoDate, options)

        val writtenDF = spark.read.parquet(outputDir.toUri.toString)
        assert(writtenDF.count() == 2)
      }
    }

    "partition the data based on specified records per partition" in {
      val sink = getUseCase(recordsPerPartition = Some(1L))

      testWithDataFrameSentToSink(inputDf, sink) { (fsUtils, outputDir) =>
        val partitions = fsUtils.getFilesRecursive(outputDir, "*.parquet")
        assert(partitions.size == 3)
      }
    }

    "partition the data based on specified number of partitions" in {
      val sink = getUseCase(numberOfPartitions = Some(2))

      testWithDataFrameSentToSink(inputDf, sink) { (fsUtils, outputDir) =>
        val partitions = fsUtils.getFilesRecursive(outputDir, "*.parquet")
        assert(partitions.size == 2)
      }
    }

    "save dataframe even if it is empty" in {
      val emptyDataFrame = spark.emptyDataset[String].toDF("some_col")
      val sink = getUseCase(saveEmpty = true)

      testWithDataFrameSentToSink(emptyDataFrame, sink) { (fsUtils, outputDir) =>
        val writtenDf = spark.read.parquet(outputDir.toUri.toString)
        assert(writtenDf.count() == 0)
        assert(fsUtils.getFilesRecursive(outputDir, "*.parquet").nonEmpty)
      }
    }

    "if specified, not save an empty dataframe" in {
      val emptyDataFrame = spark.emptyDataset[String].toDF("some_col")
      val sink = getUseCase(saveEmpty = false)

      testWithDataFrameSentToSink(emptyDataFrame, sink) { (fsUtils, outputDir) =>
        assert(fsUtils.getFilesRecursive(outputDir, "*.parquet").isEmpty)
      }
    }

    "throw an exception when of both records per partition and number of partitions are specified" in {
      val sink = getUseCase(
        numberOfPartitions = Some(2),
        recordsPerPartition = Some(4L)
      )

      val ex = intercept[IllegalArgumentException] {
        sink.send(inputDf, "dummy_table", null, infoDate, Map("path" -> "/does/not/exist"))
      }

      assert(ex.getMessage.contains("Both number.of.partitions and records.per.partition are specified for Spark sink"))
      assert(ex.getMessage.contains("Please specify only one of those options"))
    }

    "throw an exception if output path is not specified" in {
      val sink = getUseCase()

      val ex = intercept[IllegalArgumentException] {
        sink.send(inputDf, "dummy_table", null, infoDate, Map.empty)
      }

      assert(ex.getMessage.contains("path is not specified for Spark sink, table: dummy_table"))
    }
  }

  def getUseCase(format: String = "parquet",
                 formatOptions: Map[String, String] = Map.empty,
                 mode: String = "overwrite",
                 partitionBy: Seq[String] = Seq.empty,
                 numberOfPartitions: Option[Int] = None,
                 recordsPerPartition: Option[Long] = None,
                 saveEmpty: Boolean = true
                 ): SparkSink = {
    val conf = ConfigFactory.empty()
    new SparkSink(
      format,
      formatOptions,
      mode,
      partitionBy,
      numberOfPartitions,
      recordsPerPartition,
      saveEmpty,
      1,
      conf
    )
  }

}
