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

package za.co.absa.pramen.extras.tests.sink

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.extras.mocks.QueryExecutorSpy
import za.co.absa.pramen.extras.sink.EnceladusUtils
import za.co.absa.pramen.extras.utils.FsUtils

import java.nio.file.{Files, Paths}
import java.time.LocalDate

class EnceladusUtilsSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {
  import za.co.absa.pramen.extras.sink.InfoVersionStatus._
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)
  private val rawPathPattern = "{year}/{month}/{day}/v{version}"
  private val publishPathPattern = "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}"

  "getNextEnceladusVersion" should {
    "return initial versions if only the base folders exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_raw") { tempRaw =>
        withTempDirectory("enceladus_publish") { tempPublish =>
          Files.createDirectories(Paths.get(tempRaw, "my_table"))
          Files.createDirectories(Paths.get(tempPublish, "my_table"))

          val nextVersionTry = utils.getNextEnceladusVersion(infoDate,
            new Path(tempRaw, "my_table"),
            Some(new Path(tempPublish, "my_table")),
            None)

          assert(nextVersionTry.get == 1)
        }
      }
    }

    "return initial versions if only raw folders have versions" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_raw") { tempRaw =>
        withTempDirectory("enceladus_publish") { tempPublish =>
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v1"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v2"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v3"))

          Files.createDirectories(Paths.get(tempPublish, "my_table"))

          val nextVersionTry = utils.getNextEnceladusVersion(infoDate,
            new Path(tempRaw, "my_table"),
            Some(new Path(tempPublish, "my_table")),
            None)

          assert(nextVersionTry.get == 1)
        }
      }
    }

    "return next version if published versions exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_raw") { tempRaw =>
        withTempDirectory("enceladus_publish") { tempPublish =>
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v1"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v3"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v4"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v5"))

          Files.createDirectories(Paths.get(tempPublish, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=1"))
          Files.createDirectories(Paths.get(tempPublish, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=2"))
          Files.createDirectories(Paths.get(tempPublish, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=3"))

          val nextVersionTry = utils.getNextEnceladusVersion(infoDate,
            new Path(tempRaw, "my_table"),
            Some(new Path(tempPublish, "my_table")),
            None)

          assert(nextVersionTry.get == 4)
        }
      }
    }

    "return next version if hive versions exist" in {
      val partitiondDf = Seq(
        "enceladus_info_date=2022-02-18/enceladus_info_version=1",
        "enceladus_info_date=2022-02-18/enceladus_info_version=3",
        "enceladus_info_date=2022-02-18/enceladus_info_version=5"
      ).toDF("partition")

      val (utils, _) = getUseCase(Option(partitiondDf))

      withTempDirectory("enceladus_raw") { tempRaw =>
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v1"))
          Files.createDirectories(Paths.get(tempRaw, "my_table", "2022", "02", "18", "v3"))

          val nextVersionTry = utils.getNextEnceladusVersion(infoDate,
            new Path(tempRaw, "my_table"),
            None,
            Some("dummy_hive_table"))

          assert(nextVersionTry.get == 6)
        }
    }

    "fail if neither publish folder nor hive table is defined" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        Files.createDirectories(Paths.get(tempDir, "my_table"))

        val path = new Path(tempDir, "my_table")
        val nextVersionTry = utils.getNextEnceladusVersion(infoDate, path, None, None)

        assert(nextVersionTry.isFailure)
        assert(nextVersionTry.failed.get.getMessage.contains("No publish path or hive table specified"))
      }
    }

    "fail if the raw publish folder does not exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_raw") { tempRaw =>
        withTempDirectory("enceladus_publish") { tempPublish =>
          Files.createDirectories(Paths.get(tempRaw, "my_table"))

          val nextVersionTry = utils.getNextEnceladusVersion(infoDate,
            new Path(tempRaw, "my_table"),
            Some(new Path(tempPublish, "my_table")),
            None)

          assert(nextVersionTry.isFailure)
          assert(nextVersionTry.failed.get.getMessage.contains("Publish path does not exist"))
        }
      }
    }
  }

  "getMaxVersionInRaw" should {
    "return the maximum version in the publish folder" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v1"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v2"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v3"))

        val maxVersionTry = utils.getMaxVersionInRaw(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry.isInstanceOf[Detected])
        assert(maxVersionTry.asInstanceOf[Detected].maxVersion == 3)
      }
    }

    "return None for empty publish folder" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        Files.createDirectories(Paths.get(tempDir, "my_table"))

        val maxVersionTry = utils.getMaxVersionInRaw(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry == NotPresent)
      }
    }

    "fail if the base folder does not exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        val maxVersionTry = utils.getMaxVersionInRaw(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry.isInstanceOf[DetectionFailure])
      }
    }
  }

  "getMaxVersionInPublish" should {
    "return the maximum version in the publish folder" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        Files.createDirectories(Paths.get(tempDir, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=1"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=2"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "enceladus_info_date=2022-02-18", "enceladus_info_version=3"))

        val maxVersionTry = utils.getMaxVersionInPublish(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry.isInstanceOf[Detected])
        assert(maxVersionTry.asInstanceOf[Detected].maxVersion == 3)
      }
    }

    "return None for empty publish folder" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        Files.createDirectories(Paths.get(tempDir, "my_table"))

        val maxVersionTry = utils.getMaxVersionInPublish(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry == NotPresent)
      }
    }

    "fail if the base folder does not exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        val maxVersionTry = utils.getMaxVersionInPublish(new Path(tempDir, "my_table"), "enceladus_info_date", infoDate)

        assert(maxVersionTry.isInstanceOf[DetectionFailure])
      }
    }
  }

  "getMaxVersionFromHive" should {
    "have partitions" when {
      val partitiondDf = Seq(
        "enceladus_info_date=2022-02-18/enceladus_info_version=1",
        "enceladus_info_date=2022-02-18/enceladus_info_version=3",
        "enceladus_info_date=2022-02-18/enceladus_info_version=5"
      ).toDF("partition")

      val (utils, qe) = getUseCase(Option(partitiondDf))

      val maxVersionTry = utils.getMaxVersionFromHive("mydb.mytable1", "enceladus_info_date", infoDate)

      "execute the proper query" in {
        assert(qe.queried.head == "SHOW PARTITIONS mydb.mytable1")
      }

      "return the proper result" in {
        assert(maxVersionTry.isInstanceOf[Detected])
        assert(maxVersionTry.asInstanceOf[Detected].maxVersion == 5)
      }
    }

    "empty partitions" when {
      val partitiondDf = Seq.empty[String].toDF("partition")

      val (utils, qe) = getUseCase(Option(partitiondDf))

      val maxVersionTry = utils.getMaxVersionFromHive("mydb.mytable1", "enceladus_info_date", infoDate)

      "execute the proper query" in {
        assert(qe.queried.head == "SHOW PARTITIONS mydb.mytable1")
      }

      "return the proper result" in {
        assert(maxVersionTry == NotPresent)
      }
    }

    "rethrow hive exception" when {
      val partitiondDf = Seq.empty[String].toDF("partition")

      val (utils, qe) = getUseCase(Option(partitiondDf), Option(new IllegalStateException("Dummy")))

      val maxVersionTry = utils.getMaxVersionFromHive("mydb.mytable2", "enceladus_info_date", infoDate)

      "execute the proper query" in {
        assert(qe.queried.head == "SHOW PARTITIONS mydb.mytable2")
      }

      "return the proper result" in {
        assert(maxVersionTry.isInstanceOf[DetectionFailure])
        assert(maxVersionTry.asInstanceOf[DetectionFailure].ex.getMessage == "Dummy")
      }
    }
  }

  "getMaxVersionFromDirs" should {
    "find the maximum version in a dir" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v1"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v8"))
        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18", "v2"))

        val partitionParent = utils.getParentPartitionPath(new Path(tempDir, "my_table"), rawPathPattern, "enceladus_info_date", infoDate)

        val actual = utils.getMaxVersionFromDirs(partitionParent, utils.rawVersionRegEx, fsUtils)

        assert(actual.isInstanceOf[Detected])
        assert(actual.asInstanceOf[Detected].maxVersion == 8)
      }
    }

    "return None for an empty dir" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        Files.createDirectories(Paths.get(tempDir, "my_table", "2022", "02", "18"))

        val partitionParent = utils.getParentPartitionPath(new Path(tempDir, "my_table"), rawPathPattern, "enceladus_info_date", infoDate)

        val actual = utils.getMaxVersionFromDirs(partitionParent, utils.rawVersionRegEx, fsUtils)

        assert(actual == NotPresent)
      }
    }

    "return Failure if the folder does not exist" in {
      val (utils, _) = getUseCase()

      withTempDirectory("enceladus_utils") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val partitionParent = utils.getParentPartitionPath(new Path(tempDir, "my_table"), rawPathPattern, "enceladus_info_date", infoDate)

        val actual = utils.getMaxVersionFromDirs(partitionParent, utils.rawVersionRegEx, fsUtils)

        assert(actual.isInstanceOf[DetectionFailure])
      }
    }
  }

  "getMaxVersionFromList" should {
    "return maximum version in a raw folder" in {
      val (utils, _) = getUseCase()

      val partitions = Seq(
        "/bigdata/raw/my_table/2022/02/18/v1",
        "/bigdata/raw/my_table/2022/02/18/v2",
        "/bigdata/raw/my_table/2022/02/18/v10"
      )

      val actual = utils.getMaxVersionFromList(partitions, utils.rawVersionRegEx)

      assert(actual.isInstanceOf[Detected])
      assert(actual.asInstanceOf[Detected].maxVersion == 10)
    }

    "return maximum version in a raw inner folder" in {
      val (utils, _) = getUseCase()

      val partitions = Seq(
        "v1",
        "v2",
        "v10"
      )

      val actual = utils.getMaxVersionFromList(partitions, utils.rawVersionRegEx)

      assert(actual.isInstanceOf[Detected])
      assert(actual.asInstanceOf[Detected].maxVersion == 10)
    }


    "return maximum version in a publish folder" in {
      val (utils, _) = getUseCase()

      val partitions = Seq(
        "/bigdata/publish/my_table/enceladus_info_date=2022-02-18/enceladus_info_version=1",
        "/bigdata/publish/my_table/enceladus_info_date=2022-02-18/enceladus_info_version=2",
        "/bigdata/publish/my_table/enceladus_info_date=2022-02-18/enceladus_info_version=11"
      )

      val actual = utils.getMaxVersionFromList(partitions, utils.publishVersionRegEx)

      assert(actual.isInstanceOf[Detected])
      assert(actual.asInstanceOf[Detected].maxVersion == 11)
    }

    "return maximum version in a Hive table" in {
      val (utils, _) = getUseCase()

      val partitions = Seq(
        "enceladus_info_date=2022-02-18/enceladus_info_version=1",
        "enceladus_info_date=2022-02-18/enceladus_info_version=2",
        "enceladus_info_date=2022-02-18/enceladus_info_version=11"
      )

      val actual = utils.getMaxVersionFromList(partitions, utils.publishVersionRegEx)

      assert(actual.isInstanceOf[Detected])
      assert(actual.asInstanceOf[Detected].maxVersion == 11)
    }

    "return None for the empty folder" in {
      val (utils, _) = getUseCase()

      val partitions = Seq.empty[String]

      val actual = utils.getMaxVersionFromList(partitions, utils.publishVersionRegEx)

      assert(actual == NotPresent)
    }
  }

  "getParentPartitionPath" should {
    "return raw the parent path" in {
      val (utils, _) = getUseCase()

      val action = utils.getParentPartitionPath(new Path("/bigdata/raw/my_table"), rawPathPattern, "enceladus_info_date", infoDate)

      assert(action.toString == "/bigdata/raw/my_table/2022/02/18")
    }

    "return publish the parent path" in {
      val (utils, _) = getUseCase()

      val action = utils.getParentPartitionPath(new Path("/bigdata/publish/my_table"), publishPathPattern, "enceladus_info_date", infoDate)

      assert(action.toString == "/bigdata/publish/my_table/enceladus_info_date=2022-02-18")
    }
  }

  def getUseCase(partitionDf: Option[DataFrame] = None,
                 throwException: Option[Throwable] = None): (EnceladusUtils, QueryExecutorSpy) = {
    implicit val qe: QueryExecutorSpy = new QueryExecutorSpy(dfToReturn = partitionDf, throwException = throwException)
    val utils = new EnceladusUtils("{year}/{month}/{day}/v{version}",
      "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}",
      "enceladus_info_date"
     )
    (utils, qe)
  }
}
