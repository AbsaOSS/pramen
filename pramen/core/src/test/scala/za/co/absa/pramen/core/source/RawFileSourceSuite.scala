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

package za.co.absa.pramen.core.source

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{Query, Source}
import za.co.absa.pramen.core.ExternalChannelFactoryReflect
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.source.RawFileSource.FILE_PATTERN_CASE_SENSITIVE_KEY
import za.co.absa.pramen.core.utils.{FsUtils, LocalFsUtils}

import java.io.{File, FileNotFoundException}
import java.time.LocalDate

class RawFileSourceSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {
  private val infoDate = LocalDate.of(2022, 2, 18)

  val emptyConfig: Config = ConfigFactory.empty()
  val tempDir: String = createTempDir("raw_file_source")
  val emptyDirPath = new Path(tempDir, "empty")
  val metastorePath = new Path(tempDir, "mt")
  val filesPath = new Path(tempDir, "files")
  val filesPatternPath = new Path(tempDir, "pattern_files")
  val filesPattern = new Path(filesPatternPath, "FILE_TEST_{{yyyy-MM-dd}}*.dat")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

    fsUtils.createDirectoryRecursive(emptyDirPath)
    fsUtils.createDirectoryRecursive(metastorePath)
    fsUtils.createDirectoryRecursive(filesPath)
    fsUtils.createDirectoryRecursive(filesPatternPath)

    fsUtils.writeFile(new Path(filesPath, "1.dat"), "123")
    fsUtils.writeFile(new Path(filesPath, "2.dat"), "4567")
    fsUtils.writeFile(new Path(filesPath, "_3.dat"), "89")
    fsUtils.writeFile(new Path(filesPatternPath, "FILE_TEST_2022-02-18_A.dat"), "123")
    fsUtils.writeFile(new Path(filesPatternPath, "FILE_TEST_2022-02-18_A.txt"), "999")
    fsUtils.writeFile(new Path(filesPatternPath, "FILE_TEST_2022-02-18_B.dat"), "456")
    fsUtils.writeFile(new Path(filesPatternPath, "FILE_TEST_2022-02-18_C.DAT"), "456")
    fsUtils.writeFile(new Path(filesPatternPath, "FILE_TEST_2022-02-19_D.dat"), "789")
  }

  override def afterAll(): Unit = {
    super.afterAll()

    LocalFsUtils.deleteTemp(new File(tempDir))
  }

  private val conf: Config = ConfigFactory.parseString(
    """
      | pramen {
      |   sources = [
      |    {
      |      name = "files"
      |      factory.class = "za.co.absa.pramen.core.source.RawFileSource"
      |
      |      option.test = "test1"
      |    }
      |  ]
      | }
      |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "the factory" should {
    "be able to create a proper Source job object" in {
      val srcConfig = conf.getConfigList("pramen.sources")
      val src1Config = srcConfig.get(0)

      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, conf, "pramen.sources.0", "source")

      assert(src.isInstanceOf[RawFileSource])
      assert(src.asInstanceOf[RawFileSource].options.contains("test"))
    }

    "be able to get a source by its name" in {
      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "files", "source")

      assert(src.isInstanceOf[RawFileSource])
      assert(src.asInstanceOf[RawFileSource].options("test") == "test1")
    }

    "be able to get a source by its name from the manager" in {
      val src = SourceManager.getSourceByName("files", conf, None)

      assert(src.isInstanceOf[RawFileSource])
    }

    "throw an exception if a source is not found" in {
      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "Dummy", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Unknown name of a data source: Dummy"))
    }
  }

  "hasInfoDateColumn" should {
    "return true if the date pattern is present" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assert(source.hasInfoDateColumn(Query.Path(filesPattern.toString)))
    }

    "return false if the file pattern is absent" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assert(!source.hasInfoDateColumn(Query.Path("s3://bucket/prefix/*")))
    }

    "return false on custom path query" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assert(!source.hasInfoDateColumn(Query.Custom(Map("file.1" -> "f1.dat"))))
    }

    "throw an exception on unsupported query type" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assertThrows[IllegalArgumentException] {
        source.hasInfoDateColumn(Query.Table("abc"))
      }
    }
  }

  "getRecordCount" should {
    "return the number of files in the directory for a path" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val count = source.getRecordCount(Query.Path(filesPath.toString), null, null)

      assert(count == 9)
    }

    "return the number of files in the directory for a path pattern" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val count = source.getRecordCount(Query.Path(filesPattern.toString), infoDate, infoDate)

      assert(count == 6)
    }

    "return the number of files in the directory for a path pattern and range query" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val count = source.getRecordCount(Query.Path(filesPattern.toString), infoDate, infoDate.plusDays(1))

      assert(count == 9)
    }

    "return the number of files in the directory for a list of files" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val options = Map(
        "file.1" -> new Path(filesPath, "1.dat").toString,
        "file.2" -> new Path(filesPath, "2.dat").toString
      )

      val count = source.getRecordCount(Query.Custom(options), null, null)

      assert(count == 7)
    }

    "thrown an exception when parent path does not exist" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val innerPath = new Path(filesPattern, "inner")

      assertThrows[FileNotFoundException] {
        source.getRecordCount(Query.Path(innerPath.toString), infoDate, infoDate)
      }
    }
  }

  "getData" should {
    "return the list of files for a directory" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val result = source.getData(Query.Path(filesPath.toString), null, null, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.length == 3)
      assert(filesInDf.exists(_.contains("1.dat")))
      assert(filesInDf.exists(_.contains("2.dat")))
      assert(filesInDf.exists(_.contains("_3.dat")))

      assert(filesRead.length == 3)
      assert(filesRead.contains("1.dat"))
      assert(filesRead.contains("2.dat"))
      assert(filesRead.contains("_3.dat"))
    }

    "return the list of files for a pattern" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val result = source.getData(Query.Path(filesPattern.toString), infoDate, infoDate, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.length == 2)
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_A.dat")))
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_B.dat")))

      assert(filesRead.length == 2)
      assert(filesRead.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(filesRead.contains("FILE_TEST_2022-02-18_B.dat"))
    }

    "return the list of files for a case insensitive pattern" in {
      val conf = ConfigFactory.parseString(
        s"$FILE_PATTERN_CASE_SENSITIVE_KEY = false"
      )
      val source = new RawFileSource(conf, null)(spark)

      val result = source.getData(Query.Path(filesPattern.toString), infoDate, infoDate, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.length == 3)
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_A.dat")))
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_B.dat")))
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_C.DAT")))

      assert(filesRead.length == 3)
      assert(filesRead.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(filesRead.contains("FILE_TEST_2022-02-18_B.dat"))
      assert(filesRead.contains("FILE_TEST_2022-02-18_C.DAT"))
    }

    "return the list of files for a pattern with range query" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val result = source.getData(Query.Path(filesPattern.toString), infoDate, infoDate.plusDays(1), null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.length == 3)
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_A.dat")))
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-18_B.dat")))
      assert(filesInDf.exists(_.contains("FILE_TEST_2022-02-19_D.dat")))

      assert(filesRead.length == 3)
      assert(filesRead.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(filesRead.contains("FILE_TEST_2022-02-18_B.dat"))
      assert(filesRead.contains("FILE_TEST_2022-02-19_D.dat"))
    }

    "return the list of files for a list of files" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val options = Map(
        "file.1" -> new Path(filesPath, "1.dat").toString,
        "file.2" -> new Path(filesPath, "2.dat").toString
      )

      val result = source.getData(Query.Custom(options), null, null, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.exists(_.contains("1.dat")))
      assert(filesInDf.exists(_.contains("2.dat")))

      assert(filesRead.exists(_.contains("1.dat")))
      assert(filesRead.exists(_.contains("2.dat")))
    }
  }

  "getPaths" should {
    "work for a directory" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val files = source.getPaths(Query.Path(filesPath.toString), infoDate, infoDate)
        .map(_.getPath.getName)

      assert(files.length == 3)

      assert(files.contains("1.dat"))
      assert(files.contains("2.dat"))
      assert(files.contains("_3.dat"))
    }

    "work for a list of files" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val options = Map(
        "file.1" -> new Path(filesPath, "1.dat").toString,
        "file.2" -> new Path(filesPath, "_3.dat").toString
      )

      val files = source.getPaths(Query.Custom(options), infoDate, infoDate)
        .map(_.getPath.getName)

      assert(files.length == 2)

      assert(files.contains("1.dat"))
      assert(files.contains("_3.dat"))
    }

    "throw an exception when the query is SQL or a table" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assertThrows[IllegalArgumentException] {
        source.getPaths(Query.Table("abc"), infoDate, infoDate)
      }
    }
  }

  "getPatternBasedFilesForRange" should {
    "return the list of files for a date range" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      val files = source.getPatternBasedFilesForRange(filesPatternPath.toString, infoDate, infoDate.plusDays(1))
        .map(_.getPath.getName)

      assert(files.length == 5)

      assert(files.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(files.contains("FILE_TEST_2022-02-18_B.dat"))
      assert(files.contains("FILE_TEST_2022-02-19_D.dat"))
    }

    "throw an exception if the date range is incorrect" in {
      val source = new RawFileSource(emptyConfig, null)(spark)

      assertThrows[IllegalArgumentException] {
        source.getPatternBasedFilesForRange(filesPattern.toString, infoDate.plusDays(1), infoDate)
      }
    }
  }

  "getListOfFiles" should {
    val specificPattern = new Path(filesPatternPath, "FILE_TEST_2022-02-18*.dat")
    val nonExistingPath = new Path(filesPatternPath, "inner")
    val nonPattern = new Path(nonExistingPath, "FILE_TEST_2022-02-18*.dat")

    "return the list of files for the pattern" in {
      val files = RawFileSource.getListOfFiles(specificPattern.toString, caseSensitive = true)
        .map(_.getPath.getName)

      assert(files.length == 2)

      assert(files.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(files.contains("FILE_TEST_2022-02-18_B.dat"))
    }

    "return the list of files for the case insensitive pattern" in {
      val caseInsensitivePattern = new Path(filesPatternPath, "FILE_test_2022-02-18*.Dat")
      val files = RawFileSource.getListOfFiles(caseInsensitivePattern.toString, caseSensitive = false)
        .map(_.getPath.getName)

      assert(files.length == 3)

      assert(files.contains("FILE_TEST_2022-02-18_A.dat"))
      assert(files.contains("FILE_TEST_2022-02-18_B.dat"))
      assert(files.contains("FILE_TEST_2022-02-18_C.DAT"))
    }

    "return an empty list on empty directory" in {
      val emptyPattern = new Path(emptyDirPath, "empty")
      val files = RawFileSource.getListOfFiles(emptyPattern.toString, caseSensitive = true)

      assert(files.isEmpty)
    }

    "throw an exception if parent path does not exist" in {
      assertThrows[FileNotFoundException] {
        RawFileSource.getListOfFiles(nonPattern.toString, caseSensitive = true)
      }
    }
  }
}
