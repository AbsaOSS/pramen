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
import za.co.absa.pramen.core.utils.{FsUtils, LocalFsUtils}

import java.io.File

class RawFileSourceSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {

  val tempDir: String = createTempDir("raw_file_source")
  val metastorePath = new Path(tempDir, "mt")
  val filesPath = new Path(tempDir, "files")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

    fsUtils.createDirectoryRecursive(metastorePath)
    fsUtils.createDirectoryRecursive(filesPath)

    fsUtils.writeFile(new Path(filesPath, "1.dat"), "123")
    fsUtils.writeFile(new Path(filesPath, "2.dat"), "4567")
    fsUtils.writeFile(new Path(filesPath, "_3.dat"), "89")
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

      val src = ExternalChannelFactoryReflect.fromConfig[Source](src1Config, "pramen.sources.0", "source")

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

  "getRecordCount" should {
    "return the number of files in the directory for a path" in {
      val source = new RawFileSource(null, null)(spark)

      val count = source.getRecordCount(Query.Path(filesPath.toString), null, null)

      assert(count == 3)
    }

    "return the number of files in the directory for a list of files" in {
      val source = new RawFileSource(null, null)(spark)

      val options = Map("file.1" -> "f1.dat", "file.2" -> "f2.dat")

      val count = source.getRecordCount(Query.Custom(options), null, null)

      assert(count == 2)
    }
  }

  "getData" should {
    "return the list of files for a directory" in {
      val source = new RawFileSource(null, null)(spark)

      val result = source.getData(Query.Path(filesPath.toString), null, null, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.exists(_.contains("1.dat")))
      assert(filesInDf.exists(_.contains("2.dat")))
      assert(filesInDf.exists(_.contains("_3.dat")))

      assert(filesRead.exists(_.contains("1.dat")))
      assert(filesRead.exists(_.contains("2.dat")))
      assert(filesRead.exists(_.contains("_3.dat")))
    }

    "return the list of files for a list of files" in {
      val source = new RawFileSource(null, null)(spark)

      val options = Map("file.1" -> "f1.dat", "file.2" -> "f2.dat")

      val result = source.getData(Query.Custom(options), null, null, null)

      val filesInDf = result.data.collect().map(_.toString())
      val filesRead = result.filesRead

      assert(filesInDf.exists(_.contains("f1.dat")))
      assert(filesInDf.exists(_.contains("f2.dat")))

      assert(filesRead.exists(_.contains("f1.dat")))
      assert(filesRead.exists(_.contains("f2.dat")))
    }
  }
}
