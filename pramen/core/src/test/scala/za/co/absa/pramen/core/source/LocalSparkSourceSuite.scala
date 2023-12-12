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
import za.co.absa.pramen.core.utils.LocalFsUtils

import java.io.File
import java.nio.file.Paths
import java.time.LocalDate

class LocalSparkSourceSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {
  import spark.implicits._

  val tempDir: String = createTempDir("local_spark_source")
  val sourceTemp = new Path(tempDir, "temp")
  val filesPath = new Path(tempDir, "files")

  override def beforeAll(): Unit = {
    super.beforeAll()

    Range(0, 10).toDF("id")
      .write
      .option("sep", "|")
      .csv(filesPath.toString)
  }

  override def afterAll(): Unit = {
    super.afterAll()

    LocalFsUtils.deleteTemp(new File(tempDir))
  }

  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
       |   sources = [
       |    {
       |      name = "spark1"
       |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
       |
       |      temp.hadoop.path = "$sourceTemp"
       |      file.name.pattern = "*.csv"
       |
       |      format = "csv"
       |
       |      has.information.date.column = true
       |      information.date.column = "INFO_DATE"
       |      information.date.type = "date"
       |      information.date.format = "yyyy-MM-DD"
       |    },
       |    {
       |      name = "spark2"
       |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
       |
       |      temp.hadoop.path = "$sourceTemp"
       |      file.name.pattern = "*.csv"
       |      recursive = true
       |
       |      format = "csv"
       |
       |      has.information.date.column = false
       |      limit.records = 3
       |      information.date.column = "INFO_DATE"
       |      information.date.type = "date"
       |      information.date.app.format = "yyyy-MM-DD"
       |      information.date.sql.format = "YYYY-mm-DD"
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

      assert(src.isInstanceOf[LocalSparkSource])
      assert(src.asInstanceOf[LocalSparkSource].hadoopTempPath == sourceTemp.toString)
      assert(src.asInstanceOf[LocalSparkSource].fileNamePattern == "*.csv")
      assert(!src.asInstanceOf[LocalSparkSource].isRecursive)
    }

    "be able to get a source by its name" in {
      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "spark2", "source")

      assert(src.isInstanceOf[LocalSparkSource])
      assert(src.asInstanceOf[LocalSparkSource].hadoopTempPath == sourceTemp.toString)
      assert(src.asInstanceOf[LocalSparkSource].fileNamePattern == "*.csv")
      assert(src.asInstanceOf[LocalSparkSource].isRecursive)
    }

    "be able to get a source by its name from the manager" in {
      val src = SourceManager.getSourceByName("spark1", conf, None)

      assert(src.isInstanceOf[LocalSparkSource])
      assert(src.asInstanceOf[LocalSparkSource].hadoopTempPath == sourceTemp.toString)
      assert(src.asInstanceOf[LocalSparkSource].fileNamePattern == "*.csv")
      assert(!src.asInstanceOf[LocalSparkSource].isRecursive)
    }

    "throw an exception if a source is not found" in {
      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "Dummy", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Unknown name of a data source: Dummy"))
    }

    "throw an exception when a source name is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
           |    },
           |    {
           |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
           |      has.information.date.column = false
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("A name is not configured for 2 source(s)"))
    }

    "should work when format is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "test"
           |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
           |
           |      temp.hadoop.path = "$sourceTemp"
           |      file.name.pattern = "*.csv"
           |
           |      has.information.date.column = false
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[LocalSparkSource]

      assert(src.isInstanceOf[LocalSparkSource])
      assert(src.asInstanceOf[LocalSparkSource].hadoopTempPath == sourceTemp.toString)
      assert(src.asInstanceOf[LocalSparkSource].fileNamePattern == "*.csv")
      assert(!src.asInstanceOf[LocalSparkSource].isRecursive)
    }

    "throw an exception when a factory class is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "mysource1"
           |    },
           |    {
           |      name = "mysource2"
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Factory class is not configured for 2 source(s)"))
    }

    "throw an exception when there are duplicate source names" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "mysource1"
           |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
           |    },
           |    {
           |      name = "MYsource1"
           |      factory.class = "za.co.absa.pramen.core.source.LocalSparkSource"
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[JdbcSource]
      }

      assert(ex.getMessage.contains("Duplicate source names: mysource1, MYsource1"))
    }
  }

  "hasInfoDateColumn" should {
    "always return false" in {
      val src = SourceManager.getSourceByName("spark2", conf, None)

      assert(!src.hasInfoDateColumn(null))
    }
  }

  "getReader()" should {
    "work according to the access pattern" in {
      val src = SourceManager.getSourceByName("spark2", conf, None)

      src.connect()

      val cnt = src.getRecordCount(Query.Path(filesPath.toString), LocalDate.now(), LocalDate.now())
      val df = src.getData(Query.Path(filesPath.toString), LocalDate.now(), LocalDate.now(), Nil).data

      assert(cnt == 10)
      assert(df.count() == 10)

      val filesBeforeClose = LocalFsUtils.getListOfFiles(Paths.get(sourceTemp.toString), includeDirs = true).length
      src.close()
      val filesAfterClose = LocalFsUtils.getListOfFiles(Paths.get(sourceTemp.toString), includeDirs = true).length

      assert(filesBeforeClose == 1)
      assert(filesAfterClose == 0)
    }
    "throw an exception on a non-path query" in {
      val src = SourceManager.getSourceByName("spark2", conf, None)

      src.connect()
      src.connect() // this is intended

      val ex = intercept[IllegalArgumentException] {
        src.getRecordCount(Query.Table("dbtanle"), LocalDate.now(), LocalDate.now())
      }

      assert(ex.getMessage.contains("Query 'Table(dbtanle)' is not supported."))

      src.close()
    }
  }
}
