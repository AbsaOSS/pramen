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
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{Query, Source}
import za.co.absa.pramen.core.ExternalChannelFactoryReflect
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.reader.TableReaderSpark
import za.co.absa.pramen.core.utils.LocalFsUtils

import java.io.File
import java.time.LocalDate

class SparkSourceSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with SparkTestBase {
  import spark.implicits._

  val tempDir: String = createTempDir("spark_source")
  val sourceTemp = new Path(tempDir, "temp")
  val filesPath = new Path(tempDir, "files")

  override def beforeAll(): Unit = {
    super.beforeAll()

    Range(0, 10).toDF("id")
      .repartition(2)
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
       |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
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
       |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
       |
       |      format = "csv"
       |      schema = "id int"
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

      assert(src.isInstanceOf[SparkSource])
      assert(src.asInstanceOf[SparkSource].format.contains("csv"))
      assert(src.asInstanceOf[SparkSource].schema.isEmpty)
    }

    "be able to get a source by its name" in {
      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "spark2", "source")

      assert(src.isInstanceOf[SparkSource])
      assert(src.asInstanceOf[SparkSource].format.contains("csv"))
      assert(src.asInstanceOf[SparkSource].schema.contains("id int"))
    }

    "be able to get a source by its name from the manager" in {
      val src = SourceManager.getSourceByName("spark1", conf, None)

      assert(src.isInstanceOf[SparkSource])
      assert(src.asInstanceOf[SparkSource].format.contains("csv"))
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
           |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
           |    },
           |    {
           |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
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

    "work when format is not configured" in {
      val conf = ConfigFactory.parseString(
        s"""
           | pramen {
           |   sources = [
           |    {
           |      name = "test"
           |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
           |
           |      has.information.date.column = false
           |    }
           |  ]
           | }
           |""".stripMargin)
        .withFallback(ConfigFactory.load())
        .resolve()

      val src = ExternalChannelFactoryReflect.fromConfigByName[Source](conf, None, "pramen.sources", "test", "source").asInstanceOf[SparkSource]

      assert(src.format.isEmpty)
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
           |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
           |    },
           |    {
           |      name = "MYsource1"
           |      factory.class = "za.co.absa.pramen.core.source.SparkSource"
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
    "return true if the source is configured with info date column" in {
      val src = SourceManager.getSourceByName("spark1", conf, None)

      assert(src.hasInfoDateColumn(null))
    }

    "return false if the source is configured without info date column" in {
      val src = SourceManager.getSourceByName("spark2", conf, None)

      assert(!src.hasInfoDateColumn(null))
    }
  }

  "getRecordCount()" should {
    val src = SourceManager.getSourceByName("spark2", conf, None)

    "return proper record count" in {
      val cnt = src.getRecordCount(Query.Path(filesPath.toString), LocalDate.now(), LocalDate.now())

      assert(cnt == 10)
    }
  }

  "getData()" should {
    val src = SourceManager.getSourceByName("spark2", conf, None)

    "return proper dataframe for a path" in {
      val result = src.getData(Query.Path(filesPath.toString), LocalDate.now(), LocalDate.now(), Nil)

      assert(result.data.count() == 10)
    }

    "return proper dataframe for a catalog table" in {
      val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      exampleDf.createOrReplaceTempView("my_table1")

      val result = src.getData(Query.Table("my_table1"), LocalDate.now(), LocalDate.now(), Nil)

      assert(result.data.count() == 3)

      spark.catalog.dropTempView("my_table1")
    }

    "return proper dataframe for an SQL statement" in {
      val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      exampleDf.createOrReplaceTempView("my_table2")

      val result = src.getData(Query.Sql("SELECT * FROM my_table2 WHERE b > 1"), LocalDate.now(), LocalDate.now(), Nil)

      assert(result.data.count() == 2)

      spark.catalog.dropTempView("my_table2")
    }

    "return proper list of files for a path" in {
      val result = src.getData(Query.Path(filesPath.toString), LocalDate.now(), LocalDate.now(), Nil)

      assert(result.filesRead.length == 2)
    }

    "throw an exception on incorrect query type" in {
      val ex = intercept[IllegalArgumentException] {
        src.getData(Query.Custom(Map.empty), LocalDate.now(), LocalDate.now(), Nil)
      }

      assert(ex.getMessage.contains("is not supported by the Spark source. Use 'path', 'table' or 'sql' instead."))
    }
  }

  "getReader()" should {
    val src = SourceManager.getSourceByName("spark2", conf, None).asInstanceOf[SparkSource]

    "get a reader for path" in {
      val reader = src.getReader(Query.Path(filesPath.toString))

      assert(reader.isInstanceOf[TableReaderSpark])
    }

    "get a reader for the SQL query" in {
      val reader = src.getReader(Query.Sql("select * from table"))

      assert(reader.isInstanceOf[TableReaderSpark])
    }

    "get a reader for a catalog table SQL query" in {
      val reader = src.getReader(Query.Sql("select * from table"))

      assert(reader.isInstanceOf[TableReaderSpark])
    }

    "throw an exception on an unsupported query type" in {
      val ex = intercept[IllegalArgumentException] {
        src.getReader(Query.Custom(Map.empty))
      }

      assert(ex.getMessage.contains("is not supported by the Spark source. Use 'path', 'table' or 'sql' instead."))
    }
  }
}
