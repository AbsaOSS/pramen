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

package za.co.absa.pramen.core.integration

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.JdbcUrlSelectorImpl
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.{FsUtils, JdbcNativeUtils, ResourceUtils}

import java.time.LocalDate

class IncrementalPipelineSuite extends AnyWordSpec
  with SparkTestBase
  with RelationalDbFixture
  with BeforeAndAfter
  with BeforeAndAfterAll
  with TempDirFixture
  with TextComparisonFixture {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  private val infoDate = LocalDate.of(2021, 2, 18)

  "File-based sourcing" should {
    val expected1 =
      """{"id":"1","name":"John"}
        |{"id":"2","name":"Jack"}
        |{"id":"3","name":"Jill"}
        |""".stripMargin

    val expected2 =
      """{"id":"1","name":"John"}
        |{"id":"2","name":"Jack"}
        |{"id":"3","name":"Jill"}
        |{"id":"4","name":"Mary"}
        |{"id":"5","name":"Jane"}
        |{"id":"6","name":"Kate"}
        |""".stripMargin

    "work end to end as a normal run" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val df1 = spark.read.parquet(table1Path.toString)
        val actual1 = df1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual1, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val exitCode2 = AppRunner.runPipeline(conf)
        assert(exitCode2 == 0)

        val df2 = spark.read.parquet(table1Path.toString)

        val batchIds = df2.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val actual2 = df2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual2, expected2)

        JdbcNativeUtils.withResultSet(new JdbcUrlSelectorImpl(jdbcConfig), "SELECT * FROM \"offsets\"", 1) { rs =>
          val mt = rs.getMetaData

          for (i <- 1 to mt.getColumnCount) {
            print(mt.getColumnName(i) + "\t")
          }
          println("")

          while (rs.next()) {
            for (i <- 1 to mt.getColumnCount) {
              print(rs.getString(i) + "\t")
            }
            println("")
          }
        }
      }
    }
  }

  def getConfig(basePath: String, isRerun: Boolean = false, useDataFrame: Boolean = false): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/incremental_pipeline.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
        s"""base.path = "$basePathEscaped"
           |use.dataframe = $useDataFrame
           |pramen.runtime.is.rerun = $isRerun
           |pramen.current.date = "$infoDate"
           |
           |pramen.bookkeeping.jdbc {
           |  driver = "$driver"
           |  url = "$url"
           |  user = "$user"
           |  password = "$password"
           |}
           |$configContents
           |""".stripMargin
      ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
