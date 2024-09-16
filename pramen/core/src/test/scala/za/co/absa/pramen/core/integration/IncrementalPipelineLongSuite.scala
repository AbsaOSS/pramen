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
import org.apache.spark.sql.functions.lit
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.OffsetManagerJdbc
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.JdbcUrlSelectorImpl
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.{FsUtils, JdbcNativeUtils, ResourceUtils}

import java.time.LocalDate

class IncrementalPipelineLongSuite extends AnyWordSpec
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

  "For inputs without information date the pipeline" should {
    val expected1 =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |""".stripMargin

    val expected2 =
      """{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    val expectedAll =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    "work end to end as a normal run" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val exitCode2 = AppRunner.runPipeline(conf)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1After, expectedAll)
        compareText(actualTable2After, expectedAll)
      }
    }

    "work with incremental ingestion and normal transformer" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir, isTransformerIncremental = false)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val exitCode2 = AppRunner.runPipeline(conf)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1After, expectedAll)
        compareText(actualTable2After, expectedAll)
      }
    }

    "work end to end as rerun" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf2 = getConfig(tempDir, isRerun = true)
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 1)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        // Expecting original records
        compareText(actualTable1After, expected1)
        compareText(actualTable2After, expected1)
      }
    }

    "work end to end as rerun with deletion of records" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf2 = getConfig(tempDir, isRerun = true)
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.isEmpty)

        // Expecting empty records
        assert(dfTable1After.isEmpty)
        assert(dfTable2After.isEmpty)
      }
    }

    "work end to end as rerun with deletion of records with previous data present" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path0 = new Path(tempDir, new Path("landing", "landing_file0.csv"))
        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path0, "id,name\n0,Old\n")

        val conf0 = getConfig(tempDir, useInfoDate = infoDate.minusDays(1))

        val exitCode0 = AppRunner.runPipeline(conf0)
        fsUtils.deleteFile(path1)

        assert(exitCode0 == 0)

        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf1 = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf1)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf2 = getConfig(tempDir, isRerun = true)
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.isEmpty)

        // Expecting empty records
        assert(dfTable1After.isEmpty)
        assert(dfTable2After.isEmpty)
      }
    }

    "run for a historical date range with force update" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf1 = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf1)
        assert(exitCode1 == 0)

        val table1Path1 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path1 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path1.toString)
        val dfTable2Before = spark.read.parquet(table2Path1.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf2 = getConfig(tempDir, isHistoricalRun = true, useInfoDate = infoDate.plusDays(1))
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val table1Path2 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val table2Path2 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val dfTable1After1 = spark.read.parquet(table1Path1.toString)
        val dfTable2After1 = spark.read.parquet(table2Path1.toString)
        val dfTable1After2 = spark.read.parquet(table1Path2.toString)
        val dfTable2After2 = spark.read.parquet(table2Path2.toString)
        val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        val batchIdsOld = dfTable1After1.select("pramen_batchid").distinct().collect().map(_.getLong(0))
        val batchIdsNew = dfTable1After2.select("pramen_batchid").distinct().collect().map(_.getLong(0))

        assert(batchIdsOld.length == 1)
        assert(batchIdsNew.length == 1)

        // The batch id for all info dates in range should be the same since they were re-ran
        assert(batchIdsOld.head == batchIdsNew.head)

        // Expecting empty records
        compareText(actualTable1After1, expected1)
        compareText(actualTable2After1, expected1)
        compareText(actualTable1After2, expected2)
        compareText(actualTable2After2, expected2)
      }
    }

    "run for a historical date range with fill gaps update" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf1 = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf1)
        assert(exitCode1 == 0)

        val table1Path1 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path1 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path1.toString)
        val dfTable2Before = spark.read.parquet(table2Path1.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf2 = getConfig(tempDir, isHistoricalRun = true, historyRunMode = "fill_gaps", useInfoDate = infoDate.plusDays(1))
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val table1Path2 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val table2Path2 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val dfTable1After1 = spark.read.parquet(table1Path1.toString)
        val dfTable2After1 = spark.read.parquet(table2Path1.toString)
        val dfTable1After2 = spark.read.parquet(table1Path2.toString)
        val dfTable2After2 = spark.read.parquet(table2Path2.toString)
        val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        val batchIdsOld = dfTable1After1.select("pramen_batchid").distinct().collect().map(_.getLong(0))
        val batchIdsNew = dfTable1After2.select("pramen_batchid").distinct().collect().map(_.getLong(0))

        assert(batchIdsOld.length == 1)
        assert(batchIdsNew.length == 1)

        // The batch id for all info dates in range should not be the same since they we ran only for missing data
        assert(batchIdsOld.head != batchIdsNew.head)

        // Expecting empty records
        compareText(actualTable1After1, expected1)
        compareText(actualTable2After1, expected1)
        compareText(actualTable1After2, expected2)
        compareText(actualTable2After2, expected2)
      }
    }

    "deal with uncommitted changes when no data" in {
      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      om.startWriteOffsets("table1", infoDate, OffsetValue.fromString("long", "1"))

      Thread.sleep(10)

      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val dfTable1 = spark.read.parquet(table1Path.toString)
        val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1, expected1)

        val batchIds = dfTable1.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 1)

        val offsets = om.getOffsets("table1", infoDate).sortBy(_.createdAt)

        assert(offsets.length == 1)

        assert(offsets.head.minOffset.valueString.toLong == Long.MinValue)
        assert(offsets.head.maxOffset.get.valueString.toLong == 3)
        assert(offsets.head.committedAt.nonEmpty)
      }
    }

    "deal with uncommitted changes when there is data" in {
      val om1 = new OffsetManagerJdbc(pramenDb.db, 123L)
      om1.startWriteOffsets("table1", infoDate, OffsetValue.LongType(Long.MinValue))

      Thread.sleep(10)

      val om2 = new OffsetManagerJdbc(pramenDb.db, 123L)
      om2.startWriteOffsets("table1", infoDate, OffsetValue.LongType(2))

      Thread.sleep(10)

      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")
        fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")

        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path1.toString)
          .withColumn("pramen_batchid", lit(123L))

        df.write.parquet(table1Path.toString)

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val dfTable1 = spark.read.parquet(table1Path.toString)
        val dfTable2 = spark.read.parquet(table2Path.toString)
        val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2 = dfTable2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1, expectedAll)
        compareText(actualTable2, expected2)    // ToDo This logic is to be changed when incremental transformations are supported

        val batchIds = dfTable1.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val offsets = om2.getOffsets("table1", infoDate).sortBy(_.createdAt)

        assert(offsets.length == 2)

        assert(offsets.head.minOffset.valueString.toLong == Long.MinValue)
        assert(offsets.head.maxOffset.get.valueString.toLong == 3)
        assert(offsets.head.committedAt.nonEmpty)
        assert(offsets(1).minOffset.valueString.toLong == 3)
        assert(offsets(1).maxOffset.get.valueString.toLong == 6)
        assert(offsets(1).committedAt.nonEmpty)
      }
    }

    "fail is the input data type does not conform" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

        val conf = getConfig(tempDir, inferSchema = false)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 2)
      }
    }

    "fail if the output table does not have the offset field" in {
      val om1 = new OffsetManagerJdbc(pramenDb.db, 123L)
      om1.startWriteOffsets("table1", infoDate, OffsetValue.LongType(Long.MinValue))

      Thread.sleep(10)

      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, "name\nJohn\nJack\nJill\n")
        fsUtils.writeFile(path2, "name\nMary\nJane\nKate\n")

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")

        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path1.toString)
          .withColumn("pramen_batchid", lit(123L))

        df.write.parquet(table1Path.toString)

        val conf = getConfig(tempDir)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 2)
      }
    }
  }

  "For inputs with information date the pipeline" should {
    val csv1Str = s"id,name,info_date\n0,Old,${infoDate.minusDays(1)}\n1,John,$infoDate\n2,Jack,$infoDate\n3,Jill,$infoDate\n99,New,${infoDate.plusDays(1)}\n"
    val csv2Str = s"id,name,info_date\n1,John,${infoDate.minusDays(1)}\n4,Mary,$infoDate\n5,Jane,$infoDate\n6,Kate,$infoDate\n999,New2,${infoDate.plusDays(1)}\n"

    val expected1 =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |""".stripMargin

    val expected2 =
      """{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    val expectedAll =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    "work end to end as a normal run" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, csv1Str)

        val conf = getConfig(tempDir, hasInfoDate = true)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, csv2Str)

        val exitCode2 = AppRunner.runPipeline(conf)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1After, expectedAll)
        compareText(actualTable2After, expectedAll)
      }
    }

    "work with incremental ingestion and normal transformer" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, csv1Str)

        val conf = getConfig(tempDir, hasInfoDate = true, isTransformerIncremental = false)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, csv2Str)

        val exitCode2 = AppRunner.runPipeline(conf)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 2)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1After, expectedAll)
        compareText(actualTable2After, expectedAll)
      }
    }

    "work end to end as rerun" in {
      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
        fsUtils.writeFile(path1, csv1Str)

        val conf = getConfig(tempDir, hasInfoDate = true)

        val exitCode1 = AppRunner.runPipeline(conf)
        assert(exitCode1 == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path.toString)
        val dfTable2Before = spark.read.parquet(table2Path.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)

        fsUtils.deleteFile(path1)
        fsUtils.writeFile(path2, csv2Str)

        val conf2 = getConfig(tempDir, isRerun = true, hasInfoDate = true)
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val dfTable1After = spark.read.parquet(table1Path.toString)
        val dfTable2After = spark.read.parquet(table2Path.toString)

        val batchIds = dfTable1After.select("pramen_batchid").distinct().collect()

        assert(batchIds.length == 1)

        val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        // Expecting original records
        compareText(actualTable1After, expected2)
        compareText(actualTable2After, expected2)
      }
    }

    "work for historical runs" in {
      val csv1Str = s"id,name,info_date\n0,Old,${infoDate.minusDays(1)}\n1,John,$infoDate\n2,Jack,$infoDate\n3,Jill,$infoDate\n99,New,${infoDate.plusDays(2)}\n"
      val csv2Str = s"id,name,info_date\n4,Mary,${infoDate.plusDays(1)}\n5,Jane,${infoDate.plusDays(1)}\n6,Kate,${infoDate.plusDays(1)}\n999,New2,${infoDate.plusDays(2)}\n"

      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
        val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

        fsUtils.writeFile(path1, csv1Str)
        fsUtils.writeFile(path2, csv2Str)

        val conf1 = getConfig(tempDir, hasInfoDate = true)

        val exitCode1 = AppRunner.runPipeline(conf1)
        assert(exitCode1 == 0)

        val table1Path1 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path1 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1Before = spark.read.parquet(table1Path1.toString)
        val dfTable2Before = spark.read.parquet(table2Path1.toString)
        val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1Before, expected1)
        compareText(actualTable2Before, expected1)


        val conf2 = getConfig(tempDir, hasInfoDate = true, isHistoricalRun = true, historyRunMode = "fill_gaps", useInfoDate = infoDate.plusDays(1))
        val exitCode2 = AppRunner.runPipeline(conf2)
        assert(exitCode2 == 0)

        val table1Path2 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val table2Path2 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=${infoDate.plusDays(1)}")
        val dfTable1After1 = spark.read.parquet(table1Path1.toString)
        val dfTable2After1 = spark.read.parquet(table2Path1.toString)
        val dfTable1After2 = spark.read.parquet(table1Path2.toString)
        val dfTable2After2 = spark.read.parquet(table2Path2.toString)
        val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        val batchIdsOld = dfTable1After1.select("pramen_batchid").distinct().collect().map(_.getLong(0))
        val batchIdsNew = dfTable1After2.select("pramen_batchid").distinct().collect().map(_.getLong(0))

        assert(batchIdsOld.length == 1)
        assert(batchIdsNew.length == 1)

        // The batch id for all info dates in range should not be the same since they we ran only for missing data
        assert(batchIdsOld.head != batchIdsNew.head)

        // Expecting empty records
        compareText(actualTable1After1, expected1)
        compareText(actualTable2After1, expected1)
        compareText(actualTable1After2, expected2)
        compareText(actualTable2After2, expected2)
      }
    }
  }

  "Edge cases" should {
    val expected1 =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |""".stripMargin

    val expected2 =
      """{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |""".stripMargin

    "offsets cross info days" in {
      val csv1Str = s"id,name,info_date\n0,Old,${infoDate.minusDays(2)}\n1,John,${infoDate.minusDays(1)}\n2,Jack,${infoDate.minusDays(1)}\n3,Jill,$infoDate\n4,Mary,$infoDate\n"

      withTempDirectory("incremental1") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))

        fsUtils.writeFile(path1, csv1Str)

        val conf1 = getConfig(tempDir, hasInfoDate = true)

        val exitCode1 = AppRunner.runPipeline(conf1)
        assert(exitCode1 == 0)

        val table1Path1 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table2Path1 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table1Path2 = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val table2Path2 = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val dfTable1_1 = spark.read.parquet(table1Path1.toString)
        val dfTable2_1 = spark.read.parquet(table2Path1.toString)
        val dfTable1_2 = spark.read.parquet(table1Path2.toString)
        val dfTable2_2 = spark.read.parquet(table2Path2.toString)

        val actualTable1_1 = dfTable1_1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2_1 = dfTable2_1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable1_2 = dfTable1_2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
        val actualTable2_2 = dfTable2_2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

        compareText(actualTable1_1, expected1)
        compareText(actualTable2_1, expected1)
        compareText(actualTable1_2, expected2)
        compareText(actualTable2_2, expected2)

        val om = new OffsetManagerJdbc(pramenDb.db, 123L)

        val offsets1 = om.getOffsets("table1", infoDate.minusDays(1))
        assert(offsets1.length == 1)
        assert(offsets1.head.minOffset.valueString.toLong == Long.MinValue )
        assert(offsets1.head.maxOffset.get.valueString.toLong == 2)
        assert(offsets1.head.committedAt.nonEmpty)

        val offsets2 = om.getOffsets("table1", infoDate)

        assert(offsets2.length == 1)
        assert(offsets2.head.minOffset.valueString.toLong == Long.MinValue)
        assert(offsets2.head.maxOffset.get.valueString.toLong == 4)
        assert(offsets2.head.committedAt.nonEmpty)
      }
    }
  }

  def getConfig(basePath: String,
                isRerun: Boolean = false,
                useDataFrame: Boolean = false,
                isTransformerIncremental: Boolean = true,
                isHistoricalRun: Boolean = false,
                historyRunMode: String = "force",
                inferSchema: Boolean = true,
                hasInfoDate: Boolean = false,
                useInfoDate: LocalDate = infoDate): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/incremental_pipeline.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")
    val transformerSchedule = if (isTransformerIncremental) "incremental" else "daily"
    val historicalConfigStr = if (isHistoricalRun) {
      s"""pramen.load.date.from = "${useInfoDate.minusDays(1)}"
         |pramen.load.date.to = "$useInfoDate"
         |pramen.runtime.run.mode = "$historyRunMode"
         |""".stripMargin
    } else {
      ""
    }

    val conf = ConfigFactory.parseString(
        s"""base.path = "$basePathEscaped"
           |use.dataframe = $useDataFrame
           |pramen.runtime.is.rerun = $isRerun
           |pramen.current.date = "$useInfoDate"
           |transformer.schedule = "$transformerSchedule"
           |infer.schema = $inferSchema
           |$historicalConfigStr
           |has.information.date.column = $hasInfoDate
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

  /* This method is used to inspect offsets after operations */
  private def debugOffsets(): Unit = {
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
