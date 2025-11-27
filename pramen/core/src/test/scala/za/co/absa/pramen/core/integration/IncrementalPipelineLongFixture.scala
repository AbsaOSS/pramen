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
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.api.offset.DataOffset.CommittedOffset
import za.co.absa.pramen.api.offset.OffsetType
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.OffsetManagerJdbc
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.JdbcUrlSelectorImpl
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.{FsUtils, JdbcNativeUtils, ResourceUtils}

import java.sql.Date
import java.time.LocalDate

class IncrementalPipelineLongFixture extends AnyWordSpec
  with SparkTestBase
  with RelationalDbFixture
  with BeforeAndAfter
  with BeforeAndAfterAll
  with TempDirFixture
  with TextComparisonFixture {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  lazy val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  private val infoDate = LocalDate.of(2021, 2, 18)

  private val BATCH_ID_COLUMN = "pramen_batchid"
  private val INFO_DATE_COLUMN = "pramen_info_date"

  val expectedOffsetOnly1: String =
    """{"id":1,"name":"John"}
      |{"id":2,"name":"Jack"}
      |{"id":3,"name":"Jill"}
      |""".stripMargin

  val expectedOffsetOnly2: String =
    """{"id":4,"name":"Mary"}
      |{"id":5,"name":"Jane"}
      |{"id":6,"name":"Kate"}
      |""".stripMargin

  val expectedOffsetOnlyAll: String =
    """{"id":1,"name":"John"}
      |{"id":2,"name":"Jack"}
      |{"id":3,"name":"Jill"}
      |{"id":4,"name":"Mary"}
      |{"id":5,"name":"Jane"}
      |{"id":6,"name":"Kate"}
      |""".stripMargin

  val csv1WithInfoDateStr = s"id,name,info_date\n0,Old,${infoDate.minusDays(1)}\n1,John,$infoDate\n2,Jack,$infoDate\n3,Jill,$infoDate\n99,New,${infoDate.plusDays(1)}\n"
  val csv2WithInfoDateStr = s"id,name,info_date\n1,John,${infoDate.minusDays(1)}\n4,Mary,$infoDate\n5,Jane,$infoDate\n6,Kate,$infoDate\n999,New2,${infoDate.plusDays(1)}\n"
  val csvWithInfoDateSchema = "id int,name string,info_date date"

  val expectedWithInfoDate1: String =
    """{"id":1,"name":"John"}
      |{"id":2,"name":"Jack"}
      |{"id":3,"name":"Jill"}
      |""".stripMargin

  val expectedWithInfoDate2: String =
    """{"id":4,"name":"Mary"}
      |{"id":5,"name":"Jane"}
      |{"id":6,"name":"Kate"}
      |""".stripMargin

  val expectedWithInfoDateAll: String =
    """{"id":1,"name":"John"}
      |{"id":2,"name":"Jack"}
      |{"id":3,"name":"Jill"}
      |{"id":4,"name":"Mary"}
      |{"id":5,"name":"Jane"}
      |{"id":6,"name":"Kate"}
      |""".stripMargin

  val csv1WithTimestampStr = s"tss,name\n1613563930000,Old\n1613639398123,John\n1613639398124,Jack\n1613639399123,Jill\n1613740330000,New\n"

  val expectedWithTimestamp1: String =
    """{"ts":"2021-02-18T11:09:58.123+02:00","name":"John","pramen_info_date":"2021-02-18"}
      |{"ts":"2021-02-18T11:09:58.124+02:00","name":"Jack","pramen_info_date":"2021-02-18"}
      |{"ts":"2021-02-18T11:09:59.123+02:00","name":"Jill","pramen_info_date":"2021-02-18"}
      |""".stripMargin

  val expectedWithTimestamp2: String =
    """{"ts":"2021-02-18T11:09:58.123+02:00","name":"John","pramen_info_date":"2021-02-18"}
      |{"ts":"2021-02-18T11:09:58.124+02:00","name":"Jack","pramen_info_date":"2021-02-18"}
      |{"ts":"2021-02-18T11:09:59.123+02:00","name":"Jill","pramen_info_date":"2021-02-18"}
      |{"ts":"2021-02-19T15:12:10.000+02:00","name":"New","pramen_info_date":"2021-02-19"}
      |""".stripMargin

  def testOffsetOnlyNormalRun(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val exitCode2 = AppRunner.runPipeline(conf)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedOffsetOnlyAll)
      compareText(actualTable2After, expectedOffsetOnlyAll)
    }
    succeed
  }

  def testOffsetOnlyRunningOutOfOrderOffsets(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, useInfoDate = infoDate.minusDays(1))
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After0 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val dfTable2After0 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      val batchIds = dfTable1After1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.isEmpty)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)
      val offsets0 = om.getOffsets("table1", infoDate.minusDays(2))
      val offsets1 = om.getOffsets("table1", infoDate.minusDays(1))
      val offsets2 = om.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset])

      assert(offsets0.isEmpty)
      assert(offsets1.isEmpty)
      assert(offsets2.nonEmpty)

      assert(offsets2.head.maxOffset.valueString == "3")

      // Expecting empty records
      assert(dfTable1After0.isEmpty)
      assert(dfTable2After0.isEmpty)
      assert(dfTable1After1.isEmpty)
      assert(dfTable2After1.isEmpty)
      compareText(actualTable1After2, expectedOffsetOnly1)
      compareText(actualTable2After2, expectedOffsetOnly1)
    }
    succeed
  }

  def testOffsetOnlyUncommittedLateOffsets(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat, useInfoDate = infoDate.minusDays(2))

      // Running a job that ingests initial data for offsets 1..3 at 2021-02-16
      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      // Adding an uncommitted offset for 2021-02-17
      val om = new OffsetManagerJdbc(pramenDb.db, 123L)
      om.startWriteOffsets("table1", infoDate.minusDays(1), OffsetType.IntegralType)
      Thread.sleep(10)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      // Writing data for the metastore table for the uncommitted offset at 2021-02-17
      val dfIn = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path2.toString)
        .withColumn(INFO_DATE_COLUMN, lit(Date.valueOf(infoDate.minusDays(1))))

      dfIn.write
        .mode(SaveMode.Append)
        .partitionBy(INFO_DATE_COLUMN)
        .format(metastoreFormat)
        .save(table1Path.toString)

      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      // Checking expected offset table
      val offsetsBefore0 = om.getOffsets("table1", infoDate.minusDays(2))
      val offsetsBefore1 = om.getOffsets("table1", infoDate.minusDays(1))
      val offsetsBefore2 = om.getOffsets("table1", infoDate)

      assert(offsetsBefore0.length == 1)
      assert(offsetsBefore1.length == 1)
      assert(offsetsBefore2.isEmpty)

      assert(offsetsBefore0.head.isCommitted)
      assert(!offsetsBefore1.head.isCommitted)
      assert(offsetsBefore0.head.asInstanceOf[CommittedOffset].minOffset.valueString == "1")
      assert(offsetsBefore0.head.asInstanceOf[CommittedOffset].maxOffset.valueString == "3")

      // Running the job for 2021-02-18. It should reconcile uncommitted offsets and return with 'No data' since no data arrived since 2021-02-17
      val conf2 = getConfig(tempDir, metastoreFormat, useInfoDate = infoDate)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 2)

      // Checking expected offset table
      val offsetsAfter0 = om.getOffsets("table1", infoDate.minusDays(2))
      val offsetsAfter1 = om.getOffsets("table1", infoDate.minusDays(1))
      val offsetsAfter2 = om.getOffsets("table1", infoDate)

      assert(offsetsAfter0.length == 1)
      assert(offsetsAfter1.length == 1)
      assert(offsetsAfter2.isEmpty)

      assert(offsetsAfter0.head.isCommitted)
      assert(offsetsAfter1.head.isCommitted)
      assert(offsetsAfter0.head.asInstanceOf[CommittedOffset].minOffset.valueString == "1")
      assert(offsetsAfter0.head.asInstanceOf[CommittedOffset].maxOffset.valueString == "3")
      assert(offsetsAfter1.head.asInstanceOf[CommittedOffset].minOffset.valueString == "4")
      assert(offsetsAfter1.head.asInstanceOf[CommittedOffset].maxOffset.valueString == "6")

      // Checking expected data
      val dfTable1After0 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val dfTable2After0 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(2)))
      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1After0 = dfTable1After0.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After0 = dfTable2After0.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      /* This will be enabled when incremental processing is implemented.
      val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")*/

      // Expecting empty records
      compareText(actualTable1After0, expectedOffsetOnly1)
      compareText(actualTable2After0, expectedOffsetOnly1)
      compareText(actualTable1After1, expectedOffsetOnly2)
      /*compareText(actualTable2After1, expectedOffsetOnly2) This will be enabled when incremental processing is implemented. */
      assert(dfTable1After2.isEmpty)
      assert(dfTable2After2.isEmpty)
    }
    succeed
  }

  def testOffsetOnlyIncrementalIngestionNormalTransformer(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat, isTransformerIncremental = false)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val exitCode2 = AppRunner.runPipeline(conf)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedOffsetOnlyAll)
      compareText(actualTable2After, expectedOffsetOnlyAll)
    }
    succeed
  }

  def testOffsetOnlyRerun(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isRerun = true)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 1)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      // Expecting original records
      compareText(actualTable1After, expectedOffsetOnly1)
      compareText(actualTable2After, expectedOffsetOnly1)
    }
    succeed
  }

  def testOffsetOnlyRerunWithRecordsDeletion(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isRerun = true)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.isEmpty)

      // Expecting empty records
      assert(dfTable1After.isEmpty)
      assert(dfTable2After.isEmpty)
    }
    succeed
  }

  def testOffsetOnlySkipRerunWithoutOffsets(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isRerun = true, useInfoDate = infoDate.minusDays(1))
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      val batchIds = dfTable1After1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.isEmpty)

      // Expecting empty records
      assert(dfTable1After1.isEmpty)
      assert(dfTable2After1.isEmpty)
      compareText(actualTable1After2, expectedOffsetOnly1)
      compareText(actualTable2After2, expectedOffsetOnly1)
    }
    succeed
  }

  def testOffsetOnlyRerunWithRecordsDeletionAndPreviousDataPresent(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path0 = new Path(tempDir, new Path("landing", "landing_file0.csv"))
      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path0, "id,name\n0,Old\n")

      val conf0 = getConfig(tempDir, metastoreFormat, useInfoDate = infoDate.minusDays(1))

      val exitCode0 = AppRunner.runPipeline(conf0)
      fsUtils.deleteFile(path1)

      assert(exitCode0 == 0)

      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf1 = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isRerun = true)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.isEmpty)

      // Expecting empty records
      assert(dfTable1After.isEmpty)
      assert(dfTable2After.isEmpty)
    }
    succeed
  }

  def testOffsetOnlyHistoricalDateRangeWithForceUpdate(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf1 = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      val table1Path1 = new Path(tempDir, "table1")
      val table2Path1 = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isHistoricalRun = true, useInfoDate = infoDate.plusDays(1))
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val table1Path2 = new Path(tempDir, "table1")
      val table2Path2 = new Path(tempDir, "table2")
      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path2.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path2.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      val batchIdsOld = dfTable1After1.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))
      val batchIdsNew = dfTable1After2.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))

      assert(batchIdsOld.length == 1)
      assert(batchIdsNew.length == 1)

      // The batch id for all info dates in range should be the same since they were re-ran
      assert(batchIdsOld.head == batchIdsNew.head)

      // Expecting empty records
      compareText(actualTable1After1, expectedOffsetOnly1)
      compareText(actualTable2After1, expectedOffsetOnly1)
      compareText(actualTable1After2, expectedOffsetOnly2)
      compareText(actualTable2After2, expectedOffsetOnly2)
    }
    succeed
  }

  def testOffsetOnlyHistoricalDateRangeWithFillGaps(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf1 = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      val table1Path1 = new Path(tempDir, "table1")
      val table2Path1 = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedOffsetOnly1)
      compareText(actualTable2Before, expectedOffsetOnly1)

      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val conf2 = getConfig(tempDir, metastoreFormat, isHistoricalRun = true, historyRunMode = "fill_gaps", useInfoDate = infoDate.plusDays(1))
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val table1Path2 = new Path(tempDir, "table1")
      val table2Path2 = new Path(tempDir, "table2")
      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path1.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path2.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path2.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      val batchIdsOld = dfTable1After1.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))
      val batchIdsNew = dfTable1After2.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))

      assert(batchIdsOld.length == 1)
      assert(batchIdsNew.length == 1)

      // The batch id for all info dates in range should not be the same since they we ran only for missing data
      assert(batchIdsOld.head != batchIdsNew.head)

      // Expecting empty records
      compareText(actualTable1After1, expectedOffsetOnly1)
      compareText(actualTable2After1, expectedOffsetOnly1)
      compareText(actualTable1After2, expectedOffsetOnly2)
      compareText(actualTable2After2, expectedOffsetOnly2)
    }
    succeed
  }

  def testOffsetOnlyDealWithUncommittedOffsetsWithNoPath(metastoreFormat: String): Assertion = {
    val om = new OffsetManagerJdbc(pramenDb.db, 123L)

    om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

    Thread.sleep(10)

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val table1Path = new Path(tempDir, "table1")
      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))

      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val dfTable1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1, expectedOffsetOnly1)

      val batchIds = dfTable1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 1)

      val offsets = om.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset]).sortBy(_.createdAt)

      assert(offsets.length == 1)

      assert(offsets.head.minOffset.valueString.toLong == 1)
      assert(offsets.head.maxOffset.valueString.toLong == 3)
    }
    succeed
  }

  def testOffsetOnlyDealWithUncommittedOffsetsWithNoData(metastoreFormat: String): Assertion = {
    val om = new OffsetManagerJdbc(pramenDb.db, 123L)

    om.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

    Thread.sleep(10)

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val table1Path = new Path(tempDir, "table1")
      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))

      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path1.toString)
        .withColumn(BATCH_ID_COLUMN, lit(123L))
        .withColumn(INFO_DATE_COLUMN, lit(Date.valueOf(infoDate)))

      df.filter(col("id") < 0)
        .write
        .format(metastoreFormat)
        .partitionBy(INFO_DATE_COLUMN)
        .save(table1Path.toString)

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val dfTable1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1, expectedOffsetOnly1)

      val batchIds = dfTable1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 1)

      val offsets = om.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset]).sortBy(_.createdAt)

      assert(offsets.length == 1)

      assert(offsets.head.minOffset.valueString.toLong == 1)
      assert(offsets.head.maxOffset.valueString.toLong == 3)
    }
    succeed
  }

  def testOffsetOnlyDealWithUncommittedOffsetsWithData(metastoreFormat: String): Assertion = {
    val om1 = new OffsetManagerJdbc(pramenDb.db, 123L)
    om1.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

    Thread.sleep(10)

    val om2 = new OffsetManagerJdbc(pramenDb.db, 123L)
    om2.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

    Thread.sleep(10)

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")
      fsUtils.writeFile(path2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path1.toString)
        .withColumn(BATCH_ID_COLUMN, lit(123L))
        .withColumn(INFO_DATE_COLUMN, lit(Date.valueOf(infoDate)))

      df.write
        .format(metastoreFormat)
        .partitionBy(INFO_DATE_COLUMN)
        .save(table1Path.toString)

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val dfTable1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2 = dfTable2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1, expectedOffsetOnlyAll)
      compareText(actualTable2, expectedOffsetOnlyAll)

      val batchIds = dfTable1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val offsets = om2.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset]).sortBy(_.createdAt)

      assert(offsets.length == 2)

      assert(offsets.head.minOffset.valueString.toLong == 1)
      assert(offsets.head.maxOffset.valueString.toLong == 3)
      assert(offsets(1).minOffset.valueString.toLong == 4)
      assert(offsets(1).maxOffset.valueString.toLong == 6)
    }
    succeed
  }

  def testOffsetOnlyFailWhenInputDataDoesNotConform(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      fsUtils.writeFile(path1, "id,name\n1,John\n2,Jack\n3,Jill\n")

      val conf = getConfig(tempDir, metastoreFormat, inferSchema = false, csvSchema = "id string,name string")

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 2)
    }
    succeed
  }

  def testOffsetOnlyFailWhenInputTableDoestHaveOffsetField(metastoreFormat: String): Assertion = {
    val om1 = new OffsetManagerJdbc(pramenDb.db, 123L)
    om1.startWriteOffsets("table1", infoDate, OffsetType.IntegralType)

    Thread.sleep(10)

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, "name\nJohn\nJack\nJill\n")
      fsUtils.writeFile(path2, "name\nMary\nJane\nKate\n")

      val table1Path = new Path(tempDir, "table1")

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path1.toString)
        .withColumn(BATCH_ID_COLUMN, lit(123L))
        .withColumn(INFO_DATE_COLUMN, lit(Date.valueOf(infoDate)))

      df.write.format(metastoreFormat)
        .partitionBy(INFO_DATE_COLUMN)
        .save(table1Path.toString)

      val conf = getConfig(tempDir, metastoreFormat)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 2)
    }
    succeed
  }

  def testWithInfoDateNormalRun(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, csv1WithInfoDateStr)

      val conf = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedWithInfoDate1)
      compareText(actualTable2Before, expectedWithInfoDate1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, csv2WithInfoDateStr)

      val exitCode2 = AppRunner.runPipeline(conf)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.parquet(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After = spark.read.parquet(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedWithInfoDateAll)
      compareText(actualTable2After, expectedWithInfoDateAll)
    }
    succeed
  }

  def testWithInfoDateIncrementalIngestionNormalTransformer(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, csv1WithInfoDateStr)

      val conf = getConfig(tempDir, metastoreFormat, hasInfoDate = true, isTransformerIncremental = false, inferSchema = false, csvSchema = csvWithInfoDateSchema)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedWithInfoDate1)
      compareText(actualTable2Before, expectedWithInfoDate1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, csv2WithInfoDateStr)

      val exitCode2 = AppRunner.runPipeline(conf)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedWithInfoDateAll)
      compareText(actualTable2After, expectedWithInfoDateAll)
    }
    succeed
  }

  def testWithInfoDateRerun(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      fsUtils.writeFile(path1, csv1WithInfoDateStr)

      val conf = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedWithInfoDate1)
      compareText(actualTable2Before, expectedWithInfoDate1)

      fsUtils.deleteFile(path1)
      fsUtils.writeFile(path2, csv2WithInfoDateStr)

      val conf2 = getConfig(tempDir, metastoreFormat, isRerun = true, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 1)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      // Expecting original records
      compareText(actualTable1After, expectedWithInfoDate2)
      compareText(actualTable2After, expectedWithInfoDate2)
    }
    succeed
  }

  def testWithInfoDateHistoricalDateRange(metastoreFormat: String): Assertion = {
    val csv1Str = s"id,name,info_date\n0,Old,${infoDate.minusDays(1)}\n1,John,$infoDate\n2,Jack,$infoDate\n3,Jill,$infoDate\n99,New,${infoDate.plusDays(2)}\n"
    val csv2Str = s"id,name,info_date\n4,Mary,${infoDate.plusDays(1)}\n5,Jane,${infoDate.plusDays(1)}\n6,Kate,${infoDate.plusDays(1)}\n999,New2,${infoDate.plusDays(2)}\n"

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

      fsUtils.writeFile(path1, csv1Str)
      fsUtils.writeFile(path2, csv2Str)

      val conf1 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)

      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedWithInfoDate1)
      compareText(actualTable2Before, expectedWithInfoDate1)

      val conf2 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, isHistoricalRun = true, historyRunMode = "fill_gaps", useInfoDate = infoDate.plusDays(1), inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After1 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable1After2 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val dfTable2After2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.plusDays(1)))
      val actualTable1After1 = dfTable1After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After1 = dfTable2After1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable1After2 = dfTable1After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After2 = dfTable2After2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      val batchIdsOld = dfTable1After1.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))
      val batchIdsNew = dfTable1After2.select(BATCH_ID_COLUMN).distinct().collect().map(_.getLong(0))

      assert(batchIdsOld.length == 1)
      assert(batchIdsNew.length == 1)

      // The batch id for all info dates in range should not be the same since they we ran only for missing data
      assert(batchIdsOld.head != batchIdsNew.head)

      // Expecting empty records
      compareText(actualTable1After1, expectedWithInfoDate1)
      compareText(actualTable2After1, expectedWithInfoDate1)
      compareText(actualTable1After2, expectedWithInfoDate2)
      compareText(actualTable2After2, expectedWithInfoDate2)
    }
    succeed
  }

  def testWithTimestampNormalRun(metastoreFormat: String): Assertion = {
    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1Csv = new Path(tempDir, new Path("landing_csv", "landing_file1.csv"))
      val path1Parquet = new Path(tempDir, "landing")
      fsUtils.writeFile(path1Csv, csv1WithTimestampStr)

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path1Csv.toString)
        .withColumn("ts", (col("tss") / 1000).cast(TimestampType))

      df.write.parquet(path1Parquet.toString)

      val conf = getConfig(tempDir, metastoreFormat, hasInfoDate = true, resource = "/test/config/incremental_pipeline_ts.conf")

      val exitCode1 = AppRunner.runPipeline(conf)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString)
      val actualTable1Before = dfTable1Before.select("ts", "name", INFO_DATE_COLUMN).orderBy("ts").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("ts", "name", INFO_DATE_COLUMN).orderBy("ts").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedWithTimestamp1)
      compareText(actualTable2Before, expectedWithTimestamp1)

      val conf2 = getConfig(tempDir, metastoreFormat, isTransformerIncremental = false, hasInfoDate = true, useInfoDate = infoDate.plusDays(1), resource = "/test/config/incremental_pipeline_ts.conf")
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1After = spark.read.format(metastoreFormat).load(table1Path.toString)
      val dfTable2After = spark.read.format(metastoreFormat).load(table2Path.toString)

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val actualTable1After = dfTable1After.select("ts", "name", INFO_DATE_COLUMN).orderBy("ts").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("ts", "name", INFO_DATE_COLUMN).orderBy("ts").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedWithTimestamp2)
      compareText(actualTable2After, expectedWithTimestamp2)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      val offsets1 = om.getOffsets("table1", infoDate.minusDays(1)).map(_.asInstanceOf[CommittedOffset])
      assert(offsets1.isEmpty)
      assert(offsets1.isEmpty)

      val offsets2 = om.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset])
      assert(offsets2.length == 1)
      assert(offsets2.head.minOffset.valueString.toLong == 1613639398123L)
      assert(offsets2.head.maxOffset.valueString.toLong == 1613639399123L)

      val offsets3 = om.getOffsets("table1", infoDate.plusDays(1)).map(_.asInstanceOf[CommittedOffset])
      assert(offsets3.length == 1)
      assert(offsets3.head.minOffset.valueString.toLong == 1613740330000L)
      assert(offsets3.head.maxOffset.valueString.toLong == 1613740330000L)
    }
    succeed
  }

  def testOffsetCrossInfoDateEdgeCase(metastoreFormat: String): Assertion = {
    val expected =
      """{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |""".stripMargin

    val csv1Str = s"id,name,info_date\n0,Old,${infoDate.minusDays(2)}\n1,John,${infoDate.minusDays(1)}\n2,Jack,${infoDate.minusDays(1)}\n3,Jill,$infoDate\n4,Mary,$infoDate\n"

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))

      fsUtils.writeFile(path1, csv1Str)

      val conf1 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = "id int,name string,info_date date")

      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")
      val dfTable1_1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable2_1 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate.minusDays(1)))
      val dfTable1_2 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2_2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val actualTable1_2 = dfTable1_2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2_2 = dfTable2_2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      assert(dfTable1_1.isEmpty)
      assert(dfTable2_1.isEmpty)
      compareText(actualTable1_2, expected)
      compareText(actualTable2_2, expected)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      val offsets1 = om.getOffsets("table1", infoDate.minusDays(1)).map(_.asInstanceOf[CommittedOffset])
      assert(offsets1.isEmpty)

      val offsets2 = om.getOffsets("table1", infoDate).map(_.asInstanceOf[CommittedOffset])
      assert(offsets2.length == 1)
      assert(offsets2.head.minOffset.valueString.toLong == 3)
      assert(offsets2.head.maxOffset.valueString.toLong == 4)
    }
    succeed
  }

  def testTransformerPicksUpFromDoubleIngestedData(metastoreFormat: String): Assertion = {
    val csv1DataStr = s"id,name,info_date\n1,John,$infoDate\n2,Jack,$infoDate\n"
    val csv2DataStr = s"id,name,info_date\n3,Jill,$infoDate\n4,Mary,$infoDate\n"
    val csv3DataStr = s"id,name,info_date\n5,Jane,$infoDate\n6,Kate,$infoDate\n"

    val expectedStr1: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |""".stripMargin

    val expectedStr2: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |""".stripMargin

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      val path3 = new Path(tempDir, new Path("landing", "landing_file3.csv"))

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")

      fsUtils.writeFile(path1, csv1DataStr)
      val conf1 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      fsUtils.writeFile(path2, csv2DataStr)
      val conf2 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema, isTransformerDisabled = true)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1Before = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2Before = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1Before = dfTable1Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2Before = dfTable2Before.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1Before, expectedStr2)
      compareText(actualTable2Before, expectedStr1)

      fsUtils.writeFile(path3, csv3DataStr)

      val conf3 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode3 = AppRunner.runPipeline(conf3)
      assert(exitCode3 == 0)

      val dfTable1After = spark.read.parquet(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2After = spark.read.parquet(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))

      val batchIds = dfTable1After.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 3)

      val actualTable1After = dfTable1After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2After = dfTable2After.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1After, expectedWithInfoDateAll)
      compareText(actualTable2After, expectedWithInfoDateAll)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      val offsets = om.getOffsets("table1->table2", infoDate).map(_.asInstanceOf[CommittedOffset])
      assert(offsets.length == 1)
    }
    succeed
  }

  def testNormalRunAfterRerun(metastoreFormat: String): Assertion = {
    val csv1DataStr = s"id,name,info_date\n1,John,$infoDate\n2,Jack,$infoDate\n"
    val csv2DataStr = s"id,name,info_date\n3,Jill,$infoDate\n4,Mary,$infoDate\n"

    val expectedStr1: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |""".stripMargin

    val expectedStr2: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |""".stripMargin

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")

      fsUtils.writeFile(path1, csv1DataStr)
      val conf1 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema, isRerun = true)
      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      fsUtils.writeFile(path2, csv2DataStr)
      val conf2 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      val dfTable1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2 = dfTable2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1, expectedStr2)
      compareText(actualTable2, expectedStr2)

      val batchIds = dfTable1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      val offsets = om.getOffsets("table1->table2", infoDate).map(_.asInstanceOf[CommittedOffset])
      assert(offsets.length == 1)
    }
    succeed
  }

  def testNormalRunAfterRerunAfterNormalRun(metastoreFormat: String): Assertion = {
    val csv1DataStr = s"id,name,info_date\n1,John,$infoDate\n2,Jack,$infoDate\n"
    val csv2DataStr = s"id,name,info_date\n3,Jill,$infoDate\n4,Mary,$infoDate\n"
    val csv3DataStr = s"id,name,info_date\n5,Jane,$infoDate\n6,Kate,$infoDate\n"

    val expectedStr: String =
      """{"id":1,"name":"John"}
        |{"id":2,"name":"Jack"}
        |{"id":3,"name":"Jill"}
        |{"id":4,"name":"Mary"}
        |{"id":5,"name":"Jane"}
        |{"id":6,"name":"Kate"}
        |""".stripMargin

    withTempDirectory("incremental1") { tempDir =>
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

      val path1 = new Path(tempDir, new Path("landing", "landing_file1.csv"))
      val path2 = new Path(tempDir, new Path("landing", "landing_file2.csv"))
      val path3 = new Path(tempDir, new Path("landing", "landing_file3.csv"))

      val table1Path = new Path(tempDir, "table1")
      val table2Path = new Path(tempDir, "table2")

      fsUtils.writeFile(path1, csv1DataStr)
      val conf1 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode1 = AppRunner.runPipeline(conf1)
      assert(exitCode1 == 0)

      fsUtils.writeFile(path2, csv2DataStr)
      val conf2 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema, isRerun = true)
      val exitCode2 = AppRunner.runPipeline(conf2)
      assert(exitCode2 == 0)

      fsUtils.writeFile(path3, csv3DataStr)
      val conf3 = getConfig(tempDir, metastoreFormat, hasInfoDate = true, inferSchema = false, csvSchema = csvWithInfoDateSchema)
      val exitCode3 = AppRunner.runPipeline(conf3)
      assert(exitCode3 == 0)

      val dfTable1 = spark.read.format(metastoreFormat).load(table1Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val dfTable2 = spark.read.format(metastoreFormat).load(table2Path.toString).filter(col(INFO_DATE_COLUMN) === Date.valueOf(infoDate))
      val actualTable1 = dfTable1.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")
      val actualTable2 = dfTable2.select("id", "name").orderBy("id").toJSON.collect().mkString("\n")

      compareText(actualTable1, expectedStr)
      compareText(actualTable2, expectedStr)

      val batchIds = dfTable1.select(BATCH_ID_COLUMN).distinct().collect()

      assert(batchIds.length == 2)

      val om = new OffsetManagerJdbc(pramenDb.db, 123L)

      val offsets = om.getOffsets("table1->table2", infoDate).map(_.asInstanceOf[CommittedOffset])
      assert(offsets.length == 1)
    }
    succeed
  }

  def getConfig(basePath: String,
                metastoreFormat: String,
                isRerun: Boolean = false,
                useFileList: Boolean = false,
                isTransformerIncremental: Boolean = true,
                isTransformerDisabled: Boolean = false,
                isHistoricalRun: Boolean = false,
                historyRunMode: String = "force",
                inferSchema: Boolean = true,
                csvSchema: String = "id int,name string",
                hasInfoDate: Boolean = false,
                useInfoDate: LocalDate = infoDate,
                resource: String = "/test/config/incremental_pipeline.conf"): Config = {
    val configContents = ResourceUtils.getResourceString(resource)
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
           |use.file.list = $useFileList
           |pramen.runtime.is.rerun = $isRerun
           |pramen.current.date = "$useInfoDate"
           |transformer.schedule = "$transformerSchedule"
           |transformer.disabled = "$isTransformerDisabled"
           |infer.schema = $inferSchema
           |$historicalConfigStr
           |has.information.date.column = $hasInfoDate
           |metastore.format = $metastoreFormat
           |csv.schema = "$csvSchema"
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
