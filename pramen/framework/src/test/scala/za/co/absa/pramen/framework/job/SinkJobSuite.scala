/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec
import za.co.absa.pramen.api.metastore.{MetaTable, MetaTableStats, Metastore, MetastoreReader}
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.mocks.job.SinkSpy
import za.co.absa.pramen.framework.mocks.metastore.MetastoreReaderMock
import za.co.absa.pramen.framework.mocks.{AppContextMock, MetaTableFactory}
import za.co.absa.pramen.framework.sink.SinkManager
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.LocalDate

class SinkJobSuite extends WordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  "send data to a sink from a metastore table" in {
    val expected =
      """[ {
        |  "A" : 1,
        |  "INFO_DATE" : "2021-10-12",
        |  "c" : 3,
        |  "d" : 3.0
        |}, {
        |  "A" : 2,
        |  "INFO_DATE" : "2021-10-12",
        |  "c" : 4,
        |  "d" : 4.0
        |}, {
        |  "A" : 3,
        |  "INFO_DATE" : "2021-10-12",
        |  "c" : 5,
        |  "d" : 5.0
        |} ]""".stripMargin

    val infoDate = LocalDate.of(2021, 10, 12)

    val metastore = getMetastoreMock

    implicit val conf: Config = ConfigFactory.parseString(
      s"""
         | pramen.metastore {
         |   tables = [
         |    {
         |      name = "table1"
         |      format = "parquet"
         |      path = "/dummy/path1"
         |    }
         |  ]
         | }
         | pramen.sinks = [
         |  {
         |    name = "mysink"
         |    factory.class = "za.co.absa.pramen.framework.mocks.job.SinkSpy"
         |
         |    custom.config = "myconfig"
         |  }
         | ]
         |${SinkJob.PREFIX} {
         |  job.name = "SinkJobSuite"
         |
         |  schedule.type = "daily"
         |
         |  sink = "mysink"
         |
         |  tables = [
         |    {
         |      input.metastore.table = table1
         |
         |      specialOption = "success"
         |
         |      transformations = [
         |        { col="c", expr="a+2" },
         |        { col="d", expr="cast(c as decimal(10,4))" },
         |        { col="e", expr="2" }
         |        { col="e", expr="drop" }
         |        { col="b", expr="" }
         |      ]
         |    }
         |  ]
         |}
         |""".stripMargin)

    AppContextMock.initAppContext(Some(conf))

    val sc = SinkContext.fromConfig(conf, SinkJob.PREFIX)
    val sink = SinkManager.getSinkByName(sc.sinkName, conf, None).asInstanceOf[SinkSpy]

    val job = new SinkJob("SinkJobSuite",
      sc.schedule,
      metastore,
      (_: SinkTable) => sink,
      sc.sinkName,
      sc.tables)

    assert(job.isInstanceOf[SinkJob])
    assert(job.name == "SinkJobSuite")
    assert( job.getDependencies.length == 1)
    assert( job.getDependencies.head.inputTables.length == 1)

    val inputMetastoreTable = job.getDependencies.head.inputTables.head
    val outputMetastoreTable = job.getDependencies.head.outputTable

    assert(inputMetastoreTable == "table1")
    assert(outputMetastoreTable == "table1->mysink")

    val countOpt = job.runTask(Seq("table1"), infoDate, infoDate, infoDate)

    val dfOut = sink.dfs.head

    val actual = SparkUtils.dataFrameToJson(dfOut.orderBy("A"))

    assert(countOpt.contains(3))
    assert(sink.connectCalled == 1)
    assert(sink.closeCalled == 1)
    assert(sink.dfs.length == 1)

    compareText(actual, expected)

    AppContextFactory.close()
  }

  def getMetastoreMock: Metastore = {
    val dfIn = List((1, "a", "2021-10-12"), (2, "b", "2021-10-12"), (3, "c", "2021-10-12")).toDF("A", "B", "INFO_DATE")

    val reader = new MetastoreReaderMock(Seq("table1" -> dfIn, "table2" -> dfIn), LocalDate.of(2021, 10, 12))
    new Metastore {
      override def getRegisteredTables: Seq[String] = Seq("table1", "table2")

      override def getRegisteredMetaTables: Seq[MetaTable] = Seq(
        MetaTableFactory.getDummyMetaTable("table1"),
        MetaTableFactory.getDummyMetaTable("table2")
      )

      override def isTableAvailable(tableName: String, infoDate: LocalDate): Boolean = true

      override def isDataAvailable(tableName: String, infoDateFromOpt: Option[LocalDate], infoDateToOpt: Option[LocalDate]): Boolean = true

      override def getTableDef(tableName: String): MetaTable = null

      override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = reader.getTable(tableName, infoDateFrom, infoDateTo)

      override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = reader.getLatest(tableName, until)

      override def getReader(tableName: String): TableReader = null

      override def getWriter(tableName: String): TableWriter = null

      override def saveTable(tableName: String, infoDate: LocalDate, df: DataFrame, inputRecordCount: Option[Long] = None): MetaTableStats = null

      override def getStats(tableName: String, infoDate: LocalDate): MetaTableStats = MetaTableStats(0, None)

      override def getMetastoreReader(tables: Seq[String], infoDate: LocalDate): MetastoreReader = reader
    }
  }
}
