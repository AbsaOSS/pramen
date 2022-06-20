/*
 * Copyright 2020 ABSA Group Limited
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

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.TableDataFrame
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.mocks.AppContextMock
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.LocalDate

class IngestionJobSuite extends WordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  "do an ingestion from a given source" in {
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

    val dfIn = List((1, "a", "2021-10-12"), (2, "b", "2021-10-12"), (3, "c", "2021-10-12")).toDF("A", "B", "INFO_DATE")

    val conf = ConfigFactory.parseString(
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
         | pramen.sources = [
         |  {
         |    name = "parquet1"
         |    factory.class = "za.co.absa.pramen.framework.source.ParquetSource"
         |
         |    has.information.date.column = true
         |    information.date.column = "INFO_DATE"
         |    information.date.app.format = "yyyy-MM-dd"
         |  }
         | ]
         |${IngestionJob.PREFIX} {
         |  job.name = "Ingestion"
         |
         |  schedule.type = "daily"
         |
         |  source = "parquet1"
         |
         |  tables = [
         |    {
         |      input.path = "/dummy/path2"
         |      output.metastore.table = table1
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

    val job = IngestionJob(conf, spark)

    assert(job.isInstanceOf[IngestionJob])
    assert(job.name == "Ingestion")

    val dfWithSchemaTransformed = job.schemaTransformation(TableDataFrame("table1", dfIn), infoDate)
    val dfOut = job.runTask(TableDataFrame("table1", dfWithSchemaTransformed), infoDate, infoDate, infoDate)

    val actual = SparkUtils.dataFrameToJson(dfOut.orderBy("A"))

    compareText(actual, expected)

    AppContextFactory.close()
  }
}
