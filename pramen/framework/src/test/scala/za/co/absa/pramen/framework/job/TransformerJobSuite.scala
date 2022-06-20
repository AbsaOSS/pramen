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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.framework.mocks.AppContextMock
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.LocalDate

class TransformerJobSuite extends WordSpec with TempDirFixture with SparkTestBase with TextComparisonFixture {

  import spark.implicits._

  "the factory" should {
    "be able to create a proper Pramen job object" in {
      withTempDirectory("transformer_test") { tempDir =>
        val expected =
          """[ {
            |  "A" : 1,
            |  "B" : "a",
            |  "c" : "a",
            |  "d" : "2021-10-12"
            |}, {
            |  "A" : 2,
            |  "B" : "b",
            |  "c" : "a",
            |  "d" : "2021-10-12"
            |}, {
            |  "A" : 3,
            |  "B" : "c",
            |  "c" : "a",
            |  "d" : "2021-10-12"
            |} ]""".stripMargin

        val infoDate = LocalDate.of(2021, 10, 12)
        List((1, "a"), (2, "b"), (3, "c")).toDF("A", "B")
          .withColumn("syncpramen_info_date", lit("2021-10-12").cast(DateType))
          .write.mode(SaveMode.Overwrite).parquet(tempDir)

        val conf = ConfigFactory.parseString(
          s"""
             | pramen.metastore {
             |   tables = [
             |    {
             |      name = "table1"
             |      format = "parquet"
             |      path = "$tempDir"
             |    },
             |    {
             |      name = "table2"
             |      format = "parquet"
             |      path = "/c/d/e"
             |    }
             |  ]
             | }
             |${TransformerJob.PREFIX} {
             |  job.name = MyJob
             |  factory.class = za.co.absa.pramen.framework.job.TransformerMock
             |  schedule.type = "daily"
             |  input.tables = [ table1, table2 ]
             |  output.table = table2
             |  output.info.date.expr = "beginOfWeek(@infoDate)"
             |  option {
             |    opt1 = a
             |  }
             |}
             |""".stripMargin)

        AppContextMock.initAppContext(Some(conf))

        val job = TransformerJob(conf, spark)

        assert(job.isInstanceOf[TransformerJob])
        assert(job.name == "MyJob")

        val df = job.runTask(Nil, infoDate, infoDate, infoDate)

        val actual = SparkUtils.dataFrameToJson(df.drop("syncpramen_info_date").orderBy("A"))

        compareText(actual, expected)

        AppContextFactory.close()
      }
    }

    "fail validation if some tables are not registered" in {
      withTempDirectory("transformer_test") { tempDir =>
        val conf = ConfigFactory.parseString(
          s"""
             | pramen.metastore {
             |   tables = [
             |    {
             |      name = "table1"
             |      format = "parquet"
             |      path = "$tempDir"
             |    },
             |    {
             |      name = "table2"
             |      format = "parquet"
             |      path = "/c/d/e"
             |    }
             |  ]
             | }
             |${TransformerJob.PREFIX} {
             |  job.name = MyJob
             |  factory.class = za.co.absa.pramen.framework.job.TransformerMock
             |  schedule.type = "daily"
             |  input.tables = [ table2, table3 ]
             |  output.table = table4
             |  output.info.date.expr = "beginOfWeek(@infoDate)"
             |  option {
             |    opt1 = a
             |  }
             |}
             |""".stripMargin)

        AppContextMock.initAppContext(Some(conf))

        val ex = intercept[IllegalArgumentException] {
          TransformerJob(conf, spark)
        }

        assert(ex.getMessage.contains("Tables not registered in the metastore: table3, table4"))

        AppContextFactory.close()
      }
    }
  }
}
