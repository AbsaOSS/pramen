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
import org.scalatest.WordSpec
import za.co.absa.pramen.api.schedule.EveryDay

class IngestionContextSuite extends WordSpec {
  "fromConfig" should {
    "create a context from config" in {
      val conf = ConfigFactory.parseString(
        """ pramen {
          |   sources = [ ]
          | }
          |
          |pramen.ingestion {
          |  job.name = "MyJob"
          |  source = "jdbc1"
          |
          |  schedule.type = "daily"
          |
          |  output.info.date.expr = "AAA"
          |  processing.timestamp.col = "BBB"
          |
          |  tables = [
          |    {
          |       output.metastore.table = table11
          |       input.table = table12
          |       columns = [ "a", "b", "c" ]
          |    },
          |    {
          |       output.metastore.table = table22
          |       input.sql = "SELECT * FROM X"
          |    }
          |  ]
          |}
          |""".stripMargin)

      val sourcingContext = IngestionContext.fromConfig(conf, "pramen.ingestion")

      assert(sourcingContext.sourceName == "jdbc1")
      assert(sourcingContext.schedule.isInstanceOf[EveryDay])
      assert(sourcingContext.tables.length == 2)
      assert(sourcingContext.jobName == "MyJob")
      assert(sourcingContext.outputInfoDateExpression.contains("AAA"))
      assert(sourcingContext.processingTimestampCol.contains("BBB"))
    }
  }
}
