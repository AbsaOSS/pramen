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
import za.co.absa.pramen.api.schedule.EveryDay

class SinkContextSuite extends WordSpec {
  "fromConfig" should {
    "create a context from config" in {
      val conf = ConfigFactory.parseString(
        """ pramen {
          |   sinks = [ ]
          | }
          |
          |pramen.sink {
          |  job.name = "MyJob"
          |  sink = "kafka1"
          |
          |  schedule.type = "daily"
          |
          |  tables = [
          |    {
          |       input.metastore.table = table11
          |       output.topic.name = "topic1"
          |    },
          |    {
          |       input.metastore.table = table22
          |    }
          |  ]
          |}
          |""".stripMargin)

      val sourcingContext = SinkContext.fromConfig(conf, "pramen.sink")

      assert(sourcingContext.sinkName == "kafka1")
      assert(sourcingContext.schedule.isInstanceOf[EveryDay])
      assert(sourcingContext.tables.length == 2)
      assert(sourcingContext.jobName == "MyJob")
      assert(sourcingContext.tables.head.options.get("topic.name").contains("topic1"))
    }
  }
}
