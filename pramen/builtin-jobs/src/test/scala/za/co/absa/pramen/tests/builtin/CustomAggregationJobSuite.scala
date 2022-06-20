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

package za.co.absa.pramen.tests.builtin

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.JobDependency
import za.co.absa.pramen.api.schedule.Monthly
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.builtin.CustomAggregationJob
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.mocks.{AppContextMock, DummyAggregationJob}

import java.time.LocalDate

class CustomAggregationJobSuite extends WordSpec with SparkTestBase {
  "Monthly aggregation job" should {
    "be properly read from config" in {
      AppContextMock.initAppContext()

      val conf = ConfigFactory.parseResources("test/monthly_aggregation_job.conf")

      val job = CustomAggregationJob(conf, spark)

      assert(job.name == "Job Name")
      assert(job.getUnderlyingJob.isInstanceOf[DummyAggregationJob])
      assert(job.getDependencies == List(JobDependency(List("table1", "table2"), "output.table.name")))
      assert(job.transformOutputInfoDate(LocalDate.of(2020, 11, 2)) == LocalDate.of(2020, 10, 31))
      assert(job.getSchedule == Monthly(Seq(2)))

      AppContextFactory.close()
    }
  }
}
