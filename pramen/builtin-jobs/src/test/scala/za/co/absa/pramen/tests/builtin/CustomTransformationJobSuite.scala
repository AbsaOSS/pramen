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

import java.time.{DayOfWeek, LocalDate}

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.JobDependency
import za.co.absa.pramen.api.schedule.Weekly
import za.co.absa.pramen.base.SparkTestBase
import za.co.absa.pramen.builtin.CustomTransformationJob
import za.co.absa.pramen.mocks.DummyTransformationJob

class CustomTransformationJobSuite extends WordSpec with SparkTestBase {
  "Weekly transformation job" should {
    "be properly read from config" in {
      val conf = ConfigFactory.parseResources("test/weekly_transformation_job.conf")

      val job = CustomTransformationJob(conf, spark)

      assert(job.name == "Job Name")
      assert(job.getUnderlyingJob.isInstanceOf[DummyTransformationJob])
      assert(job.getDependencies == List(JobDependency(List("employees", "check_weekly"), "output.table.name")))
      assert(job.transformOutputInfoDate(LocalDate.of(2020, 11, 12)) == LocalDate.of(2020, 11, 11))
      assert(job.getSchedule == Weekly(Seq(DayOfWeek.SUNDAY)))
    }
  }
}
