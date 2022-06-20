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

package za.co.absa.pramen.framework.tests.loader

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.loader.JobLoader
import za.co.absa.pramen.framework.mocks.job.SourceJobStub

class JobLoaderSuite extends WordSpec {

  "loadJob()" should {
    val sparkNull: SparkSession = null
    val config = ConfigFactory.parseString("dummy.option = true")

    "return None if the factory returns null" in {
      val job = JobLoader.loadJob("za.co.absa.pramen.framework.mocks.job.SourceJobDummy", config, sparkNull)

      assert(job.isEmpty)
    }

    "load a job by class name and pass the config option" in {
      val job = JobLoader.loadJob("za.co.absa.pramen.framework.mocks.job.SourceJobStub", config, sparkNull)

      assert(job.isDefined)
      assert(job.get.isInstanceOf[SourceJobStub])
      assert(job.get.asInstanceOf[SourceJobStub].dummyOption.isDefined)
      assert(job.get.asInstanceOf[SourceJobStub].dummyOption.get)
    }

    "throw an exception is class not found" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJob("za.co.absa.pramen.mocks.framework.job.NoSuchJobJob", config, sparkNull)
      }
      assert(ex.getMessage == "Class 'za.co.absa.pramen.mocks.framework.job.NoSuchJobJob' could not be found")
    }

    "throw an exception if class is not a singleton type" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJob("za.co.absa.pramen.framework.mocks.reader.ReaderStub", config, sparkNull)
      }
      assert(ex.getMessage.contains("is not a singleton"))
    }

    "throw an exception if class is not a job factory" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJob("za.co.absa.pramen.framework.mocks.dao.MongoDbSingleton", config, sparkNull)
      }
      assert(ex.getMessage.contains("is not an instance of"))
    }

    "throw an exception if the factory throws an exception" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJob("za.co.absa.pramen.framework.mocks.job.ThrowJobDummy", config, sparkNull)
      }
      assert(ex.getMessage.contains("Unable to build a job using its factory"))
      assert(ex.getCause.getMessage.contains("Factory is called"))
    }
  }

  "loadJobs()" should {
    val sparkNull: SparkSession = null
    val config = ConfigFactory.parseString("dummy.option = true")

    "throw an exception factory returns null" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJobs("za.co.absa.pramen.framework.mocks.job.SourceJobDummy" :: Nil)(config, sparkNull)
      }

      assert(ex.getMessage.contains("Unable to load Job from factory"))
    }

    "load a job by class name and pass the config option" in {
      val jobs = JobLoader.loadJobs("za.co.absa.pramen.framework.mocks.job.SourceJobStub" :: Nil)(config, sparkNull)

      assert (jobs.nonEmpty)

      val job = jobs.head

      assert(job.isInstanceOf[SourceJobStub])
      assert(job.asInstanceOf[SourceJobStub].dummyOption.isDefined)
      assert(job.asInstanceOf[SourceJobStub].dummyOption.get)
    }

    "thrown an exception if the factory returns null" in {
      val ex = intercept[IllegalArgumentException] {
        JobLoader.loadJobs("za.co.absa.pramen.framework.mocks.job.SourceJobDummy" :: Nil)(config, sparkNull)
      }

      assert(ex.getMessage.contains("Unable to load Job from factory"))
    }

  }
}
