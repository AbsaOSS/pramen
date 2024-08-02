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

package za.co.absa.pramen.core.tests.runner

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.{doAnswer, mock, when => whenMock}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.config.Keys._
import za.co.absa.pramen.core.runner.PipelineSparkSessionBuilder

class PipelineSparkSessionBuilderSuite extends AnyWordSpec {
  "applyHadoopConfig" should {
    "do nothing for an empty hadoop config" in {
      val conf = ConfigFactory.empty()

      val sparkMock = mock(classOf[SparkSession])

      PipelineSparkSessionBuilder.applyHadoopConfig(sparkMock, conf)
    }

    "apply the given config to the spark session" in {
      val conf = ConfigFactory.parseString(
        s"""$HADOOP_REDACT_TOKENS = [ secret ]
           |$HADOOP_OPTION_PREFIX {
           |  fs.s3a.aws.credentials.provider = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
           |  fs.s3a.secret.key = "acs123"
           |}
           |""".stripMargin
      )

      val sparkMock = mock(classOf[SparkSession])
      val scMock = mock(classOf[SparkContext])
      val hcMock = mock(classOf[Configuration])

      whenMock(sparkMock.sparkContext).thenReturn(scMock)
      whenMock(scMock.hadoopConfiguration).thenReturn(hcMock)

      var key1Set = false
      var key2Set = false

      doAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          key1Set = true
        }
      }).when(hcMock).set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

      doAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          key2Set = true
        }
      }).when(hcMock).set("fs.s3a.secret.key", "acs123")

      PipelineSparkSessionBuilder.applyHadoopConfig(sparkMock, conf)

      assert(key1Set)
      assert(key2Set)
    }

    "apply the given config to the spark session version 2" in {
      val conf = ConfigFactory.parseString(
        s"""$HADOOP_REDACT_TOKENS = [ secret ]
           |$HADOOP_OPTION_PREFIX_V2 {
           |  fs.s3a.aws.credentials.provider = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
           |  fs.s3a.secret.key = "acs123"
           |}
           |""".stripMargin
      )

      val sparkMock = mock(classOf[SparkSession])
      val scMock = mock(classOf[SparkContext])
      val hcMock = mock(classOf[Configuration])

      whenMock(sparkMock.sparkContext).thenReturn(scMock)
      whenMock(scMock.hadoopConfiguration).thenReturn(hcMock)

      var key1Set = false
      var key2Set = false

      doAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          key1Set = true
        }
      }).when(hcMock).set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

      doAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          key2Set = true
        }
      }).when(hcMock).set("fs.s3a.secret.key", "acs123")

      PipelineSparkSessionBuilder.applyHadoopConfig(sparkMock, conf)

      assert(key1Set)
      assert(key2Set)
    }
  }

  "getSparkAppName" should {
    "use the name configured when available" in {
      val conf = ConfigFactory.parseString("""pramen.spark.app.name = "My App"""")

      val appName = PipelineSparkSessionBuilder.getSparkAppName(conf)

      assert(appName == "My App")
    }

    "allow substitutions in spark app name" in {
      val conf = ConfigFactory.parseString(
        """pramen {
          |  pipeline.name = "Data sourcing"
          |  spark.app.name = "My App - "${pramen.pipeline.name}
          |}
          |""".stripMargin
      ).resolve()

      val appName = PipelineSparkSessionBuilder.getSparkAppName(conf)

      assert(appName == "My App - Data sourcing")
    }

    "use pipeline name when configured" in {
      val conf = ConfigFactory.parseString("""pramen.pipeline.name = "Data sourcing"""")

      val appName = PipelineSparkSessionBuilder.getSparkAppName(conf)

      assert(appName == "Pramen - Data sourcing")
    }

    "use the default name when not configured" in {
      val conf = ConfigFactory.empty()

      val appName = PipelineSparkSessionBuilder.getSparkAppName(conf)

      assert(appName == "Pramen")
    }
  }
}
