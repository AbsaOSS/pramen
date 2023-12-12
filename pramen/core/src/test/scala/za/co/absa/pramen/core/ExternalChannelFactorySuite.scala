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

package za.co.absa.pramen.core

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.ExternalChannel
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.mocks.{ExternalChannelMock, ExternalChannelV2Mock}

class ExternalChannelFactorySuite extends AnyWordSpec with SparkTestBase {
  "fromConfig" should {
    "be able to construct a channel from factory" in {
      val conf = ConfigFactory.parseString(
        """factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMock"
          |key1 = "test1"
          |key2 = "test2"
          |""".stripMargin)

      val channel = ExternalChannelFactoryReflect.fromConfig[ExternalChannelMock](conf, conf, "", "dummy")

      assert(channel.isInstanceOf[ExternalChannelMock])
      assert(channel.value1 == "test1")
      assert(channel.value2 == "test2")
    }

    "be able to construct a channel from factory v2" in {
      val workflowConf = ConfigFactory.parseString(
        """test = "test"
          |""".stripMargin)


      val conf = ConfigFactory.parseString(
        """factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelV2Mock"
          |key1 = "test1"
          |key2 = "test2"
          |""".stripMargin)

      val channel = ExternalChannelFactoryReflect.fromConfig[ExternalChannelV2Mock](conf, workflowConf, "", "dummy")

      assert(channel.isInstanceOf[ExternalChannelV2Mock])
      assert(channel.value1 == "test1")
      assert(channel.value2 == "test2")
      assert(channel.workflowConfig.hasPath("test"))
    }

    "throw an exception if a class is not specified" in {
      val conf = ConfigFactory.parseString(
        """key1 = "test1"
          |key2 = "test2"
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfig[ExternalChannelMock](conf, conf, "", "dummy")
      }

      assert(ex.getMessage.contains("A class should be specified for the dummy"))
    }
  }

  "fromConfigByName" should {
    "be able to construct a channel from array and name" in {
      val conf = ConfigFactory.parseString(
        """channels = [
          | {
          |   name = "test_name"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMock"
          |   key1 = "test1"
          |   key2 = "test2"
          | }
          |]
          |""".stripMargin)

      val channel = ExternalChannelFactoryReflect.fromConfigByName[ExternalChannelMock](conf, None, "channels", "test_name", "dummy")

      assert(channel.isInstanceOf[ExternalChannelMock])
      assert(channel.value1 == "test1")
      assert(channel.value2 == "test2")
    }

    "be able to construct a channel from array and name v2" in {
      val conf = ConfigFactory.parseString(
        """test = "test"
          |channels = [
          | {
          |   name = "test_name"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelV2Mock"
          |   key1 = "test1"
          |   key2 = "test2"
          | }
          |]
          |""".stripMargin)

      val externalChannel = ExternalChannelFactoryReflect.fromConfigByName[ExternalChannel](conf, None, "channels", "test_name", "dummy")

      assert(externalChannel.isInstanceOf[ExternalChannelV2Mock])
      val channel = externalChannel.asInstanceOf[ExternalChannelV2Mock]
      assert(channel.value1 == "test1")
      assert(channel.value2 == "test2")
      assert(channel.workflowConfig.hasPath("test"))
      assert(channel.workflowConfig.hasPath("channels"))
      assert(channel.config.hasPath("key1"))
    }

    "be able to construct a channel with a config override" in {
      val conf1 = ConfigFactory.parseString(
        """channels = [
          | {
          |   name = "test_name"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMock"
          |   key1 = "test1"
          |   key2 = "test2"
          | }
          |]
          |""".stripMargin)

      val conf2 = ConfigFactory.parseString(
        """key1 = "test3"
          |key2 = "test2"
          |""".stripMargin)

      val channel = ExternalChannelFactoryReflect.fromConfigByName[ExternalChannelMock](conf1, Some(conf2), "channels", "test_name", "dummy")

      assert(channel.isInstanceOf[ExternalChannelMock])
      assert(channel.value1 == "test3")
      assert(channel.value2 == "test2")
    }

    "throw an exception if channel name not found" in {
      val conf = ConfigFactory.parseString(
        """channels = [
          | {
          |   name = "test_name"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMock"
          |   key1 = "test1"
          |   key2 = "test2"
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.fromConfigByName[ExternalChannelMock](conf, None, "channels", "dummy_name", "dummy type")
      }

      assert(ex.getMessage.contains("Unknown name of a data dummy type: dummy_name"))
    }
  }

  "validateConfig" should {
    "return if the config is correct" in {
      val conf = ConfigFactory.parseString(
        """channels = [
          | {
          |   name = "test_name"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMock"
          |   key1 = "test1"
          |   key2 = "test2"
          | }
          |]
          |""".stripMargin)

      ExternalChannelFactoryReflect.validateConfig(conf, "channels", "dummy")
    }

    "throw an exception if there are validation issues" in {
      val conf = ConfigFactory.parseString(
        """channels = [
          | {
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMockA"
          | },
          | {
          |   name = "test2"
          | },
          | {
          |   name = "test3"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMockB"
          | },
          | {
          |   name = "test3"
          |   factory.class = "za.co.absa.pramen.core.mocks.ExternalChannelMockC"
          | }
          |]
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        ExternalChannelFactoryReflect.validateConfig(conf, "channels", "dummy type")
      }

      assert(ex.getMessage.contains("Configuration error for a dummy type at 'channels'"))
      assert(ex.getMessage.contains("A name is not configured for 1 dummy type(s)"))
      assert(ex.getMessage.contains("Factory class is not configured for 1 dummy type(s)"))
      assert(ex.getMessage.contains("Duplicate dummy type names: test3, test3"))
    }
  }

}
