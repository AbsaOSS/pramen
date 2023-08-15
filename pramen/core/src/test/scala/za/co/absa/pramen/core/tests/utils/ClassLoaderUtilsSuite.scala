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

package za.co.absa.pramen.core.tests.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Transformer
import za.co.absa.pramen.core.mocks.transformer._
import za.co.absa.pramen.core.mocks.{DummySingletonClass, DummySingletonFactory, DummySingletonTrait}
import za.co.absa.pramen.core.utils.ClassLoaderUtils

class ClassLoaderUtilsSuite extends AnyWordSpec{
  val entityConfig: Config = ConfigFactory.parseString(
    """
      | name = "test"
      | key1 = "value1"
      |""".stripMargin)

  val appConfig: Config = ConfigFactory.parseString(
    """ app = "app name"
      | entities = [
      |   {
      |     name = "test"
      |     key1 = "value1"
      |   }
      | ]
      |""".stripMargin)

  "loadSingletonClassOfType" should {
    "be able to create to create an singleton" in {
      val singletonFactory = ClassLoaderUtils.loadSingletonClassOfType[DummySingletonFactory[DummySingletonTrait]](
        "za.co.absa.pramen.core.mocks.DummySingletonClass")

      assert(singletonFactory.isInstanceOf[DummySingletonFactory[DummySingletonTrait]])

      val constructedObject = singletonFactory.apply("dummy")

      assert(constructedObject.isInstanceOf[DummySingletonClass])
      assert(constructedObject.asInstanceOf[DummySingletonClass].dummyParam == "dummy")
    }

    "fail if the class is not found" in {
      val ex = intercept[IllegalArgumentException] {
        ClassLoaderUtils.loadSingletonClassOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.core.mocks.NonExistentSingletonClass")
      }

      assert(ex.getMessage.contains("could not be found"))
    }

    "fail if the class is not a singleton" in {
      val ex = intercept[IllegalArgumentException] {
        ClassLoaderUtils.loadSingletonClassOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.core.mocks.transformer.DummyTransformer1")
      }

      assert(ex.getMessage.contains("is not a singleton"))
    }

    "fail if the class is a proper instance of the trait" in {
      val ex = intercept[IllegalArgumentException] {
        ClassLoaderUtils.loadSingletonClassOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.core.mocks.DummyEmptySingletonClass")
      }

      assert(ex.getMessage.contains("is not an instance of"))
    }
  }

  "loadConfigurableClass" should {
    "be able to create a class without constructor parameters" in {
      val transformer = ClassLoaderUtils.loadConfigurableClass[Transformer]("za.co.absa.pramen.core.mocks.transformer.DummyTransformer1", null)

      assert(transformer.isInstanceOf[DummyTransformer1])
    }

    "be able to create a class without constructor with an entity config" in {
      val transformer = ClassLoaderUtils.loadConfigurableClass[Transformer]("za.co.absa.pramen.core.mocks.transformer.DummyTransformer2", entityConfig)

      assert(transformer.isInstanceOf[DummyTransformer2])
      assert(transformer.asInstanceOf[DummyTransformer2].transformerConfig.getString("key1") == "value1")
    }
  }

  "loadEntityConfigurableClass" should {
    "be able to create a class without constructor parameters" in {
      val transformer = ClassLoaderUtils.loadEntityConfigurableClass[Transformer]("za.co.absa.pramen.core.mocks.transformer.DummyTransformer1", null, null)

      assert(transformer.isInstanceOf[DummyTransformer1])
    }

    "be able to create a class without constructor with an entity config" in {
      val transformer = ClassLoaderUtils.loadEntityConfigurableClass[Transformer]("za.co.absa.pramen.core.mocks.transformer.DummyTransformer2", entityConfig, null)

      assert(transformer.isInstanceOf[DummyTransformer2])
      assert(transformer.asInstanceOf[DummyTransformer2].transformerConfig.getString("key1") == "value1")
    }

    "be able to create a class without constructor with entity and configs" in {
      val transformer = ClassLoaderUtils.loadEntityConfigurableClass[Transformer]("za.co.absa.pramen.core.mocks.transformer.DummyTransformer3", entityConfig, appConfig)

      assert(transformer.isInstanceOf[DummyTransformer3])
      assert(transformer.asInstanceOf[DummyTransformer3].transformerConfig.getString("key1") == "value1")
      assert(transformer.asInstanceOf[DummyTransformer3].appConfig.getString("app") == "app name")
    }
  }
}
