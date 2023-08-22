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

package za.co.absa.pramen.api.common

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.mocks.{DummySingletonClass, DummySingletonFactory, DummySingletonTrait}

class FactoryLoaderSuite extends AnyWordSpec {
  "loadSingletonClassOfType" should {
    "be able to create to create an singleton" in {
      val singletonFactory = FactoryLoader.loadSingletonFactoryOfType[DummySingletonFactory[DummySingletonTrait]](
        "za.co.absa.pramen.api.mocks.DummySingletonClass")

      assert(singletonFactory.isInstanceOf[DummySingletonFactory[DummySingletonTrait]])

      val constructedObject = singletonFactory.apply("dummy")

      assert(constructedObject.isInstanceOf[DummySingletonClass])
      assert(constructedObject.asInstanceOf[DummySingletonClass].dummyParam == "dummy")
    }

    "fail if the class is not found" in {
      val ex = intercept[IllegalArgumentException] {
        FactoryLoader.loadSingletonFactoryOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.api.mocks.NonExistentSingletonClass")
      }

      assert(ex.getMessage.contains("could not be found"))
    }

    "fail if the class is not a singleton" in {
      val ex = intercept[IllegalArgumentException] {
        FactoryLoader.loadSingletonFactoryOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.api.Source")
      }

      assert(ex.getMessage.contains("is not a singleton"))
    }

    "fail if the class is a proper instance of the trait" in {
      val ex = intercept[IllegalArgumentException] {
        FactoryLoader.loadSingletonFactoryOfType[DummySingletonFactory[DummySingletonTrait]](
          "za.co.absa.pramen.api.mocks.DummyEmptySingletonClass")
      }

      assert(ex.getMessage.contains("is not an instance of"))
    }
  }

}
