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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.CircularBuffer

class CircularBufferSuite extends AnyWordSpec {
  "add()" should {
    "be able to add a single element" in {
      val cb = new CircularBuffer[String](10)

      cb.add("A")

      assert(cb.length == 1)
      assert(cb.size == 1)
      assert(cb.get() sameElements Array[String]("A"))
    }

    "throw an exception if maxSize <= 0" in {
      intercept[IllegalArgumentException] {
        new CircularBuffer[String](0)
      }
    }
  }

  "get()" when {
    "size is 1" should {
      "be able to work when nothing is added to the array" in {
        val cb = new CircularBuffer[String](1)

        assert(cb.length == 0)
        assert(cb.size == 0)
        assert(cb.get() sameElements Array.empty[String])
      }

      "be able to work when capacity is matched" in {
        val cb = new CircularBuffer[String](1)

        cb.add("A")

        assert(cb.length == 1)
        assert(cb.size == 1)
        assert(cb.get() sameElements Array[String]("A"))
      }

      "be able to work when capacity is reached" in {
        val cb = new CircularBuffer[String](1)

        cb.add("A")
        cb.add("B")

        assert(cb.length == 1)
        assert(cb.size == 1)
        assert(cb.get() sameElements Array[String]("B"))
      }
    }

    "size is 3" should {
      "be able get the proper elements after the capacity is matched" in {
        val cb = new CircularBuffer[String](3)

        cb.add("A")
        cb.add("B")
        cb.add("C")

        assert(cb.length == 3)
        assert(cb.size == 3)
        assert(cb.get() sameElements Array[String]("A", "B", "C"))
      }

      "be able get the proper elements after the capacity is reached" in {
        val cb = new CircularBuffer[String](3)

        cb.add("A")
        cb.add("B")
        cb.add("C")
        cb.add("D")

        assert(cb.length == 3)
        assert(cb.size == 3)
        assert(cb.get() sameElements Array[String]("B", "C", "D"))
      }
    }
  }

}
