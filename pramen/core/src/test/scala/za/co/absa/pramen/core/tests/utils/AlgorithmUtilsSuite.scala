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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.AlgorithmUtils._

class AlgorithmUtilsSuite extends AnyWordSpec {
  private val log = LoggerFactory.getLogger(this.getClass)

  "findDuplicates" should {
    "work for an empty list" in {
      assert(findDuplicates(Seq.empty).isEmpty)
    }

    "work for a single element list" in {
      val actual = findDuplicates(Seq("a"))

      assert(actual.isEmpty)
    }

    "work for 2 element list" in {
      val actual = findDuplicates(Seq("a", "a"))

      assert(actual.length == 1)
      assert(actual.head == "a")
    }

    "retain the original order" in {
      val actual = findDuplicates(Seq("b", "a", "c", "b", "a", "d", "A", "B"))

      assert(actual.length == 4)
      assert(actual == Seq("b", "a", "A", "B"))
    }
  }

  "AlgorithmUtils.findCycle" should {
    "return an empty list if the input is empty" in {
      val m = Seq.empty[(String, String)]
      assert(findCycle(m).isEmpty)
    }

    "return an empty list if there are no cycles in the list" in {
      val m = Seq[(String, String)]("A" -> "B", "B" -> "C", "X" -> "A", "Y" -> "A")
      assert(findCycle(m).isEmpty)
    }

    "return a list of fields for a trivial self-reference cycle" in {
      val m = Seq[(String, String)]("A" -> "B", "C" -> "C")
      assert(findCycle(m) == "C" :: "C" :: Nil)
    }

    "return a list of fields for a multiple fields cycle chain self-reference cycle" in {
      val m = Seq[(String, String)]("A" -> "B", "B" -> "C", "C" -> "D", "D" -> "A")
      val cycle = findCycle(m)

      // Due the nature of HashMap the cycle elements can start from any cycle element
      assert(cycle.contains("A"))
      assert(cycle.contains("B"))
      assert(cycle.contains("C"))
      assert(cycle.contains("D"))
      assert(cycle.head == cycle.last)
    }

    "return a cycle part of a path that contains a cycle" in {
      val m = Seq[(String, String)]("0" -> "A", "A" -> "B", "B" -> "C", "C1" -> "C", "C" -> "D", "D1" -> "E", "D" -> "B")
      val cycle = findCycle(m)

      // Due the nature of HashMap the cycle elements can start from any cycle element
      assert(cycle.contains("B"))
      assert(cycle.contains("C"))
      assert(cycle.contains("D"))
      assert(cycle.head == cycle.last)
    }
  }

  "actionWithRetry" should {
    "work when it works on the first attempt" in {
      var attempts = 0
      actionWithRetry(3, log) {
        attempts += 1
      }

      assert(attempts == 1)
    }

    "work when it works on the second attempt" in {
      var attempt = 0
      actionWithRetry(3, log) {
        attempt += 1
        if (attempt == 1) {
          throw new RuntimeException("test")
        }
      }

      assert(attempt == 2)
    }

    "fail when out of attempts" in {
      var attempt = 0
      val ex = intercept[RuntimeException] {
        actionWithRetry(2, log) {
          attempt += 1
          throw new RuntimeException("test")
        }
      }

      assert(attempt == 2)
      assert(ex.getMessage == "test")
    }

    "fail when out of attempts with an exception with a cause" in {
      var attempt = 0
      val ex = intercept[RuntimeException] {
        actionWithRetry(2, log) {
          attempt += 1
          throw new RuntimeException("test", new RuntimeException("test1"))
        }
      }

      assert(attempt == 2)
      assert(ex.getMessage == "test")
      assert(ex.getCause.getMessage == "test1")
    }
  }

}
