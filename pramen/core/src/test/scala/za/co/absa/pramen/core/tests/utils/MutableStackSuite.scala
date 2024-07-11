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
import za.co.absa.pramen.core.utils.MutableStack

class MutableStackSuite extends AnyWordSpec {
  "push()" should {
    "add an element to the stack" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      assert(stack.peek() == 1)
    }

    "add multiple elements to the stack" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      stack.push(2)
      stack.push(3)
      assert(stack.peek() == 3)
    }
  }

  "pop()" should {
    "remove the top element from the stack" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      stack.push(2)
      stack.push(3)
      assert(stack.pop().get == 3)
      assert(stack.pop().get == 2)
      assert(stack.pop().get == 1)
    }

    "return None if the stack is empty" in {
      val stack = new MutableStack[Int]
      assert(stack.pop().isEmpty)
    }
  }

  "peek()" should {
    "return the top element from the stack" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      stack.push(2)
      stack.push(3)
      assert(stack.peek() == 3)

      stack.pop()
      assert(stack.peek() == 2)

      stack.pop()
      assert(stack.peek() == 1)
    }

    "throw an exception if the stack is empty" in {
      val stack = new MutableStack[Int]
      assertThrows[NoSuchElementException] {
        stack.peek()
      }
    }
  }

  "isEmpty()" should {
    "return true if the stack is empty" in {
      val stack = new MutableStack[Int]
      assert(stack.isEmpty)
    }

    "return false if the stack is not empty" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      assert(!stack.isEmpty)
    }
  }

  "nonEmpty()" should {
    "return true if the stack is not empty" in {
      val stack = new MutableStack[Int]
      stack.push(1)
      assert(stack.nonEmpty)
    }

    "return false if the stack is empty" in {
      val stack = new MutableStack[Int]
      assert(!stack.nonEmpty)
    }
  }
}
