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

package za.co.absa.pramen.api.sql

import org.scalatest.wordspec.AnyWordSpec

class QuotingPolicySuite extends AnyWordSpec {
  "QuotingPolicy.fromString" should {
    "return Never for 'never'" in {
      assert(QuotingPolicy.fromString("never") == QuotingPolicy.Never)
      assert(QuotingPolicy.fromString("never").name == "never")
    }

    "return Always for 'always'" in {
      assert(QuotingPolicy.fromString("always") == QuotingPolicy.Always)
      assert(QuotingPolicy.fromString("always").name == "always")
    }

    "return Auto for 'auto'" in {
      assert(QuotingPolicy.fromString("auto") == QuotingPolicy.Auto)
      assert(QuotingPolicy.fromString("auto").name == "auto")
    }

    "throw an exception for an unknown quoting policy" in {
      assertThrows[IllegalArgumentException] {
        QuotingPolicy.fromString("unknown")
      }
    }
  }
}
