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

package za.co.absa.pramen.core.app.config

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.runner.splitter.RunMode

class RunModeSuite extends AnyWordSpec {
  "RunMode.fromString()" should {
    "correctly convert 'fill_gaps'" in {
      assert(RunMode.fromString("fill_gaps") == RunMode.SkipAlreadyRan)
    }

    "correctly convert 'check_updates'" in {
      assert(RunMode.fromString("check_updates") == RunMode.CheckUpdates)
    }

    "correctly convert 'force'" in {
      assert(RunMode.fromString("force") == RunMode.ForceRun)
    }

    "throw an exception when given an invalid string" in {
      assertThrows[IllegalArgumentException](RunMode.fromString("invalid"))
    }
  }

  "RunMode.toString()" should {
    "correctly convert 'fill_gaps'" in {
      assert(RunMode.SkipAlreadyRan.toString == "fill_gaps")
    }

    "correctly convert 'check_updates'" in {
      assert(RunMode.CheckUpdates.toString == "check_updates")
    }

    "correctly convert 'force'" in {
      assert(RunMode.ForceRun.toString == "force")
    }
  }
}
