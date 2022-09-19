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

import org.scalatest.WordSpec
import za.co.absa.pramen.core.utils.BuildPropertyUtils

class BuildPropertyUtilsSuite extends WordSpec {
  "buildVersion" should {
    "be replaced by the current version" in {
      assert(!BuildPropertyUtils.instance.buildVersion.contains("$"))
    }
  }

  "buildTimestamp" should {
    "be replaced by the build timestamp" in {
      assert(!BuildPropertyUtils.instance.buildTimestamp.contains("$"))
    }
  }

  "getFullVersion" should {
    "be replaced by the build timestamp" in {
      assert(!BuildPropertyUtils.instance.getFullVersion.contains("$"))
    }
  }

}
