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

package za.co.absa.pramen.core.tests.utils.hive

import org.scalatest.WordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.utils.hive.{HiveHelper, HiveHelperImpl}

class HiveHelperSuite extends WordSpec with SparkTestBase {
  "HiveHelper" should {
    "create a default instance of HiveHelper" in {
      val hiveHelper = HiveHelper(spark)

      assert(hiveHelper != null)
      assert(hiveHelper.isInstanceOf[HiveHelperImpl])
    }
  }
}
