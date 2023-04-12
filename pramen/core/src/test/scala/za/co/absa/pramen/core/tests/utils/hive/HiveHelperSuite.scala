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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.metastore.model.{DataFormat, HiveConfig, HiveDefaultConfig}
import za.co.absa.pramen.core.utils.hive.{HiveHelper, HiveHelperImpl, QueryExecutorJdbc, QueryExecutorSpark}

class HiveHelperSuite extends AnyWordSpec with SparkTestBase {
  "HiveHelper" should {
    "create a default instance of HiveHelper" in {
      val hiveHelper = HiveHelper(ConfigFactory.empty())(spark)

      assert(hiveHelper != null)
      assert(hiveHelper.isInstanceOf[HiveHelperImpl])
    }
  }

  "fromHiveConfig()" should {
    "return the helper backed by Spark metastore" in {
      val hiveConfig = HiveConfig.getNullConfig

      val hiveHelper = HiveHelper.fromHiveConfig(hiveConfig).asInstanceOf[HiveHelperImpl]

      assert(hiveHelper.queryExecutor.isInstanceOf[QueryExecutorSpark])
    }

    "return the helper backed by a JDBC connection" in {
      val conf = ConfigFactory.parseString(
        """  jdbc {
          |  driver = driver
          |  url = url
          |  user = user
          |  password = pass
          |}
          |""".stripMargin)

      val hiveDefaultConfig = HiveDefaultConfig.getNullConfig

      val hiveConfig = HiveConfig.fromConfigWithDefaults(conf, hiveDefaultConfig, DataFormat.Parquet("Dummy", None))

      val hiveHelper = HiveHelper.fromHiveConfig(hiveConfig).asInstanceOf[HiveHelperImpl]

      assert(hiveHelper.queryExecutor.isInstanceOf[QueryExecutorJdbc])
    }
  }
}
