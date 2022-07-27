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

package za.co.absa.pramen.framework

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.bookkeeper.BookkeeperNull
import za.co.absa.pramen.framework.journal.JournalNull
import za.co.absa.pramen.framework.lock.TokenLockFactoryAllow
import za.co.absa.pramen.framework.utils.ResourceUtils

class AppContextFactorySuite extends WordSpec with SparkTestBase {

  "AppContextFactory" should {
    val configStr = ResourceUtils.getResourceString("/test/config/app_context.conf")

    val fullConfig =
      s"""$configStr
         |pramen.metastore {
         |  tables = [
         |    {
         |      name = "table1"
         |      format = "parquet"
         |      path = "/dummy/path"
         |      records.per.partition = 1000000
         |    }
         |  ]
         |}
         |""".stripMargin
    val configBase = ConfigFactory.parseString(fullConfig)

    val conf = configBase
      .withFallback(ConfigFactory.load())
      .withValue("pramen.stop.spark.session", ConfigValueFactory.fromAnyRef(false))
      .resolve()

    "be able to create app context from config" in {
      val context = AppContextFactory.getOrCreate(conf)

      assert(context == AppContextFactory.get)

      assert(context.bookkeeper.isInstanceOf[BookkeeperNull])
      assert(context.tokenLockFactory.isInstanceOf[TokenLockFactoryAllow])
      assert(context.journal.isInstanceOf[JournalNull])
      assert(context.metastore != null)

      context.close()
    }

    "be able to create mock app context" in {
      val bookkeeper = new BookkeeperNull()
      val journal = new JournalNull()

      val context = AppContextFactory.createMockAppContext(conf, bookkeeper, journal)

      assert(context == AppContextFactory.get)

      assert(context.bookkeeper == bookkeeper)
      assert(context.journal == journal)
      assert(context.metastore != null)

      context.close()
    }
  }
}
