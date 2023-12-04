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

package za.co.absa.pramen.core.app

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.InfoDateConfigFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.BookkeeperNull
import za.co.absa.pramen.core.journal.JournalNull
import za.co.absa.pramen.core.lock.TokenLockFactoryAllow
import za.co.absa.pramen.core.utils.ResourceUtils

class AppContextSuite extends AnyWordSpec with SparkTestBase{
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
      .resolve()

    "be able to create app context from config" in {
      val context = AppContextImpl.apply(conf)

      assert(context.bookkeeper.isInstanceOf[BookkeeperNull])
      assert(context.tokenLockFactory.isInstanceOf[TokenLockFactoryAllow])
      assert(context.journal.isInstanceOf[JournalNull])
      assert(context.metastore != null)
    }

    "be able to create mock app context" in {
      val infoDateConfig = InfoDateConfigFactory.getDummyInfoDateConfig()
      val bookkeeper = new BookkeeperNull()
      val journal = new JournalNull()

      val context = AppContextImpl.getMock(conf, infoDateConfig, bookkeeper, journal)

      assert(context.bookkeeper == bookkeeper)
      assert(context.journal == journal)
      assert(context.metastore != null)
    }
  }
}
