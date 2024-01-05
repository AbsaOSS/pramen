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

package za.co.absa.pramen.core.fixtures

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.core.InfoDateConfigFactory
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.app.{AppContext, AppContextImpl}
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, BookkeeperNull}
import za.co.absa.pramen.core.journal.{Journal, JournalNull}
import za.co.absa.pramen.core.utils.ResourceUtils

trait AppContextFixture {

  def withAppContext(spark: SparkSession,
                     confBase: Config = ConfigFactory.empty(),
                     bookkeeper: Bookkeeper = new BookkeeperNull()
                    )(f: AppContext => Unit): Unit = {
    val configStr = ResourceUtils.getResourceString("/test/config/app_context.conf")

    val testConfig = ConfigFactory.parseString(
      s"""$configStr
         |pramen.stop.spark.session = false
         |pramen.metastore.tables = [
         |    {
         |      name = "table1"
         |      format = "parquet"
         |      path = /tmp/dummy/table1
         |      records.per.partition = 1000000
         |    },
         |    {
         |      name = "table2"
         |      format = "delta"
         |      path = /tmp/dummy/table2
         |    },
         |    {
         |      name = "table3"
         |      format = "delta"
         |      path = /tmp/dummy/table_out
         |    }
         |  ]
         |""".stripMargin
    )

    val conf = testConfig
      .withFallback(confBase)
      .withFallback(ConfigFactory.load())
      .resolve()

    val infoDateConfig = InfoDateConfigFactory.getDummyInfoDateConfig()
    val journal: Journal = new JournalNull()
    val context = createMockAppContext(conf, infoDateConfig, bookkeeper, journal)(spark)

    f(context)

    context.close()
  }

  private def createMockAppContext(conf: Config,
                                   infoDateConfig: InfoDateConfig,
                                   bookkeeper: Bookkeeper,
                                   journal: Journal
                                  )(implicit spark: SparkSession): AppContext = this.synchronized {
    AppContextImpl.getMock(conf, infoDateConfig, bookkeeper, journal)
  }

}
