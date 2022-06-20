/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.mocks

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.journal.JournalNull
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.utils.ResourceUtils

object AppContextMock {
  def initAppContext(confInOpt: Option[Config] = None)(implicit spark: SparkSession): Unit = {
    val configStr = ResourceUtils.getResourceString("/test/config/app_context.conf")

    val configBase = ConfigFactory.parseString(configStr)
      .withValue("pramen.stop.spark.session", ConfigValueFactory.fromAnyRef(false))

    val conf = confInOpt match {
      case Some(confIn) =>
        confIn
          .withFallback(configBase)
          .withFallback(ConfigFactory.load())
          .resolve()
      case None =>
        configBase
          .withFallback(ConfigFactory.load())
          .resolve()
    }

    val bookkeeper = new SyncBookkeeperMock
    val journal = new JournalNull

    AppContextFactory.createMockAppContext(conf, bookkeeper, journal)
  }

}
