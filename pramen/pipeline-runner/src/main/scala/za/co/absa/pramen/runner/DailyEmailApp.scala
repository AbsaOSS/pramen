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

package za.co.absa.pramen.runner

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.config.WatcherConfig
import za.co.absa.pramen.framework.journal.JournalMongoDb
import za.co.absa.pramen.framework.mongo.MongoDbConnection
import za.co.absa.pramen.framework.notify.{Notification, SchemaDifference, SyncNotificationEmail}

import java.io.File
import java.time.Instant

object DailyEmailApp {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    implicit val conf: Config = getConfig(args)

    // Switch logging level to WARN
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val syncConfig: WatcherConfig = WatcherConfig.load(conf)

    val mongoDbConnection = MongoDbConnection.getConnection(syncConfig.bookkeepingConfig.bookkeepingConnectionString.get, syncConfig.bookkeepingConfig.bookkeepingDbName.get)

    val journal = new JournalMongoDb(mongoDbConnection)

    val now = Instant.now()
    val dayBefore = now.minusSeconds(24 * 60 * 60)

    val entries = journal.getEntries(dayBefore, now).toList

    val notification = Notification(None, syncConfig.ingestionName, syncConfig.environmentName,
      dayBefore, now, entries, List.empty[SchemaDifference])

    if (entries.nonEmpty) {
      new SyncNotificationEmail(notification).send()
    }

  }

  private def getConfig(args: Array[String]): Config = {
    val originalConfig = ConfigFactory.load()

    if (args.length < 1) {
      log.warn("No Pramen pipeline configuration is provided. Using the default application.conf\n")
      originalConfig
    } else {
      val confFile = args.head
      log.info(s"Loading $confFile...\n")
      ConfigFactory.parseFile(new File(confFile))
        .withFallback(originalConfig)
        .resolve()
    }
  }

}
