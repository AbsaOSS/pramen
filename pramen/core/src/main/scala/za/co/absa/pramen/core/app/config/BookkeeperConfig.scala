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

import com.typesafe.config.Config
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.ConfigUtils

case class BookkeeperConfig(
                              bookkeepingEnabled: Boolean,
                              bookkeepingLocation: Option[String],
                              bookkeepingHadoopFormat: HadoopFormat,
                              bookkeepingConnectionString: Option[String],
                              bookkeepingDbName: Option[String],
                              bookkeepingJdbcConfig: Option[JdbcConfig],
                              deltaDatabase: Option[String],
                              deltaTablePrefix: Option[String]
                            )

object BookkeeperConfig {
  val BOOKKEEPING_PARENT = "pramen.bookkeeping"
  val BOOKKEEPING_ENABLED = "pramen.bookkeeping.enabled"
  val BOOKKEEPING_LOCATION = "pramen.bookkeeping.location"
  val BOOKKEEPING_HADOOP_FORMAT = "pramen.bookkeeping.hadoop.format"
  val BOOKKEEPING_CONNECTION_STRING = "pramen.bookkeeping.mongodb.connection.string"
  val BOOKKEEPING_DB_NAME = "pramen.bookkeeping.mongodb.database"
  val BOOKKEEPING_DELTA_DB_NAME = "pramen.bookkeeping.delta.database"
  val BOOKKEEPING_DELTA_TABLE_PREFIX = "pramen.bookkeeping.delta.table.prefix"

  def fromConfig(conf: Config): BookkeeperConfig = {
    val bookkeepingEnabled = conf.getBoolean(BOOKKEEPING_ENABLED)
    val bookkeepingLocation = ConfigUtils.getOptionString(conf, BOOKKEEPING_LOCATION)
    val bookkeepingHadoopFormat = HadoopFormat(conf.getString(BOOKKEEPING_HADOOP_FORMAT))
    val bookkeepingConnectionString = ConfigUtils.getOptionString(conf, BOOKKEEPING_CONNECTION_STRING)
    val bookkeepingDbName = ConfigUtils.getOptionString(conf, BOOKKEEPING_DB_NAME)
    val bookkeepingJdbcConfig = JdbcConfig.loadOption(conf.getConfig(BOOKKEEPING_PARENT), BOOKKEEPING_PARENT)
    val deltaDatabase = ConfigUtils.getOptionString(conf, BOOKKEEPING_DELTA_DB_NAME)
    val deltaTablePrefix = ConfigUtils.getOptionString(conf, BOOKKEEPING_DELTA_TABLE_PREFIX)

    if (bookkeepingEnabled && bookkeepingJdbcConfig.isEmpty && bookkeepingHadoopFormat == HadoopFormat.Delta) {
      if (bookkeepingLocation.isEmpty && deltaTablePrefix.isEmpty) {
        throw new RuntimeException(s"In order to use Delta Lake for bookkeeping, either $BOOKKEEPING_LOCATION or $BOOKKEEPING_DELTA_TABLE_PREFIX must be defined. " +
        s"Preferably $BOOKKEEPING_DELTA_DB_NAME should be defined as well for managed Delta Lake tables.")
      }
    } else {
      if (bookkeepingEnabled && bookkeepingConnectionString.isEmpty && bookkeepingLocation.isEmpty && bookkeepingJdbcConfig.isEmpty) {
        throw new RuntimeException(s"One of the following should be defined: $BOOKKEEPING_PARENT.jdbc.url, $BOOKKEEPING_CONNECTION_STRING or $BOOKKEEPING_LOCATION" +
          s" when bookkeeping is enabled. You can disable bookkeeping by setting $BOOKKEEPING_ENABLED = false.")
      }

      if (bookkeepingConnectionString.isDefined && bookkeepingDbName.isEmpty) {
        throw new RuntimeException(s"Database name is not defined. Please, define $BOOKKEEPING_DB_NAME.")
      }
    }

    BookkeeperConfig(
      bookkeepingEnabled,
      bookkeepingLocation,
      bookkeepingHadoopFormat,
      bookkeepingConnectionString,
      bookkeepingDbName,
      bookkeepingJdbcConfig,
      deltaDatabase,
      deltaTablePrefix
    )
  }
}