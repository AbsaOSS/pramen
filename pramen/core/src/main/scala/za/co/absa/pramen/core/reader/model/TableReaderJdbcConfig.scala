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

package za.co.absa.pramen.framework.reader.model

import com.typesafe.config.Config
import za.co.absa.pramen.framework.utils.ConfigUtils

case class TableReaderJdbcConfig(
                                  jdbcConfig: JdbcConfig,
                                  hasInfoDate: Boolean,
                                  infoDateColumn: String,
                                  infoDateType: String,
                                  infoDateFormatApp: String = "yyyy-MM-dd",
                                  infoDateFormatSql: String = "YYYY-MM-DD",
                                  limitRecords: Option[Int] = None,
                                  saveTimestampsAsDates: Boolean = false,
                                  correctDecimalsInSchema: Boolean = false,
                                  correctDecimalsFixPrecision: Boolean = false,
                                  connectionRetries: Option[Int] = None
                                )

object TableReaderJdbcConfig {
  val HAS_INFO_DATE = "has.information.date.column"
  val INFORMATION_DATE_COLUMN = "information.date.column"
  val INFORMATION_DATE_TYPE = "information.date.type"
  val INFORMATION_DATE_APP_FORMAT = "information.date.app.format"
  val INFORMATION_DATE_SQL_FORMAT = "information.date.sql.format"

  val JDBC_SYNC_LIMIT_RECORDS = "limit.records"
  val JDBC_TIMESTAMPS_AS_DATES = "save.timestamps.as.dates"
  val CORRECT_DECIMALS_IN_SCHEMA = "correct.decimals.in.schema"
  val CORRECT_DECIMALS_FIX_PRECISION = "correct.decimals.fix.precision"
  val CONNECTION_RETRIES = "connection.retries"

  def load(conf: Config, parent: String = ""): TableReaderJdbcConfig = {
    ConfigUtils.validatePathsExistence(conf, parent, HAS_INFO_DATE :: Nil)

    val hasInformationDate = conf.getBoolean(HAS_INFO_DATE)

    if (hasInformationDate) {
      ConfigUtils.validatePathsExistence(conf,
        parent,
        INFORMATION_DATE_COLUMN :: INFORMATION_DATE_TYPE :: INFORMATION_DATE_APP_FORMAT :: INFORMATION_DATE_SQL_FORMAT :: Nil)
    }

    TableReaderJdbcConfig(
      jdbcConfig = JdbcConfig.load(conf, parent),
      hasInfoDate = conf.getBoolean(HAS_INFO_DATE),
      infoDateColumn = ConfigUtils.getOptionString(conf, INFORMATION_DATE_COLUMN).getOrElse(""),
      infoDateType = ConfigUtils.getOptionString(conf, INFORMATION_DATE_TYPE).getOrElse("date"),
      infoDateFormatApp = ConfigUtils.getOptionString(conf, INFORMATION_DATE_APP_FORMAT).getOrElse("yyyy-MM-dd"),
      infoDateFormatSql = ConfigUtils.getOptionString(conf, INFORMATION_DATE_SQL_FORMAT).getOrElse("YYYY-MM-DD"),
      limitRecords = ConfigUtils.getOptionInt(conf, JDBC_SYNC_LIMIT_RECORDS),
      saveTimestampsAsDates = ConfigUtils.getOptionBoolean(conf, JDBC_TIMESTAMPS_AS_DATES).getOrElse(false),
      correctDecimalsInSchema = ConfigUtils.getOptionBoolean(conf, CORRECT_DECIMALS_IN_SCHEMA).getOrElse(false),
      correctDecimalsFixPrecision = ConfigUtils.getOptionBoolean(conf, CORRECT_DECIMALS_FIX_PRECISION).getOrElse(false),
      connectionRetries = ConfigUtils.getOptionInt(conf, CONNECTION_RETRIES)
    )
  }
}
