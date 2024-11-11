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

package za.co.absa.pramen.core.reader.model

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.offset.OffsetInfo
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType}
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.ZoneId

case class TableReaderJdbcConfig(
                                  jdbcConfig: JdbcConfig,
                                  hasInfoDate: Boolean,
                                  infoDateColumn: String,
                                  infoDateType: SqlColumnType,
                                  infoDateFormat: String = "yyyy-MM-dd",
                                  offsetInfoOpt: Option[OffsetInfo],
                                  limitRecords: Option[Int] = None,
                                  saveTimestampsAsDates: Boolean = false,
                                  correctDecimalsInSchema: Boolean = false,
                                  correctDecimalsFixPrecision: Boolean = false,
                                  enableSchemaMetadata: Boolean = false,
                                  useJdbcNative: Boolean = false,
                                  serverTimeZone: ZoneId = ZoneId.systemDefault(),
                                  identifierQuotingPolicy: QuotingPolicy = QuotingPolicy.Auto,
                                  sqlGeneratorClass: Option[String] = None
                                )

object TableReaderJdbcConfig {
  private val log = LoggerFactory.getLogger(this.getClass)

  val HAS_INFO_DATE = "has.information.date.column"
  val INFORMATION_DATE_COLUMN = "information.date.column"
  val INFORMATION_DATE_TYPE = "information.date.type"
  val INFORMATION_DATE_FORMAT = "information.date.format"
  val INFORMATION_DATE_APP_FORMAT = "information.date.app.format"

  val JDBC_SYNC_LIMIT_RECORDS = "limit.records"
  val JDBC_TIMESTAMPS_AS_DATES = "save.timestamps.as.dates"
  val CORRECT_DECIMALS_IN_SCHEMA = "correct.decimals.in.schema"
  val CORRECT_DECIMALS_FIX_PRECISION = "correct.decimals.fix.precision"
  val ENABLE_SCHEMA_METADATA_KEY = "enable.schema.metadata"
  val USE_JDBC_NATIVE = "use.jdbc.native"
  val SERVER_TIMEZONE = "server.timezone"
  val IDENTIFIER_QUOTING_POLICY = "identifier.quoting.policy"
  val SQL_GENERATOR_CLASS_KEY = "sql.generator.class"

  def load(conf: Config, workflowConf: Config, parent: String = ""): TableReaderJdbcConfig = {
    ConfigUtils.validatePathsExistence(conf, parent, HAS_INFO_DATE :: Nil)

    val hasInformationDate = conf.getBoolean(HAS_INFO_DATE)

    if (hasInformationDate) {
      ConfigUtils.validatePathsExistence(conf,
        parent,
        INFORMATION_DATE_COLUMN :: INFORMATION_DATE_TYPE :: Nil)
    }

    val infoDateTypeStr = ConfigUtils.getOptionString(conf, INFORMATION_DATE_TYPE).getOrElse("date")

    val infoDateType = if (hasInformationDate) {
      SqlColumnType.fromStringStrict(infoDateTypeStr, parent)
    } else {
      SqlColumnType.DATE
    }

    val saveTimestampsAsDates = ConfigUtils.getOptionBoolean(conf, JDBC_TIMESTAMPS_AS_DATES).getOrElse(false)

    if (saveTimestampsAsDates) {
      log.warn(s"An obsolete flag '$JDBC_TIMESTAMPS_AS_DATES' is used. Please, use inline column transformations instead ('transformations = { ... }').")
    }

    val infoDateFormat = getInfoDateFormat(conf)

    val offsetInfoOpt = OffsetInfoParser.fromConfig(conf)

    val defaultTimezone = ConfigUtils.getOptionString(workflowConf, Keys.TIMEZONE).getOrElse("Africa/Johannesburg")
    val serverTimezone = ZoneId.of(ConfigUtils.getOptionString(conf, SERVER_TIMEZONE).getOrElse(defaultTimezone))

    val identifierQuotingPolicy = ConfigUtils.getOptionString(conf, IDENTIFIER_QUOTING_POLICY)
      .map(s => QuotingPolicy.fromString(s))
      .getOrElse(QuotingPolicy.Auto)

    TableReaderJdbcConfig(
      jdbcConfig = JdbcConfig.load(conf, parent),
      hasInfoDate = conf.getBoolean(HAS_INFO_DATE),
      infoDateColumn = ConfigUtils.getOptionString(conf, INFORMATION_DATE_COLUMN).getOrElse(""),
      infoDateType = infoDateType,
      infoDateFormat,
      offsetInfoOpt,
      limitRecords = ConfigUtils.getOptionInt(conf, JDBC_SYNC_LIMIT_RECORDS),
      saveTimestampsAsDates,
      correctDecimalsInSchema = ConfigUtils.getOptionBoolean(conf, CORRECT_DECIMALS_IN_SCHEMA).getOrElse(false),
      correctDecimalsFixPrecision = ConfigUtils.getOptionBoolean(conf, CORRECT_DECIMALS_FIX_PRECISION).getOrElse(false),
      enableSchemaMetadata = ConfigUtils.getOptionBoolean(conf, ENABLE_SCHEMA_METADATA_KEY).getOrElse(false),
      useJdbcNative = ConfigUtils.getOptionBoolean(conf, USE_JDBC_NATIVE).getOrElse(false),
      serverTimezone,
      identifierQuotingPolicy = identifierQuotingPolicy,
      sqlGeneratorClass = ConfigUtils.getOptionString(conf, SQL_GENERATOR_CLASS_KEY)
    )
  }

  def getInfoDateFormat(conf: Config): String = {
    if (conf.hasPath(INFORMATION_DATE_APP_FORMAT)) {
      log.warn(s"An obsolete option is used: '$INFORMATION_DATE_APP_FORMAT'. Please, replace it with '$INFORMATION_DATE_FORMAT'.")
      conf.getString(INFORMATION_DATE_APP_FORMAT)
    } else if (conf.hasPath(INFORMATION_DATE_FORMAT))
      conf.getString(INFORMATION_DATE_FORMAT)
    else
      "yyyy-MM-dd"
  }
}
