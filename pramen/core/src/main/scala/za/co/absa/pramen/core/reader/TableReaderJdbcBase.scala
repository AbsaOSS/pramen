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

package za.co.absa.pramen.core.reader

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.TableReader
import za.co.absa.pramen.api.sql.{SqlColumnType, SqlConfig}
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.sql.SqlGeneratorLoader
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.JdbcNativeUtils.JDBC_WORDS_TO_REDACT

abstract class TableReaderJdbcBase(jdbcReaderConfig: TableReaderJdbcConfig,
                                   jdbcUrlSelector: JdbcUrlSelector,
                                   conf: Config) extends TableReader {
  private val log = LoggerFactory.getLogger(this.getClass)

  protected val jdbcRetries: Int = jdbcReaderConfig.jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls)
  protected val extraOptions: Map[String, String] = ConfigUtils.getExtraOptions(conf, "option")

  private[core] lazy val sqlGen = {
    val gen = SqlGeneratorLoader.getSqlGenerator(jdbcReaderConfig.jdbcConfig.driver, getSqlConfig)

    if (gen.requiresConnection) {
      val (connection, _) = jdbcUrlSelector.getWorkingConnection(jdbcRetries)
      gen.setConnection(connection)
    }
    gen
  }

  private[core] def getSqlConfig: SqlConfig = {
    val dateFieldType = SqlColumnType.fromString(jdbcReaderConfig.infoDateType)
    dateFieldType match {
      case Some(infoDateType) =>
        SqlConfig(jdbcReaderConfig.infoDateColumn,
          infoDateType,
          jdbcReaderConfig.infoDateFormat,
          jdbcReaderConfig.identifierQuotingPolicy,
          jdbcReaderConfig.sqlGeneratorClass,
          ConfigUtils.getExtraConfig(conf, "sql"))
      case None => throw new IllegalArgumentException(s"Unknown info date type specified (${jdbcReaderConfig.infoDateType}). " +
        s"It should be one of: date, string, number")
    }
  }

  protected def logConfiguration(): Unit = {
    jdbcUrlSelector.logConnectionSettings()

    log.info(s"JDBC Reader Configuration:")
    log.info(s"Has information date column:  ${jdbcReaderConfig.hasInfoDate}")
    if (jdbcReaderConfig.hasInfoDate) {
      log.info(s"Info date column name:      ${jdbcReaderConfig.infoDateColumn}")
      log.info(s"Info date column data type: ${jdbcReaderConfig.infoDateType}")
      log.info(s"Info date format:           ${jdbcReaderConfig.infoDateFormat}")
    }
    log.info(s"Save timestamp as dates:      ${jdbcReaderConfig.saveTimestampsAsDates}")
    log.info(s"Correct decimals in schemas:  ${jdbcReaderConfig.correctDecimalsInSchema}")
    log.info(s"Identifier quoting policy:    ${jdbcReaderConfig.identifierQuotingPolicy.name}")
    jdbcReaderConfig.limitRecords.foreach(n => log.info(s"Limit records:                $n"))

    log.info("Extra JDBC reader Spark options:")
    ConfigUtils.renderExtraOptions(extraOptions, JDBC_WORDS_TO_REDACT)(s => log.info(s))
  }
}
