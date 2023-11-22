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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, TableReader}
import za.co.absa.pramen.core.app.config.InfoDateConfig.DEFAULT_DATE_FORMAT
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig.INFORMATION_DATE_APP_FORMAT
import za.co.absa.pramen.core.utils.{ConfigUtils, JdbcNativeUtils, TimeUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

class TableReaderJdbcNative(jdbcConfig: JdbcConfig,
                            jdbcUrlSelector: JdbcUrlSelector,
                            infoDateFormatPattern: String)
                           (implicit spark: SparkSession) extends TableReader {
  import TableReaderJdbcNative._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val url = jdbcUrlSelector.getWorkingUrl(jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls))

  private val infoDateFormatter = DateTimeFormatter.ofPattern(infoDateFormatPattern)

  logConfiguration()

  private[core] def getJdbcConfig: JdbcConfig = jdbcConfig

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val start = Instant.now()
    val sql = getFilteredSql(getSqlExpression(query), infoDateBegin, infoDateEnd, infoDateFormatter)
    log.info(s"JDBC Native count of: $sql")
    val count = JdbcNativeUtils.getJdbcNativeRecordCount(jdbcConfig, url, sql)
    val finish = Instant.now()

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    getDataFrame(getFilteredSql(getSqlExpression(query), infoDateBegin, infoDateEnd, infoDateFormatter))
  }

  private[core] def getSqlExpression(query: Query): String = {
    query match {
      case Query.Sql(sql) => sql
      case other          => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC Native reader. Use 'sql' instead.")
    }
  }

  private[core] def getDataFrame(sql: String): DataFrame = {
    log.info(s"JDBC Query: $sql")

    val df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, url, sql)

    if (log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    df
  }

  private def logConfiguration(): Unit = {
    jdbcUrlSelector.logConnectionSettings()
  }
}

object TableReaderJdbcNative {
  def apply(conf: Config,
            parent: String = "")
           (implicit spark: SparkSession): TableReaderJdbcNative = {
    val jdbcConfig = JdbcConfig.load(conf, parent)
    val urlSelector = JdbcUrlSelector(jdbcConfig)
    val infoDateFormat = ConfigUtils.getOptionString(conf, INFORMATION_DATE_APP_FORMAT).getOrElse(DEFAULT_DATE_FORMAT)

    new TableReaderJdbcNative(jdbcConfig, urlSelector, infoDateFormat)
  }

  def getFilteredSql(sqlExpression: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, formatter: DateTimeFormatter): String = {
    sqlExpression
      .replaceAll("@dateFrom", formatter.format(infoDateBegin))
      .replaceAll("@dateTo", formatter.format(infoDateEnd))
      .replaceAll("@date", formatter.format(infoDateEnd))
      .replaceAll("@infoDateBegin", formatter.format(infoDateBegin))
      .replaceAll("@infoDateEnd", formatter.format(infoDateEnd))
      .replaceAll("@infoDate", formatter.format(infoDateEnd))
  }
}
