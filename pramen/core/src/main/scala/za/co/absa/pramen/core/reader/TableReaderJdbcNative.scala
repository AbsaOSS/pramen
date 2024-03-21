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
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.utils.{JdbcNativeUtils, TimeUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

class TableReaderJdbcNative(jdbcReaderConfig: TableReaderJdbcConfig,
                            jdbcUrlSelector: JdbcUrlSelector,
                            conf: Config)
                           (implicit spark: SparkSession) extends TableReaderJdbcBase(jdbcReaderConfig, jdbcUrlSelector, conf) {
  import TableReaderJdbcNative._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val jdbcConfig = jdbcReaderConfig.jdbcConfig
  private val infoDateFormatPattern = jdbcReaderConfig.infoDateFormat
  private val infoDateFormatter = DateTimeFormatter.ofPattern(infoDateFormatPattern)
  private val url = jdbcUrlSelector.getWorkingUrl(jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls))

  logConfiguration()

  private[core] def getJdbcReaderConfig: TableReaderJdbcConfig = jdbcReaderConfig

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
    log.info(s"JDBC Native data of: $query")
    query match {
      case Query.Sql(sql)     => getDataFrame(getFilteredSql(sql, infoDateBegin, infoDateEnd, infoDateFormatter))
      case Query.Table(table) => getDataFrame(getSqlDataQuery(table, infoDateBegin, infoDateEnd, columns))
      case other              => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC Native reader. Use 'sql' or 'table' instead.")
    }
  }

  private[core] def getSqlExpression(query: Query): String = {
    query match {
      case Query.Sql(sql) => sql
      case other          => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC Native reader. Use 'sql' instead.")
    }
  }

  private[core] def getSqlDataQuery(table: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): String = {
    if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getDataQuery(table, infoDateBegin, infoDateEnd, columns, jdbcReaderConfig.limitRecords)
    } else {
      sqlGen.getDataQuery(table, columns, jdbcReaderConfig.limitRecords)
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
}

object TableReaderJdbcNative {
  def apply(conf: Config,
            parent: String = "")
           (implicit spark: SparkSession): TableReaderJdbcNative = {
    val tableReaderJdbc = TableReaderJdbcConfig.load(conf, parent)
    val urlSelector = JdbcUrlSelector(tableReaderJdbc.jdbcConfig)

    new TableReaderJdbcNative(tableReaderJdbc, urlSelector, conf)
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
