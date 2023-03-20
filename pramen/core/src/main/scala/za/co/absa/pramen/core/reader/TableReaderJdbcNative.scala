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
import za.co.absa.pramen.api.TableReader
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.{ConfigUtils, JdbcNativeUtils, TimeUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

class TableReaderJdbcNative(queryExpression: String,
                            jdbcConfig: JdbcConfig,
                            numberOfRetries: Option[Int])
                           (implicit spark: SparkSession) extends TableReader {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

  private val url = jdbcUrlSelector.getWorkingUrl(numberOfRetries.getOrElse(jdbcUrlSelector.getNumberOfUrls))

  logConfiguration()

  private[core] def getJdbcConfig: JdbcConfig = jdbcConfig

  override def getRecordCount(infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val start = Instant.now()
    val sql = getSql(infoDateBegin, infoDateEnd)
    log.info(s"JDBC Native count of: $sql")
    val count = JdbcNativeUtils.getJdbcNativeRecordCount(jdbcConfig, url, sql)
    val finish = Instant.now()

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(infoDateBegin: LocalDate, infoDateEnd: LocalDate): Option[DataFrame] = {
    val df = getDataFrame(getSql(infoDateBegin, infoDateEnd))
    Option(df)
  }

  private def getSql(infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    queryExpression
      .replaceAll("@dateFrom", dateFormatter.format(infoDateBegin))
      .replaceAll("@dateTo", dateFormatter.format(infoDateEnd))
      .replaceAll("@date", dateFormatter.format(infoDateEnd))
      .replaceAll("@infoDateBegin", dateFormatter.format(infoDateBegin))
      .replaceAll("@infoDateEnd", dateFormatter.format(infoDateEnd))
      .replaceAll("@infoDate", dateFormatter.format(infoDateEnd))
  }

  private def getDataFrame(sql: String): DataFrame = {
    log.info(s"JDBC Query: $sql")

    JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, url, sql)
  }


  private def logConfiguration(): Unit = {
    jdbcUrlSelector.logConnectionSettings()
    numberOfRetries.foreach(n => log.info(s"Retry attempts:               $n"))
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
}

object TableReaderJdbcNative {
  val CONNECTION_RETRIES = "connection.retries"

  def apply(expression: String,
            conf: Config,
            parent: String = "")
           (implicit spark: SparkSession): TableReaderJdbcNative = {
    val jdbcConfig = JdbcConfig.load(conf, parent)

    val numberOfRetries: Option[Int] = ConfigUtils.getOptionInt(conf, CONNECTION_RETRIES)

    new TableReaderJdbcNative(expression, jdbcConfig, numberOfRetries)
  }
}
