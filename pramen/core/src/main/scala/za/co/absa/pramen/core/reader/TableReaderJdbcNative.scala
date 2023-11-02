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
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig.ENABLE_SCHEMA_METADATA_KEY
import za.co.absa.pramen.core.utils.{ConfigUtils, JdbcNativeUtils, JdbcSparkUtils, TimeUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

class TableReaderJdbcNative(jdbcConfig: JdbcConfig, enableSchemaMetadata: Boolean)
                           (implicit spark: SparkSession) extends TableReader {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

  private val url = jdbcUrlSelector.getWorkingUrl(jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls))

  logConfiguration()

  private[core] def getJdbcConfig: JdbcConfig = jdbcConfig

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val start = Instant.now()
    val sql = getSql(getSqlExpression(query), infoDateBegin, infoDateEnd)
    log.info(s"JDBC Native count of: $sql")
    val count = JdbcNativeUtils.getJdbcNativeRecordCount(jdbcConfig, url, sql)
    val finish = Instant.now()

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    getDataFrame(getSql(getSqlExpression(query), infoDateBegin, infoDateEnd))
  }

  private def getSqlExpression(query: Query): String = {
    query match {
      case Query.Sql(sql) => sql
      case other          => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC Native reader. Use 'sql' instead.")
    }
  }

  private def getSql(sqlExpression: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    sqlExpression
      .replaceAll("@dateFrom", dateFormatter.format(infoDateBegin))
      .replaceAll("@dateTo", dateFormatter.format(infoDateEnd))
      .replaceAll("@date", dateFormatter.format(infoDateEnd))
      .replaceAll("@infoDateBegin", dateFormatter.format(infoDateBegin))
      .replaceAll("@infoDateEnd", dateFormatter.format(infoDateEnd))
      .replaceAll("@infoDate", dateFormatter.format(infoDateEnd))
  }

  private def getDataFrame(sql: String): DataFrame = {
    log.info(s"JDBC Query: $sql")

    var df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, url, sql)

    if (enableSchemaMetadata) {
      JdbcSparkUtils.withJdbcMetadata(jdbcConfig, sql) { jdbcMetadata =>
        val newSchema = JdbcSparkUtils.addMetadataFromJdbc(df.schema, jdbcMetadata)
        df = spark.createDataFrame(df.rdd, newSchema)
      }
    }

    if (log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    df
  }


  private def logConfiguration(): Unit = {
    jdbcUrlSelector.logConnectionSettings()
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
}

object TableReaderJdbcNative {
  def apply(conf: Config,
            parent: String = "")
           (implicit spark: SparkSession): TableReaderJdbcNative = {
    val jdbcConfig = JdbcConfig.load(conf, parent)
    val enableSchemaMetadata = ConfigUtils.getOptionBoolean(conf, ENABLE_SCHEMA_METADATA_KEY).getOrElse(false)

    new TableReaderJdbcNative(jdbcConfig, enableSchemaMetadata)
  }
}
