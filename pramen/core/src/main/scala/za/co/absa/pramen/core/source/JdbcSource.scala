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

package za.co.absa.pramen.core.source

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.reader.{JdbcUrlSelector, TableReaderJdbc, TableReaderJdbcNative}
import za.co.absa.pramen.core.source.SparkSource.DISABLE_COUNT_QUERY
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

class JdbcSource(sourceConfig: Config,
                 sourceConfigParentPath: String,
                 val jdbcReaderConfig: TableReaderJdbcConfig,
                 val disableCountQuery: Boolean)(implicit spark: SparkSession) extends Source {
  private val log = LoggerFactory.getLogger(this.getClass)

  override val config: Config = sourceConfig

  override def isDataAlwaysAvailable: Boolean = disableCountQuery

  override def hasInfoDateColumn(query: Query): Boolean = jdbcReaderConfig.hasInfoDate

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val reader = getReader(query)

    reader.getRecordCount(query, infoDateBegin, infoDateEnd)
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    val reader = getReader(query)

    val df = reader.getData(query, infoDateBegin, infoDateEnd, columns)

    SourceResult(df)
  }

  private[core] def getReader(query: Query): TableReader = {
    val urlSelector = JdbcUrlSelector(jdbcReaderConfig.jdbcConfig)

    query match {
      case Query.Table(dbTable) =>
        log.info(s"Using TableReaderJdbc to read the table: $dbTable")
        new TableReaderJdbc(jdbcReaderConfig, urlSelector, sourceConfig)
      case Query.Sql(sql) if canUseSparkBuiltInJdbcConnector(sql) =>
        log.info(s"Using TableReaderJdbc to read the query: $sql")
        new TableReaderJdbc(jdbcReaderConfig, urlSelector, sourceConfig)
      case Query.Sql(sql)  =>
        log.info(s"Using TableReaderJdbcNative to read the query: $sql")
        new TableReaderJdbcNative(jdbcReaderConfig.jdbcConfig, urlSelector, jdbcReaderConfig.infoDateFormat)
      case q =>
        throw new IllegalArgumentException(s"Unexpected '${q.name}' spec for the JDBC reader. Only 'table' or 'sql' are supported. Config path: $sourceConfigParentPath")
    }
  }

  private def canUseSparkBuiltInJdbcConnector(sql: String): Boolean = {
    sql.toLowerCase.startsWith("select") && !jdbcReaderConfig.useJdbcNative
  }
}

object JdbcSource extends ExternalChannelFactory[JdbcSource] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): JdbcSource = {
    val tableReaderJdbc = TableReaderJdbcConfig.load(conf)
    val disableCountQuery = ConfigUtils.getOptionBoolean(conf, DISABLE_COUNT_QUERY).getOrElse(false)

    new JdbcSource(conf, parentPath, tableReaderJdbc, disableCountQuery)(spark)
  }
}
