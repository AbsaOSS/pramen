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
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.utils.{ConfigUtils, JdbcNativeUtils, JdbcSparkUtils, TimeUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class TableReaderJdbc(jdbcReaderConfig: TableReaderJdbcConfig,
                      jdbcUrlSelector: JdbcUrlSelector,
                      conf: Config
                     )(implicit spark: SparkSession) extends TableReaderJdbcBase(jdbcReaderConfig, jdbcUrlSelector, conf) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val infoDateFormatter = DateTimeFormatter.ofPattern(jdbcReaderConfig.infoDateFormat)

  logConfiguration()

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val start = Instant.now
    val count = query match {
      case Query.Table(tableName) =>
        getCountForTable(tableName, infoDateBegin, infoDateEnd)
      case Query.Sql(sql) =>
        getCountForSql(sql, infoDateBegin, infoDateEnd)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }

    val finish = Instant.now

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    query match {
      case Query.Table(tableName) =>
        getDataForTable(tableName, infoDateBegin, infoDateEnd, columns)
      case Query.Sql(sql) =>
        getDataForSql(sql, infoDateBegin, infoDateEnd, columns)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }
  }

  private[core] def getJdbcConfig: TableReaderJdbcConfig = jdbcReaderConfig

  @tailrec
  final private[core] def getWithRetry[T](sql: String,
                                          isDataQuery: Boolean,
                                          retriesLeft: Int,
                                          tableOpt: Option[String])(f: DataFrame => T): T = {
    Try {
      val df = getDataFrame(sql, isDataQuery, tableOpt)
      f(df)
    } match {
      case Success(result) => result
      case Failure(ex) =>
        val currentUrl = jdbcUrlSelector.getUrl
        if (retriesLeft > 1) {
          val nextUrl = jdbcUrlSelector.getNextUrl
          log.error(s"JDBC connection error for $currentUrl. Retries left: ${retriesLeft - 1}. Retrying...", ex)
          log.info(s"Trying URL: $nextUrl")
          getWithRetry(sql, isDataQuery, retriesLeft - 1, tableOpt)(f)
        } else {
          log.error(s"JDBC connection error for $currentUrl. No connection attempts left.", ex)
          throw ex
        }
    }
  }

  private[core] def getCountForTable(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val sql = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getCountQuery(tableName, infoDateBegin, infoDateEnd)
    } else {
      sqlGen.getCountQuery(tableName)
    }

    getWithRetry[Long](sql, isDataQuery = false, jdbcRetries, None)(df =>
      // Take first column of the first row, use BigDecimal as the most generic numbers parser,
      // and then convert to Long. This is a safe way if the output is like "0E-11".
      BigDecimal(df.collect()(0)(0).toString).toLong
    )
  }

  private[core] def getCountSqlQuery(sql: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val filteredSql = TableReaderJdbcNative.getFilteredSql(sql, infoDateBegin, infoDateEnd)

    sqlGen.getCountQueryForSql(filteredSql)
  }

  private[core] def getCountForSql(sql: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val countSql = getCountSqlQuery(sql, infoDateBegin, infoDateEnd)
    var count = 0L

    log.info(s"Executing: $countSql")

    JdbcNativeUtils.withResultSet(jdbcUrlSelector, countSql, jdbcRetries) { rs =>
      if (!rs.next())
        throw new IllegalStateException(s"No rows returned by the count query: $countSql")
      else {
        count = rs.getLong(1)
      }
    }

    count
  }

  private[core] def getDataForTable(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    val sql = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getDataQuery(tableName, infoDateBegin, infoDateEnd, columns, jdbcReaderConfig.limitRecords)
    } else {
      sqlGen.getDataQuery(tableName, columns, jdbcReaderConfig.limitRecords)
    }

    val df = getWithRetry[DataFrame](sql, isDataQuery = true, jdbcRetries, Option(tableName))(df => {
      // Make sure connection to the server is made without fetching the data
      log.debug(df.schema.treeString)
      df
    })

    df
  }

  private[core] def getDataForSql(sql: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    val filteredSql = TableReaderJdbcNative.getFilteredSql(sql, infoDateBegin, infoDateEnd)
    getWithRetry[DataFrame](filteredSql, isDataQuery = true, jdbcRetries, None)(df => filterDfColumns(df, columns))
  }

  private[core] def getDataFrame(sql: String, isDataQuery: Boolean, tableOpt: Option[String]): DataFrame = {
    log.info(s"JDBC Query: $sql")
    val qry = sqlGen.getDtable(sql)

    if (log.isDebugEnabled) {
      log.debug(s"Sending to JDBC: $qry")
    }

    val connectionOptions = JdbcSparkUtils.getJdbcOptions(jdbcUrlSelector.getUrl, jdbcReaderConfig.jdbcConfig, qry, extraOptions)

    if (log.isDebugEnabled) {
      log.debug("Connection options:")
      ConfigUtils.renderExtraOptions(connectionOptions, Keys.KEYS_TO_REDACT)(s => log.debug(s))
    }

    var df = spark
      .read
      .format("jdbc")
      .options(connectionOptions)
      .load()

    if (jdbcReaderConfig.correctDecimalsInSchema || jdbcReaderConfig.correctDecimalsFixPrecision) {
      JdbcSparkUtils.getCorrectedDecimalsSchema(df, jdbcReaderConfig.correctDecimalsFixPrecision).foreach(schema =>
        df = spark
          .read
          .format("jdbc")
          .options(connectionOptions)
          .option("customSchema", schema)
          .load()
      )
    }

    if (jdbcReaderConfig.saveTimestampsAsDates) {
      df = JdbcSparkUtils.convertTimestampToDates(df)
    }

    if (isDataQuery && jdbcReaderConfig.enableSchemaMetadata) {
      log.info(s"Reading JDBC metadata from the query: $sql")
      JdbcSparkUtils.withJdbcMetadata(jdbcReaderConfig.jdbcConfig, sql) { (connection, jdbcMetadata) =>
        val schemaWithMetadata = JdbcSparkUtils.addMetadataFromJdbc(df.schema, jdbcMetadata)
        val schemaWithColumnDescriptions = tableOpt match {
          case Some(table) =>
            log.info(s"Reading JDBC metadata descriptions the table: $table")
            JdbcSparkUtils.addColumnDescriptionsFromJdbc(schemaWithMetadata, sqlGen.unquote(table), connection)
          case None =>
            schemaWithMetadata
        }
        df = spark.createDataFrame(df.rdd, schemaWithColumnDescriptions)
      }
    }

    if (isDataQuery && log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    jdbcReaderConfig.limitRecords match {
      case Some(limit) => df.limit(limit)
      case None => df
    }
  }

  private[core] def filterDfColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    if (columns.nonEmpty) {
      df.select(columns.head, columns.tail: _*)
    } else {
      df
    }
  }
}

object TableReaderJdbc {
  def apply(conf: Config, parent: String)(implicit spark: SparkSession): TableReaderJdbc = {
    val jdbcTableReaderConfig = TableReaderJdbcConfig.load(conf, parent)

    val urlSelector = JdbcUrlSelector(jdbcTableReaderConfig.jdbcConfig)

    new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, conf)
  }
}
