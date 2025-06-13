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
import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.reader.model.{JdbcConfig, TableReaderJdbcConfig}
import za.co.absa.pramen.core.utils._

import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class TableReaderJdbc(jdbcReaderConfig: TableReaderJdbcConfig,
                      jdbcUrlSelector: JdbcUrlSelector,
                      conf: Config
                     )(implicit spark: SparkSession) extends TableReaderJdbcBase(jdbcReaderConfig, jdbcUrlSelector, conf) {
  private val log = LoggerFactory.getLogger(this.getClass)

  logConfiguration()

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val transformedQuery = TableReaderJdbcNative.applyInfoDateExpressionToQuery(query, infoDateBegin, infoDateEnd)
    val start = Instant.now
    val count = transformedQuery match {
      case Query.Table(tableName) =>
        getCountForTableNatively(tableName, infoDateBegin, infoDateEnd)
      case Query.Sql(sql) =>
        getCountForSql(sql)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }

    val finish = Instant.now

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    val transformedQuery = TableReaderJdbcNative.applyInfoDateExpressionToQuery(query, infoDateBegin, infoDateEnd)
    transformedQuery match {
      case Query.Table(tableName) =>
        getDataForTable(tableName, infoDateBegin, infoDateEnd, columns)
      case Query.Sql(sql) =>
        getDataForSql(sql, columns)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }
  }

  override def getIncrementalData(query: Query, onlyForInfoDate: Option[LocalDate], offsetFrom: Option[OffsetValue], offsetTo: Option[OffsetValue], columns: Seq[String]): DataFrame = {
    query match {
      case Query.Table(tableName) =>
        getDataForTableIncremental(tableName, onlyForInfoDate, offsetFrom, offsetTo, columns)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' incremental ingestion is not supported by the JDBC reader. Use 'table' instead.")
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

  private[core] def getCountSqlQuery(sql: String): String = {
    sqlGen.getCountQueryForSql(sql)
  }

  private[core] def getCountForTableNatively(table: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val query = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getCountQuery(table, infoDateBegin, infoDateEnd)
    } else {
      sqlGen.getCountQuery(table)
    }

    getCountForCountSql(query)
  }

  private[core] def getCountForSql(sql: String): Long = {
    getCountForCountSql(getCountSqlQuery(sql))
  }

  private[core] def getCountForCountSql(countSql: String): Long = {
    var count = 0L
    log.info(s"Executing: $countSql")

    JdbcNativeUtils.withResultSet(jdbcUrlSelector, countSql, jdbcRetries) { rs =>
      if (!rs.next())
        throw new IllegalStateException(s"No rows returned by the count query: $countSql")
      else {
        if (rs.getObject(1) == null)
          throw new IllegalStateException(s"NULL returned by count query: $countSql")
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

  private[core] def getDataForTableIncremental(tableName: String,
                                               onlyForInfoDate: Option[LocalDate],
                                               offsetFrom: Option[OffsetValue],
                                               offsetTo: Option[OffsetValue],
                                               columns: Seq[String]): DataFrame = {
    val sql = sqlGen.getDataQueryIncremental(tableName, onlyForInfoDate, offsetFrom, offsetTo, columns)

    log.info(s"JDBC Query: $sql")

    val df = getWithRetry[DataFrame](sql, isDataQuery = true, jdbcRetries, Option(tableName))(df => {
      // Make sure connection to the server is made without fetching the data
      log.debug(df.schema.treeString)
      df
    })

    df
  }

  private[core] def getDataForSql(sql: String, columns: Seq[String]): DataFrame = {
    getWithRetry[DataFrame](sql, isDataQuery = true, jdbcRetries, None)(df => filterDfColumns(df, columns))
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
      if (isDataQuery) {
        df = SparkUtils.sanitizeDfColumns(df, jdbcReaderConfig.specialCharacters)
      }

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
      val schemaQuery = tableOpt match {
        case Some(table) => sqlGen.getSchemaQuery(table, Seq.empty)
        case _ => JdbcSparkUtils.getSchemaQuery(sql)
      }
      JdbcSparkUtils.withJdbcMetadata(jdbcReaderConfig.jdbcConfig, schemaQuery) { (connection, jdbcMetadata) =>
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
  def apply(conf: Config, workflowConf: Config, parent: String)(implicit spark: SparkSession): TableReaderJdbc = {
    val jdbcTableReaderConfig = TableReaderJdbcConfig.load(conf, workflowConf, parent)

    val urlSelector = JdbcUrlSelector(jdbcTableReaderConfig.jdbcConfig)

    new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, conf)
  }
}
