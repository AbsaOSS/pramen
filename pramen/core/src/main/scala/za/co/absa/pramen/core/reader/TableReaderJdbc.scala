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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DecimalType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, TableReader}
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.config.Keys.KEYS_TO_REDACT
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.sql.{SqlColumnType, SqlConfig, SqlGenerator}
import za.co.absa.pramen.core.utils.JdbcNativeUtils.JDBC_WORDS_TO_REDACT
import za.co.absa.pramen.core.utils.{ConfigUtils, JdbcSparkUtils, TimeUtils}

import java.sql.Date
import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class TableReaderJdbc(jdbcReaderConfig: TableReaderJdbcConfig,
                      jdbcUrlSelector: JdbcUrlSelector,
                      conf: Config
                     )(implicit spark: SparkSession) extends TableReader {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val extraOptions = ConfigUtils.getExtraOptions(conf, "option")

  private val jdbcRetries = jdbcReaderConfig.jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls)

  logConfiguration()

  private[core] lazy val sqlGen = {
    val gen = SqlGenerator.fromDriverName(jdbcReaderConfig.jdbcConfig.driver,
      getSqlConfig,
      ConfigUtils.getExtraConfig(conf, "sql"))

    if (gen.requiresConnection) {
      val (connection, url) = jdbcUrlSelector.getWorkingConnection(jdbcRetries)
      gen.setConnection(connection)
    }
    gen
  }

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
        getDataForTable(tableName, infoDateBegin, infoDateEnd)
      case Query.Sql(sql) =>
        getDataForSql(sql, infoDateBegin, infoDateEnd)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }
  }

  private[core] def getJdbcConfig: TableReaderJdbcConfig = jdbcReaderConfig

  @tailrec
  final private[core] def getWithRetry[T](sql: String,
                                          isDataQuery: Boolean,
                                          retriesLeft: Int)(f: DataFrame => T): T = {
    Try {
      val df = getDataFrame(sql, isDataQuery)
      f(df)
    } match {
      case Success(result) => result
      case Failure(ex) =>
        val currentUrl = jdbcUrlSelector.getUrl
        if (retriesLeft > 1) {
          val nextUrl = jdbcUrlSelector.getNextUrl
          log.error(s"JDBC connection error for $currentUrl. Retries left: ${retriesLeft - 1}. Retrying...", ex)
          log.info(s"Trying URL: $nextUrl")
          getWithRetry(sql, isDataQuery, retriesLeft - 1)(f)
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

    getWithRetry[Long](sql, isDataQuery = false, jdbcRetries)(df =>
      // Take first column of the first row, use BigDecimal as the most generic numbers parser,
      // and then convert to Long. This is a safe way if the output is like "0E-11".
      BigDecimal(df.collect()(0)(0).toString).toLong
    )
  }

  private[core] def getCountForSql(sql: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    getWithRetry[Long](sql, isDataQuery = false, jdbcRetries) { df =>
      val filteredDf = getFilteredDf(df, infoDateBegin, infoDateEnd)
      filteredDf.count()
    }
  }

  private[core] def getDataForTable(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    val sql = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getDataQuery(tableName, infoDateBegin, infoDateEnd, Nil, jdbcReaderConfig.limitRecords)
    } else {
      sqlGen.getDataQuery(tableName, Nil, jdbcReaderConfig.limitRecords)
    }

    val df = getWithRetry[DataFrame](sql, isDataQuery = true, jdbcRetries)(df => {
      // Make sure connection to the server is made without fetching the data
      log.debug(df.schema.treeString)
      df
    })

    df
  }

  private[core] def getDataForSql(sql: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    getWithRetry[DataFrame](sql, isDataQuery = true, jdbcRetries) { df =>
      getFilteredDf(df, infoDateBegin, infoDateEnd)
    }
  }

  private[core] def getDataFrame(sql: String, isDataQuery: Boolean): DataFrame = {
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
      JdbcSparkUtils.withJdbcMetadata(jdbcReaderConfig.jdbcConfig, sql) { jdbcMetadata =>
        val newSchema = JdbcSparkUtils.addMetadataFromJdbc(df.schema, jdbcMetadata)
        df = spark.createDataFrame(df.rdd, newSchema)
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

  private[core] def getFilteredDf(df: DataFrame, infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    if (jdbcReaderConfig.hasInfoDate) {
      df.filter(col(jdbcReaderConfig.infoDateColumn).cast(DateType)
        .between(Date.valueOf(infoDateBegin), Date.valueOf(infoDateEnd)))
    } else {
      df
    }
  }

  private def getSqlConfig: SqlConfig = {
    val dateFieldType = SqlColumnType.fromString(jdbcReaderConfig.infoDateType)
    dateFieldType match {
      case Some(infoDateType) =>
        SqlConfig(jdbcReaderConfig.infoDateColumn,
          infoDateType,
          jdbcReaderConfig.infoDateFormatSql,
          jdbcReaderConfig.infoDateFormatApp)
      case None => throw new IllegalArgumentException(s"Unknown info date type specified (${jdbcReaderConfig.infoDateType}). " +
        s"It should be one of: date, string, number")
    }
  }

  private def logConfiguration(): Unit = {
    jdbcUrlSelector.logConnectionSettings()

    log.info(s"JDBC Reader Configuration:")
    log.info(s"Has information date column:  ${jdbcReaderConfig.hasInfoDate}")
    if (jdbcReaderConfig.hasInfoDate) {
      log.info(s"Info date column name:        ${jdbcReaderConfig.infoDateColumn}")
      log.info(s"Info date column data type:   ${jdbcReaderConfig.infoDateType}")
      log.info(s"Info date format (SQL):       ${jdbcReaderConfig.infoDateFormatSql}")
      log.info(s"Info date format (App):       ${jdbcReaderConfig.infoDateFormatApp}")
    }
    log.info(s"Save timestamp as dates:      ${jdbcReaderConfig.saveTimestampsAsDates}")
    log.info(s"Correct decimals in schemas:  ${jdbcReaderConfig.correctDecimalsInSchema}")
    jdbcReaderConfig.limitRecords.foreach(n => log.info(s"Limit records:                $n"))

    log.info("Extra JDBC reader Spark options:")
    ConfigUtils.renderExtraOptions(extraOptions, JDBC_WORDS_TO_REDACT)(s => log.info(s))
  }
}

object TableReaderJdbc {
  def apply(conf: Config, parent: String = "")(implicit spark: SparkSession): TableReaderJdbc = {
    val jdbcTableReaderConfig = TableReaderJdbcConfig.load(conf, parent)

    val urlSelector = JdbcUrlSelector(jdbcTableReaderConfig.jdbcConfig)

    new TableReaderJdbc(jdbcTableReaderConfig, urlSelector, conf)
  }
}
