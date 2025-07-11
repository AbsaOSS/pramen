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
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.reader.model.{JdbcConfig, TableReaderJdbcConfig}
import za.co.absa.pramen.core.utils.{JdbcNativeUtils, JdbcSparkUtils, StringUtils, TimeUtils}

import java.time.{Instant, LocalDate}

class TableReaderJdbcNative(jdbcReaderConfig: TableReaderJdbcConfig,
                            jdbcUrlSelector: JdbcUrlSelector,
                            conf: Config)
                           (implicit spark: SparkSession) extends TableReaderJdbcBase(jdbcReaderConfig, jdbcUrlSelector, conf) {
  import TableReaderJdbcNative._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val jdbcConfig = jdbcReaderConfig.jdbcConfig

  private val url = jdbcUrlSelector.getWorkingUrl(jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls))

  logConfiguration()

  private[core] def getJdbcReaderConfig: TableReaderJdbcConfig = {
    jdbcReaderConfig.copy(jdbcConfig = jdbcConfig)
  }

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val transformedQuery = TableReaderJdbcNative.applyInfoDateExpressionToQuery(query, infoDateBegin, infoDateEnd)

    val start = Instant.now()
    val count = transformedQuery match {
      case Query.Table(tableName) =>
        getCountForTableNatively(tableName, infoDateBegin, infoDateEnd)
      case Query.Sql(sql) if sql.toLowerCase.trim.startsWith("select") =>
        getCountForSql(sql)
      case Query.Sql(sql) =>
        log.info(s"JDBC Native count of a non-SELECT SQL statement: $sql")
        JdbcNativeUtils.getJdbcNativeRecordCount(jdbcConfig, url, sql)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' or 'sql' instead.")
    }

    val finish = Instant.now()

    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    val transformedQuery = TableReaderJdbcNative.applyInfoDateExpressionToQuery(query, infoDateBegin, infoDateEnd)

    log.info(s"JDBC Native data of: $transformedQuery")
    transformedQuery match {
      case Query.Sql(sql)     => getDataFrame(sql, None)
      case Query.Table(table) => getDataFrame(getSqlDataQuery(table, infoDateBegin, infoDateEnd, columns), Option(table))
      case other              => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC Native reader. Use 'sql' or 'table' instead.")
    }
  }

  override def getIncrementalData(query: Query, onlyForInfoDate: Option[LocalDate], offsetFrom: Option[OffsetValue], offsetTo: Option[OffsetValue], columns: Seq[String]): DataFrame = {
    query match {
      case Query.Table(tableName) =>
        getDataForTableIncremental(tableName, onlyForInfoDate, offsetFrom, offsetTo, columns)
      case other =>
        throw new IllegalArgumentException(s"'${other.name}' incremental ingestion is not supported by the JDBC Native reader. Use 'table' instead.")
    }
  }

  private[core] def getSqlDataQuery(table: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): String = {
    if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getDataQuery(table, infoDateBegin, infoDateEnd, columns, jdbcReaderConfig.limitRecords)
    } else {
      sqlGen.getDataQuery(table, columns, jdbcReaderConfig.limitRecords)
    }
  }

  private[core] def getDataFrame(sql: String, tableOpt: Option[String]): DataFrame = {
    log.info(s"JDBC Query: $sql")

    var df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, url, sql)

    if (log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    if (jdbcReaderConfig.enableSchemaMetadata) {
      val schemaQuery = tableOpt match {
        case Some(table) => sqlGen.getSchemaQuery(table, Seq.empty)
        case _ => JdbcSparkUtils.getSchemaQuery(sql)
      }
      JdbcSparkUtils.withJdbcMetadata(jdbcConfig, schemaQuery) { (connection, _) =>
        val schemaWithColumnDescriptions = tableOpt match {
          case Some(table) =>
            log.info(s"Reading JDBC metadata descriptions the table: $table")
            JdbcSparkUtils.addColumnDescriptionsFromJdbc(df.schema, sqlGen.unquote(table), connection)
          case None =>
            df.schema
        }
        df = spark.createDataFrame(df.rdd, schemaWithColumnDescriptions)
      }
    }

    df
  }

  private[core] def getDataForTableIncremental(tableName: String,
                                               onlyForInfoDate: Option[LocalDate],
                                               offsetFrom: Option[OffsetValue],
                                               offsetTo: Option[OffsetValue],
                                               columns: Seq[String]): DataFrame = {
    val sql = sqlGen.getDataQueryIncremental(tableName, onlyForInfoDate, offsetFrom, offsetTo, columns)
    log.info(s"JDBC Query: $sql")

    var df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, url, sql)

    if (log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    if (jdbcReaderConfig.enableSchemaMetadata) {
      val schemaQuery = sqlGen.getSchemaQuery(tableName, columns)

      JdbcSparkUtils.withJdbcMetadata(jdbcReaderConfig.jdbcConfig, schemaQuery) { (connection, _) =>
        log.info(s"Reading JDBC metadata descriptions the table: $tableName")
        df = spark.createDataFrame(df.rdd,
          JdbcSparkUtils.addColumnDescriptionsFromJdbc(df.schema, sqlGen.unquote(tableName), connection))
      }
    }

    df
  }
}

object TableReaderJdbcNative {
  val FETCH_SIZE_KEY = "option.fetchsize"

  def apply(conf: Config,
            workflowConf: Config,
            parent: String = "")
           (implicit spark: SparkSession): TableReaderJdbcNative = {
    val tableReaderJdbcOrig = TableReaderJdbcConfig.load(conf, workflowConf, parent)
    val jdbcConfig = getJdbcConfig(tableReaderJdbcOrig, conf)
    val tableReaderJdbc = tableReaderJdbcOrig.copy(jdbcConfig = jdbcConfig)
    val urlSelector = JdbcUrlSelector(tableReaderJdbc.jdbcConfig)

    new TableReaderJdbcNative(tableReaderJdbc, urlSelector, conf)
  }

  private[core] def getJdbcConfig(tableReaderJdbcConfig: TableReaderJdbcConfig, conf: Config): JdbcConfig = {
    val originConfig = tableReaderJdbcConfig.jdbcConfig
    if (conf.hasPath(FETCH_SIZE_KEY)) {
      originConfig.copy(fetchSize = Option(conf.getInt(FETCH_SIZE_KEY)))
    } else {
      originConfig
    }
  }

  def applyInfoDateExpressionToQuery(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Query = {
    query match {
      case Query.Path(path) => Query.Path(applyInfoDateExpressionToString(path, infoDateBegin, infoDateEnd))
      case Query.Table(table) => Query.Table(applyInfoDateExpressionToString(table, infoDateBegin, infoDateEnd))
      case Query.Sql(sql) => Query.Sql(applyInfoDateExpressionToString(sql, infoDateBegin, infoDateEnd))
      case other => other
    }
  }

  def applyInfoDateExpressionToString(queryStr: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val expr = new DateExprEvaluator()

    expr.setValue("dateFrom", infoDateBegin)
    expr.setValue("dateTo", infoDateEnd)
    expr.setValue("date", infoDateEnd)
    expr.setValue("infoDateBegin", infoDateBegin)
    expr.setValue("infoDateEnd", infoDateEnd)
    expr.setValue("infoDate", infoDateEnd)

    StringUtils.replaceFormattedDateExpression(queryStr, expr)
  }
}
