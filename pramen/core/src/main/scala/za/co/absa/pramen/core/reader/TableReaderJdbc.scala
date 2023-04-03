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
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.core.sql.{SqlColumnType, SqlConfig, SqlGenerator}
import za.co.absa.pramen.core.utils.JdbcNativeUtils.JDBC_WORDS_TO_REDACT
import za.co.absa.pramen.core.utils.{ConfigUtils, TimeUtils}

import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class TableReaderJdbc(jdbcReaderConfig: TableReaderJdbcConfig,
                      conf: Config
                     )(implicit spark: SparkSession) extends TableReader {

  // ToDo Pass proper parent path in 0.9.0

  private val log = LoggerFactory.getLogger(this.getClass)

  private val extraOptions = ConfigUtils.getExtraOptions(conf, "option")

  private val jdbcUrlSelector = JdbcUrlSelector(jdbcReaderConfig.jdbcConfig)

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

  private[core] def getJdbcConfig: TableReaderJdbcConfig = jdbcReaderConfig

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val tableName = getTableName(query)

    val sql = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getCountQuery(tableName, infoDateBegin, infoDateEnd)
    } else {
      sqlGen.getCountQuery(tableName)
    }

    val start = Instant.now
    val count = getWithRetry[Long](sql, jdbcRetries)(df =>
      // Take first column of the first row, use BigDecimal as the most generic numbers parser,
      // and then convert to Long. This is a safe way if the output is like "0E-11".
      BigDecimal(df.collect()(0)(0).toString).toLong
    )
    val finish = Instant.now
    log.info(s"Record count: $count. Query elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
    count
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    val tableName = getTableName(query)

    val sql = if (jdbcReaderConfig.hasInfoDate) {
      sqlGen.getDataQuery(tableName, infoDateBegin, infoDateEnd, Nil, jdbcReaderConfig.limitRecords)
    } else {
      sqlGen.getDataQuery(tableName, Nil, jdbcReaderConfig.limitRecords)
    }

    val df = getWithRetry[DataFrame](sql, jdbcRetries)(df => {
      // Make sure connection to the server is made without fetching the data
      log.debug(df.schema.treeString)
      df
    })

    df
  }

  private def getTableName(query: Query): String = {
    query match {
      case Query.Table(tableName) => tableName
      case other => throw new IllegalArgumentException(s"'${other.name}' is not supported by the JDBC reader. Use 'table' instead.")
    }
  }

  @tailrec
  private def getWithRetry[T](sql: String, retriesLeft: Int)(f: DataFrame => T): T = {
    Try {
      val df = getDataFrame(sql)
      f(df)
    } match {
      case Success(result) => result
      case Failure(ex) =>
        val currentUrl = jdbcUrlSelector.getUrl
        if (retriesLeft > 1) {
          val nextUrl = jdbcUrlSelector.getNextUrl
          log.error(s"JDBC connection error for $currentUrl. Retries left: ${retriesLeft - 1}. Retrying...", ex)
          log.info(s"Trying URL: $nextUrl")
          getWithRetry(sql, retriesLeft - 1)(f)
        } else {
          log.error(s"JDBC connection error for $currentUrl. No connection attempts left.", ex)
          throw ex
        }
    }
  }

  private def getDataFrame(sql: String): DataFrame = {
    log.info(s"JDBC Query: $sql")
    val qry = sqlGen.getDtable(sql)

    if (log.isDebugEnabled) {
      log.debug(s"Sending to JDBC: $qry")
    }

    val databaseOptions = getOptions("database", jdbcReaderConfig.jdbcConfig.database)

    val connectionOptions = Map[String, String](
      "url" -> jdbcUrlSelector.getUrl,
      "driver" -> jdbcReaderConfig.jdbcConfig.driver,
      "user" -> jdbcReaderConfig.jdbcConfig.user,
      "password" -> jdbcReaderConfig.jdbcConfig.password,
      "dbtable" -> qry
    ) ++
      databaseOptions ++
      jdbcReaderConfig.jdbcConfig.extraOptions ++
      extraOptions

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
      getCorrectedDecimalsSchema(df).foreach(schema =>
        df = spark
          .read
          .format("jdbc")
          .options(connectionOptions)
          .option("customSchema", schema)
          .load()
      )
    }

    if (log.isDebugEnabled) {
      log.debug(df.schema.treeString)
    }

    if (jdbcReaderConfig.saveTimestampsAsDates) {
      df = convertTimestampToDates(df)
    }

    jdbcReaderConfig.limitRecords match {
      case Some(limit) => df.limit(limit)
      case None => df
    }
  }

  private def getOptions(optionName: String, optionValue: Option[Any]): Map[String, String] = {
    optionValue match {
      case Some(value) =>
        log.info(s"JDBC reader $optionName = $value")
        Map[String, String](optionName -> value.toString)
      case None =>
        Map[String, String]()
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

  private def convertTimestampToDates(df: DataFrame): DataFrame = {
    val dateColumns = new ListBuffer[String]

    val newFields = df.schema.fields.map(fld => {
      fld.dataType match {
        case TimestampType =>
          dateColumns += fld.name
          col(fld.name).cast(DateType).as(fld.name)
        case _ =>
          col(fld.name)
      }
    })

    if (dateColumns.nonEmpty) {
      log.info(s"The following fields have been converted to Date: ${dateColumns.mkString(", ")}")
      df.select(newFields: _*)
    } else {
      log.debug("No timestamp fields found in the dataset.")
      df
    }
  }

  private def getCorrectedDecimalsSchema(df: DataFrame): Option[String] = {
    val newSchema = new ListBuffer[String]

    df.schema.fields.foreach(field => {
      field.dataType match {
        case t: DecimalType if t.scale == 0 && t.precision <= 9 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to int")
          newSchema += s"${field.name} integer"
        case t: DecimalType if t.scale == 0 && t.precision <= 18 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to long")
          newSchema += s"${field.name} long"
        case t: DecimalType if t.scale >= 18 =>
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to decimal(38, 18)")
          newSchema += s"${field.name} decimal(38, 18)"
        case t: DecimalType if jdbcReaderConfig.correctDecimalsFixPrecision && t.scale > 0 =>
          val fixedPrecision = if (t.precision + t.scale > 38) 38 else t.precision + t.scale
          log.info(s"Correct '${field.name}' (prec=${t.precision}, scale=${t.scale}) to decimal($fixedPrecision, ${t.scale})")
          newSchema += s"${field.name} decimal($fixedPrecision, ${t.scale})"
        case _ =>
          field
      }
    })
    if (newSchema.nonEmpty) {
      val customSchema = newSchema.mkString(", ")
      log.info(s"Custom schema: $customSchema")
      Some(customSchema)
    } else {
      None
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

    new TableReaderJdbc(jdbcTableReaderConfig, conf)
  }
}
