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

package za.co.absa.pramen.core.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.impl.ResultSetToRowIterator

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Utils that help fetching data from arbitrary JDBC queries (not just SELECT) using Spark.
  *
  * Pros:
  * - Allows to execute arbitrary queries (e.g. stored procedures).
  * - Results can be arbitrarily large.
  *
  * Cons:
  * - Only limited set of data types supported.
  * - Can be much slower than Spark's JDBC, especially for result sets that don't support scrollable cursors.
  * - Partitioned read (by specifying partitioning column and range) is not supported.
  * - It executes a given query at least 2 times. So, please, do not use it for queries that
  * changes state of the database and is not idempotent (inserts, updates, deletes).
  */
object JdbcNativeUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  final val DEFAULT_CONNECTION_TIMEOUT_SECONDS = 60
  final val JDBC_WORDS_TO_REDACT = Set("password", "secret", "pwd", "token", "key", "api_key")

  /** Returns a JDBC URL and connection by a config. */
  def getConnection(jdbcConfig: JdbcConfig): (String, Connection) = {
    val urlSelector = JdbcUrlSelector(jdbcConfig)

    def getConnectionWithRetries(jdbcConfig: JdbcConfig, retriesLeft: Int): (String, Connection) = {
      val currentUrl = urlSelector.getUrl
      try {
        val connection = getJdbcConnection(jdbcConfig, currentUrl)

        (currentUrl, connection)
      } catch {
        case NonFatal(ex) if retriesLeft > 0  =>
          val nextUrl = urlSelector.getNextUrl
          log.error(s"Error connecting to $currentUrl. Retries left = $retriesLeft. Retrying with $nextUrl...", ex)
          getConnectionWithRetries(jdbcConfig, retriesLeft - 1)
        case NonFatal(ex) if retriesLeft <= 0 => throw ex
      }
    }

    jdbcConfig.retries match {
      case Some(n) => getConnectionWithRetries(jdbcConfig, n)
      case None    => getConnectionWithRetries(jdbcConfig, urlSelector.getNumberOfUrls - 1)
    }
  }

  /** Gets the number of records returned by a given query. */
  def getJdbcNativeRecordCount(jdbcConfig: JdbcConfig,
                               url: String,
                               query: String)
                              (implicit spark: SparkSession): Long = {
    val resultSet = getResultSet(jdbcConfig, url, query)
    getResultSetCount(resultSet)
  }

  /** Gets a dataframe given a JDBC query */
  def getJdbcNativeDataFrame(jdbcConfig: JdbcConfig,
                             url: String,
                             query: String)
                            (implicit spark: SparkSession): DataFrame = {

    // Executing the query
    val rs = getResultSet(jdbcConfig, url, query)
    val driverIterator = new ResultSetToRowIterator(rs, jdbcConfig.sanitizeDateTime, jdbcConfig.incorrectDecimalsAsString)
    val schema = JdbcSparkUtils.addMetadataFromJdbc(driverIterator.getSchema, rs.getMetaData)

    driverIterator.close()

    val rdd = spark.sparkContext.parallelize(Seq(query)).flatMap(q => {
      new ResultSetToRowIterator(getResultSet(jdbcConfig, url, q), jdbcConfig.sanitizeDateTime, jdbcConfig.incorrectDecimalsAsString)
    })

    spark.createDataFrame(rdd, schema)
  }

  def withResultSet(jdbcUrlSelector: JdbcUrlSelector,
                    query: String,
                    retries: Int)
                   (action: ResultSet => Unit): Unit = {
    val (connection, _) = jdbcUrlSelector.getWorkingConnection(retries)

    try {
      connection.setAutoCommit(false)

      val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      try {
        val resultSet = statement.executeQuery(query)
        try {
          action(resultSet)
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private[core] def getResultSetCount(resultSet: ResultSet): Long = {
    val countOpt = Try {
      // The fast way of getting record count from a scrollable cursor
      resultSet.last()
      resultSet.getRow.toLong
    }.toOption
    val count = countOpt.getOrElse({
      // The slow way if the underlying RDBMS doesn't support scrollable cursors for such types of queries.
      var i = 0L
      while (resultSet.next()) {
        i += 1
      }
      i
    })

    resultSet.close()
    count
  }

  private def getResultSet(jdbcConfig: JdbcConfig,
                           url: String,
                           query: String): ResultSet = {
    Class.forName(jdbcConfig.driver)
    val connection = getJdbcConnection(jdbcConfig, url)

    val statement = try {
      connection.setAutoCommit(jdbcConfig.autoCommit)

      if (jdbcConfig.driver == "org.postgresql.Driver")
        // Special handling of PostgreSQL driver that loads.
        // PostgreSQL loads full query results info memory ignoring fetch sizes and other driver options by default.
        // This is the only cursor that makes it respect fetch sizes.
        // If we encounter more exceptions, we can improve this 'patch' into a properly handling method.
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      else
        connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    } catch {
      case _: java.sql.SQLException =>
        // Fallback with more permissible result type.
        // JDBC sources should automatically downgrade result type, but Denodo driver doesn't do that.
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      case NonFatal(ex)             =>
        throw ex
    }

    jdbcConfig.fetchSize.foreach(n =>
      statement.setFetchSize(n)
    )

    statement.executeQuery(query)
  }

  private def getJdbcConnection(jdbcConfig: JdbcConfig, url: String): Connection = {
    Class.forName(jdbcConfig.driver)
    val properties = new Properties()
    properties.put("driver", jdbcConfig.driver)
    jdbcConfig.user.foreach(db => properties.put("user", db))
    jdbcConfig.password.foreach(db => properties.put("password", db))
    jdbcConfig.database.foreach(db => properties.put("database", db))
    jdbcConfig.extraOptions.foreach {
      case (k, v) =>
        properties.put(k, v)
    }

    DriverManager.setLoginTimeout(jdbcConfig.connectionTimeoutSeconds.getOrElse(DEFAULT_CONNECTION_TIMEOUT_SECONDS))
    DriverManager.getConnection(url, properties)
  }
}
