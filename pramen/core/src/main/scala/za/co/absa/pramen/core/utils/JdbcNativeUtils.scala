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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.impl.ResultSetToRowIterator

import java.net.URLClassLoader
import java.sql._
import java.util.Properties
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Utils that help to fetch data from arbitrary JDBC queries (not just SELECT) using Spark.
  *
  * Pros:
  * - Allows executing arbitrary queries (e.g. stored procedures).
  * - Results can be arbitrarily large.
  *
  * Cons:
  * - Only a limited set of data types supported.
  * - Can be much slower than Spark's JDBC, especially for result sets that don't support scrollable cursors.
  * - Partitioned read (by specifying partitioning column and range) is not supported.
  * - It executes a given query at least 2 times. So, please, do not use it for queries that
  *  change the state of the database and is not idempotent (inserts, updates, deletes).
  */
object JdbcNativeUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  final val DEFAULT_CONNECTION_TIMEOUT_SECONDS = 60
  final val JDBC_WORDS_TO_REDACT = Set("password", "secret", "pwd", "token", "key", "api_key")
  final val DRIVERS_SUPPORT_ARRAYS = Set("org.postgresql.Driver", "org.hsqldb.jdbc.JDBCDriver")

  /** Returns a JDBC URL and connection by a config. */
  def getConnection(jdbcConfig: JdbcConfig, driverOpt: Option[Driver]): (String, Connection) = {
    val urlSelector = JdbcUrlSelector(jdbcConfig)

    def getConnectionWithRetries(jdbcConfig: JdbcConfig, retriesLeft: Int): (String, Connection) = {
      val currentUrl = urlSelector.getUrl
      try {
        val connection = getJdbcConnection(jdbcConfig, currentUrl, driverOpt)

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
  def getJdbcNativeRecordCount(urlSelector: JdbcUrlSelector,
                               query: String): Long = {
    val resultSet = getResultSet(urlSelector, query)
    getResultSetCount(resultSet)
  }

  /** Gets a dataframe given a JDBC query */
  def getJdbcNativeDataFrame(urlSelector: JdbcUrlSelector,
                             query: String)
                            (implicit spark: SparkSession): DataFrame = {
    val jdbcConfig = urlSelector.jdbcConfig
    val arraysSupported = DRIVERS_SUPPORT_ARRAYS.contains(jdbcConfig.driver)
    val rs = getResultSet(urlSelector, query)

    val schema = UsingUtils.using(new ResultSetToRowIterator(rs, None, None, jdbcConfig.sanitizeDateTime, jdbcConfig.incorrectDecimalsAsString, arraysSupported)) { driverIterator =>
      JdbcSparkUtils.addMetadataFromJdbc(driverIterator.getSchema, rs.getMetaData)
    }

    val url = urlSelector.getUrl

    getJdbcNativeDataFrameSerializable(schema, url, query, urlSelector.jdbcConfig, urlSelector.jdbcDriverJarPath, arraysSupported)
  }

  /** This method uses only serializable parameters because they are going to be passed to the RDD */
  private def getJdbcNativeDataFrameSerializable(schema: StructType,
                                         url: String,
                                         query: String,
                                         jdbcConfig: JdbcConfig,
                                         jdbcDriverJarPath: Option[String],
                                         arraysSupported: Boolean)
                            (implicit spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(query)).flatMap(q => {
      val (rs, connection, classLoaderOpt) = getResultSetForRDD(jdbcConfig, url, q, jdbcDriverJarPath)
      new ResultSetToRowIterator(rs, Option(connection), classLoaderOpt, jdbcConfig.sanitizeDateTime, jdbcConfig.incorrectDecimalsAsString, arraysSupported)
    })

    spark.createDataFrame(rdd, schema)
  }

  def withResultSet(jdbcUrlSelector: JdbcUrlSelector,
                    query: String)
                   (action: ResultSet => Unit): Unit = {
    import UsingUtils.Implicits._

    val retries = jdbcUrlSelector.jdbcConfig.retries.getOrElse(jdbcUrlSelector.getNumberOfUrls)

    val (conn, _) = jdbcUrlSelector.getConnection

    for {
      statement <- conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      resultSet <- executeQuery(statement, query, retries)
    } {
      action(resultSet)
    }
  }

  private[core] def executeQuery(statement: Statement, query: String, retriesLeft: Int): ResultSet = {
    var remainingRetries = retriesLeft

    while (true) {
      try {
        return statement.executeQuery(query)
      } catch {
        case ex: SQLException if remainingRetries > 0 && ex.getMessage.contains("Index: 1, Size: 1") =>
          log.error(s"Error executing query. Retries left = $remainingRetries. Retrying...", ex)
          remainingRetries -= 1
        case ex: Throwable  =>
          throw ex
      }
    }
    throw new IllegalStateException("Unreachable")
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

  private[core] def getResultSet(urlSelector: JdbcUrlSelector,
                           query: String): ResultSet = {
    val (connection, _) = urlSelector.getConnection

    getResultSet(connection, urlSelector.jdbcConfig, query)
  }

  private[core] def getResultSetForRDD(jdbcConfig: JdbcConfig,
                                       url: String,
                                       query: String,
                                       jdbcDriverJarPath: Option[String]): (ResultSet, Connection, Option[URLClassLoader]) = {
    val dynamicDriverOpt = jdbcDriverJarPath.map(path => JdbcUrlSelector.loadDriver(path, jdbcConfig.driver))
    val connection = getJdbcConnection(jdbcConfig, url, dynamicDriverOpt.map(_.driver))
    val rs = getResultSet(connection, jdbcConfig, query)
    (rs, connection, dynamicDriverOpt.map(_.classLoader))
  }

  private[core] def getResultSet(connection: Connection,
                           jdbcConfig: JdbcConfig,
                           query: String): ResultSet = {
    val statement = try {
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

  private[core] def getJdbcConnection(jdbcConfig: JdbcConfig, url: String, driverOpt: Option[Driver]): Connection = {
    val properties = new Properties()
    jdbcConfig.user.foreach(db => properties.put("user", db))
    jdbcConfig.password.foreach(db => properties.put("password", db))
    jdbcConfig.database.foreach(db => properties.put("database", db))
    jdbcConfig.extraOptions.foreach {
      case (k, v) =>
        properties.put(k, v)
    }

    def getConnectionFromDriver(driver: Driver): Connection = {
      val conn = driver.connect(url, properties)
      if (conn == null) {
        throw new SQLException(s"Driver ${jdbcConfig.driver} returned null connection for URL: $url")
      }
      conn
    }

    DriverManager.setLoginTimeout(jdbcConfig.connectionTimeoutSeconds.getOrElse(DEFAULT_CONNECTION_TIMEOUT_SECONDS))

    val connection = driverOpt match {
      case Some(driver) =>
        getConnectionFromDriver(driver)
      case None =>
        Class.forName(jdbcConfig.driver)
        DriverManager.getConnection(url, properties)
    }

    jdbcConfig.autoCommit.foreach { autoCommit =>
      try {
        log.info(s"setAutoCommit($autoCommit)")
        connection.setAutoCommit(autoCommit)
      } catch {
        // Some drivers, like Databricks JDBC driver throw an exception on setAutoCommit(false)
        case _: Throwable => log.info(s"The JDBC driver does not support 'setAutoCommit($autoCommit)'")
      }
    }
    connection
  }
}
