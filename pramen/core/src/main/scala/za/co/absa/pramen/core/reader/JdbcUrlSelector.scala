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

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.util.{Failure, Random, Success, Try}

class JdbcUrlSelector(jdbcConfig: JdbcConfig) {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val allUrls = (jdbcConfig.primaryUrl ++ jdbcConfig.fallbackUrls).toSeq
  private val numberOfUrls = allUrls.size
  private var urlPool = allUrls

  validate()

  private var currentUrl: String = getFirstUrl

  def getUrl: String = currentUrl

  def getNextUrl: String = {
    val url = if (!haveFallbackUrls) {
      log.info(s"No fallback URLs defined. Retrying with the same URL: $currentUrl")
      getUrl
    } else {
      getRandomFallbackUrl
    }
    addSelectedUrl(url)
    url
  }

  def getNumberOfUrls: Int = numberOfUrls

  def haveFallbackUrls: Boolean = numberOfUrls > 1

  def logConnectionSettings(): Unit = {
    log.info(s"JDBC Configuration:")
    log.info(s"Driver:            ${jdbcConfig.driver}")
    log.info(s"URL:               $currentUrl")
    jdbcConfig.database.foreach(d => log.info(s"Database:          $d"))
    log.info(s"User:              ${jdbcConfig.user}")
    log.info(s"Password:          [redacted]")

    if (numberOfUrls > 1) {
      log.info("URL redundancy configuration:")
      jdbcConfig.primaryUrl.foreach(s => log.info(s"Primary URL:       $s"))
      jdbcConfig.fallbackUrls.zipWithIndex.foreach { case (s, i) => log.info(s"Pool URL ${i + 1}:        $s") }
    }
    if (jdbcConfig.extraOptions.nonEmpty) {
      log.info("JDBC extra properties:")
      jdbcConfig.extraOptions.foreach{ case (k,v) => log.info(s"$k = $v")}
    }
  }

  /** Returns an url only if it can be successfully connected to. */
  @throws[SQLException]
  def getWorkingUrl(retriesLeft: Int): String = {
    val (connection, url) = getWorkingConnection(retriesLeft)
    connection.close()
    url
  }

  /** Returns an JDBC connection with the URL that has successfully connected. */
  @throws[SQLException]
  def getWorkingConnection(retriesLeft: Int): (Connection, String) = {
    val currentUrl = getUrl

    Try {
      Class.forName(jdbcConfig.driver)

      val properties = new Properties()
      properties.put("driver", jdbcConfig.driver)
      properties.put("user", jdbcConfig.user)
      properties.put("password", jdbcConfig.password)
      jdbcConfig.database.foreach(db => properties.put("database", db))
      jdbcConfig.extraOptions.foreach{
        case (k, v) => properties.put(k, v)
      }

      DriverManager.getConnection(currentUrl, properties)
    } match {
      case Success(connection) => (connection, currentUrl)
      case Failure(ex) =>
        if (retriesLeft > 0) {
          val newUrl = getNextUrl
          log.error(s"JDBC connection error for $currentUrl. Retries left: $retriesLeft. Retrying...", ex)
          log.info(s"Trying URL: $newUrl")
          getWorkingConnection(retriesLeft - 1)
        } else {
          throw ex
        }
    }
  }

  private def getFirstUrl: String = {
    val url = jdbcConfig.primaryUrl match {
      case Some(url) =>
        log.info(s"Selected primary JDBC URL: $url")
        url
      case None =>
        val url = getRandomFallbackUrl
        log.info(s"Selected a JDBC URL from the pool: $url")
        url
    }
    addSelectedUrl(url)
    url
  }

  private def getRandomFallbackUrl: String = {
    if (numberOfUrls < 1) {
      throw new RuntimeException(s"No JDBC URLs specified.")
    } else if (numberOfUrls == 1) {
      val url = allUrls.head
      log.info(s"Selected the only available JDBC URL: $url")
      url
    } else {
      val n = Random.nextInt(urlPool.size)
      val url = urlPool(n)
      log.info(s"Selected new URL from the pool: $url")
      url
    }
  }

  private def addSelectedUrl(url: String): Unit = {
    currentUrl = url
    urlPool = urlPool.filterNot(u => u == url)
    if (urlPool.isEmpty) {
      urlPool = allUrls
    }
  }

  private def validate(): Unit = {
    if (numberOfUrls < 1) {
      throw new IllegalArgumentException(s"No JDBC URLs specified.")
    }

    if (allUrls.exists(_.isEmpty)) {
      throw new IllegalArgumentException(s"Empty string is not a valid JDBC URL.")
    }
  }

}
