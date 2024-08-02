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
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.JdbcNativeUtils.{DEFAULT_CONNECTION_TIMEOUT_SECONDS, JDBC_WORDS_TO_REDACT}

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.util.{Failure, Random, Success, Try}

class JdbcUrlSelectorImpl(val jdbcConfig: JdbcConfig) extends JdbcUrlSelector{
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
    log.info(s"Driver:             ${jdbcConfig.driver}")
    log.info(s"URL:                $currentUrl")

    jdbcConfig.database.foreach(d  => log.info(s"Database:           $d"))
    jdbcConfig.user.foreach(user   => log.info(s"User:               $user"))
    jdbcConfig.password.filter(_.nonEmpty).foreach(_  => log.info(s"Password:           [redacted]"))
    jdbcConfig.fetchSize.foreach(n => log.info(s"Fetch size:         $n"))

    log.info(s"Auto commit:        ${jdbcConfig.autoCommit}")
    log.info(s"Sanitize date/time: ${jdbcConfig.sanitizeDateTime}")

    jdbcConfig.retries.foreach(n =>   log.info(s"Retry attempts:     $n"))
    jdbcConfig.connectionTimeoutSeconds.foreach(n => log.info(s"Timeout:            $n seconds"))

    if (numberOfUrls > 1) {
      log.info("URL redundancy configuration:")
      jdbcConfig.primaryUrl.foreach(s => log.info(s"Primary URL:        $s"))
      jdbcConfig.fallbackUrls.zipWithIndex.foreach { case (s, i) => log.info(s"Pool URL ${i + 1}:         $s") }
    }
    if (jdbcConfig.extraOptions.nonEmpty) {
      log.info("JDBC extra properties:")
      ConfigUtils.renderExtraOptions(jdbcConfig.extraOptions, JDBC_WORDS_TO_REDACT)(s => log.info(s))
    }
  }

  @throws[SQLException]
  def getWorkingUrl(retriesLeft: Int): String = {
    val (connection, url) = getWorkingConnection(retriesLeft)
    connection.close()
    url
  }

  override def getProperties: Properties = {
    val properties = new Properties()
    properties.put("driver", jdbcConfig.driver)
    jdbcConfig.user.foreach(db => properties.put("user", db))
    jdbcConfig.password.foreach(db => properties.put("password", db))
    jdbcConfig.database.foreach(db => properties.put("database", db))
    jdbcConfig.extraOptions.foreach {
      case (k, v) => properties.put(k, v)
    }

    properties
  }

  @throws[SQLException]
  def getWorkingConnection(retriesLeft: Int): (Connection, String) = {
    val currentUrl = getUrl

    Try {
      Class.forName(jdbcConfig.driver)

      val properties = getProperties

      DriverManager.setLoginTimeout(jdbcConfig.connectionTimeoutSeconds.getOrElse(DEFAULT_CONNECTION_TIMEOUT_SECONDS))
      DriverManager.getConnection(currentUrl, properties)
    } match {
      case Success(connection) => (connection, currentUrl)
      case Failure(ex)         =>
        if (retriesLeft > 1) {
          val newUrl = getNextUrl
          log.error(s"JDBC connection error for $currentUrl. Retries left: ${retriesLeft - 1}. Retrying...", ex)
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
      case None      =>
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
