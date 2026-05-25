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

import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.io.File
import java.net.URLClassLoader
import java.sql.{Connection, Driver, SQLException}
import java.util.Properties

trait JdbcUrlSelector extends AutoCloseable {
  /** The JDBC configuration used for the selector. */
  def jdbcConfig: JdbcConfig

  /** Get current URL to try. */
  def getUrl: String

  /** Get next URL to try. */
  def getNextUrl: String

  /** Get he number of URLs available. */
  def getNumberOfUrls: Int

  /** Does the JDBC configuration have fallback URLs. */
  def haveFallbackUrls: Boolean

  /** Log current JDBC connection settings. */
  def logConnectionSettings(): Unit

  /** Returns an url only if it can be successfully connected to. */
  @throws[SQLException]
  def getWorkingUrl: String

  /** Returns properties for the JDBC connection. */
  def getProperties: Properties

  /** Returns the path to the Driver JAR if available. */
  def jdbcDriverJarPath: Option[String]

  /** Returns a dynamically pre-loaded driver if available. */
  def getLoadedDriver: Option[Driver]

  /** Returns an JDBC connection with the URL that has successfully connected. Can reuse existing connection */
  @throws[SQLException]
  def getConnection: (Connection, String)

  /** Returns an new JDBC connection with the URL that has successfully connected. */
  def getNewConnection(retriesLeft: Int): (Connection, String)
}

object JdbcUrlSelector {
  def apply(jdbcConfig: JdbcConfig): JdbcUrlSelector = new JdbcUrlSelectorImpl(None, jdbcConfig)

  def apply(jdbcDriverJarPath: Option[String], jdbcConfig: JdbcConfig): JdbcUrlSelector = new JdbcUrlSelectorImpl(jdbcDriverJarPath, jdbcConfig)

  def loadDriver(driverJarPath: String, driverClassName: String): Driver = {
    val jarFile = new File(driverJarPath)
    val jarURL = jarFile.toURI.toURL

    val loader = new URLClassLoader(
      Array(jarURL),
      this.getClass.getClassLoader
    )

    // Load driver class
    val driverClass = loader.loadClass(driverClassName)
    val driver = driverClass.getDeclaredConstructor().newInstance().asInstanceOf[Driver]
    driver
  }
}
