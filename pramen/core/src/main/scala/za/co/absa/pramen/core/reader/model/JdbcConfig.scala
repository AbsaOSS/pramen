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

package za.co.absa.pramen.core.reader.model

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

import scala.util.Try

case class JdbcConfig(
                       driver: String,
                       primaryUrl: Option[String],
                       fallbackUrls: Seq[String] = Nil,
                       database: Option[String] = None,
                       user: Option[String] = None,
                       password: Option[String] = None,
                       retries: Option[Int] = None,
                       fetchSize: Option[Int] = None,
                       autoCommit: Boolean = false,
                       connectionTimeoutSeconds: Option[Int] = None,
                       sanitizeDateTime: Boolean = true,
                       incorrectDecimalsAsString: Boolean = false,
                       extraOptions: Map[String, String] = Map.empty[String, String]
                     )

object JdbcConfig {
  val JDBC_CONNECTION_DRIVER = "jdbc.driver"
  val JDBC_CONNECTION_STRING = "jdbc.connection.string"
  val JDBC_CONNECTION_PRIMARY_URL = "jdbc.connection.primary.url"
  val JDBC_CONNECTION_PRIMARY_URL_SHORT = "jdbc.url"
  val JDBC_FALLBACK_URL_PREFIX = "jdbc.fallback.url"
  val JDBC_DATABASE = "jdbc.database"
  val JDBC_USER = "jdbc.user"
  val JDBC_PASSWORD = "jdbc.password"
  val JDBC_RETRIES = "jdbc.retries"
  val JDBC_FETCH_SIZE = "jdbc.fetchsize"
  val JDBC_AUTOCOMMIT = "jdbc.autocommit"
  val JDBC_CONNECTION_TIMEOUT = "jdbc.connection.timeout"
  val JDBC_SANITIZE_DATETIME = "jdbc.sanitize.datetime"
  val JDBC_INCORRECT_PRECISION_AS_STRING = "jdbc.incorrect.precision.as.string"
  val JDBC_EXTRA_OPTIONS_PREFIX = "jdbc.option"

  def load(conf: Config, parent: String = ""): JdbcConfig = {
    validateConf(conf, parent)

    val pu1 = ConfigUtils.getOptionString(conf, JDBC_CONNECTION_STRING)
    val pu2 = ConfigUtils.getOptionString(conf, JDBC_CONNECTION_PRIMARY_URL)
    val pu3 = ConfigUtils.getOptionString(conf, JDBC_CONNECTION_PRIMARY_URL_SHORT)

    val pu = Seq(pu1, pu2, pu3).flatten.distinct

    val primaryUrl = if (pu.isEmpty) {
      None
    } else if (pu.size == 1) {
      Some(pu.head)
    } else {
      throw new IllegalArgumentException(s"Please, define either $JDBC_CONNECTION_STRING, $JDBC_CONNECTION_PRIMARY_URL or $JDBC_CONNECTION_PRIMARY_URL_SHORT")
    }

    JdbcConfig(
      driver = conf.getString(JDBC_CONNECTION_DRIVER),
      primaryUrl = primaryUrl,
      ConfigUtils.getListStringsByPrefix(conf, JDBC_FALLBACK_URL_PREFIX),
      database = ConfigUtils.getOptionString(conf, JDBC_DATABASE),
      user = ConfigUtils.getOptionString(conf, JDBC_USER),
      password = ConfigUtils.getOptionString(conf, JDBC_PASSWORD),
      retries = ConfigUtils.getOptionInt(conf, JDBC_RETRIES),
      fetchSize = ConfigUtils.getOptionInt(conf, JDBC_FETCH_SIZE),
      autoCommit = ConfigUtils.getOptionBoolean(conf, JDBC_AUTOCOMMIT).getOrElse(false),
      connectionTimeoutSeconds = ConfigUtils.getOptionInt(conf, JDBC_CONNECTION_TIMEOUT),
      sanitizeDateTime = ConfigUtils.getOptionBoolean(conf, JDBC_SANITIZE_DATETIME).getOrElse(true),
      incorrectDecimalsAsString = ConfigUtils.getOptionBoolean(conf, JDBC_INCORRECT_PRECISION_AS_STRING).getOrElse(false),
      extraOptions = ConfigUtils.getExtraOptions(conf, JDBC_EXTRA_OPTIONS_PREFIX)
    )
  }

  def loadOption(conf: Config, parent: String = ""): Option[JdbcConfig] = {
    if (Try(validateConf(conf, parent)).isSuccess) {
      Some(load(conf, parent))
    } else {
      None
    }
  }

  private def validateConf(conf: Config, parent: String): Unit = {
    ConfigUtils.validatePathsExistence(conf,
      parent,
      JDBC_CONNECTION_DRIVER :: Nil)
  }
}