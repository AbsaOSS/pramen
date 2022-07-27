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

import com.typesafe.config.Config

object JavaXConfig {
  /**
    * Moves JavaX security configuration from the config to system properties
    * if it is not defined there already.
    *
    * This is needed to be executed at least once to initialize secure Kafka when running from Spark.
    *
    * @param conf A configuration.
    */
  def setJavaXProperties(conf: Config): Unit = {
    ConfigUtils.setSystemPropertyFileFallback(conf, "javax.net.ssl.trustStore")
    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.ssl.trustStorePassword")
    ConfigUtils.setSystemPropertyFileFallback(conf, "javax.net.ssl.keyStore")
    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.ssl.keyStorePassword")

    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.ssl.password")

    ConfigUtils.setSystemPropertyFileFallback(conf, "java.security.auth.login.config")
    ConfigUtils.setSystemPropertyStringFallback(conf, "java.security.krb5.conf")

    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.debug")

    ConfigUtils.setSystemPropertyStringFallback(conf, "java.security.krb5.kdc")
    ConfigUtils.setSystemPropertyStringFallback(conf, "java.security.krb5.realm")
    ConfigUtils.setSystemPropertyStringFallback(conf, "java.security.krb5.realm")
  }
}
