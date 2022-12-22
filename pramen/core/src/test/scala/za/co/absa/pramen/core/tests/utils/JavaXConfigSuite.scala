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

package za.co.absa.pramen.core.tests.utils

import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.JavaXConfig

class JavaXConfigSuite extends AnyWordSpec with BeforeAndAfter {
  private val testConfig = ConfigFactory.parseResources("test/config/test_javax.conf")

  before {
    clearJavaXProperties()
  }

  after {
    clearJavaXProperties()
  }

  "setJavaXProperties" should {
    "apply JavaX configuration if they do not exist" in {
      val filePath = Paths.get("test2.tmp")
      Files.createFile(filePath)

      JavaXConfig.setJavaXProperties(testConfig)

      Files.delete(filePath)

      assert(System.getProperty("javax.net.ssl.trustStore") == "test2.tmp")
      assert(System.getProperty("javax.net.ssl.trustStorePassword") == "TestPass1")
      assert(System.getProperty("javax.net.ssl.keyStore") == "test2.tmp")
      assert(System.getProperty("javax.net.ssl.keyStorePassword") == "TestPass2")
      assert(System.getProperty("javax.net.ssl.password") == "TestPass3")
      assert(System.getProperty("java.security.auth.login.config") == "test2.tmp")
      assert(System.getProperty("java.security.krb5.conf") == "TestKrbConf")
      assert(System.getProperty("javax.net.debug") == "TestDebug")
    }

    "not not apply JavaX configuration if they DO exist" in {
      System.setProperty("javax.net.ssl.trustStore", "1")
      System.setProperty("javax.net.ssl.trustStorePassword", "2")
      System.setProperty("javax.net.ssl.keyStore", "3")
      System.setProperty("javax.net.ssl.keyStorePassword", "4")
      System.setProperty("javax.net.ssl.password", "5")
      System.setProperty("java.security.auth.login.config", "6")
      System.setProperty("java.security.krb5.conf", "7")
      System.setProperty("javax.net.debug", "8")

      JavaXConfig.setJavaXProperties(testConfig)

      assert(System.getProperty("javax.net.ssl.trustStore") == "1")
      assert(System.getProperty("javax.net.ssl.trustStorePassword") == "2")
      assert(System.getProperty("javax.net.ssl.keyStore") == "3")
      assert(System.getProperty("javax.net.ssl.keyStorePassword") == "4")
      assert(System.getProperty("javax.net.ssl.password") == "5")
      assert(System.getProperty("java.security.auth.login.config") == "6")
      assert(System.getProperty("java.security.krb5.conf") == "7")
      assert(System.getProperty("javax.net.debug") == "8")
    }
  }

  private def clearJavaXProperties(): Unit = {
    System.clearProperty("javax.net.ssl.trustStore")
    System.clearProperty("javax.net.ssl.trustStorePassword")
    System.clearProperty("javax.net.ssl.keyStore")
    System.clearProperty("javax.net.ssl.keyStorePassword")
    System.clearProperty("javax.net.ssl.password")
    System.clearProperty("java.security.auth.login.config")
    System.clearProperty("java.security.krb5.conf")
    System.clearProperty("javax.net.debug")
  }

}
