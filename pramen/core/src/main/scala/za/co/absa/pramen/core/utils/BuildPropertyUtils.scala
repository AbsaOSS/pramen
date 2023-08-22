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

import za.co.absa.pramen.api.common.BuildPropertiesRetriever

import java.util.Properties

class BuildPropertyUtils extends BuildPropertiesRetriever {
  private val properties = new Properties()
  private val buildVersionKey = "build.version"
  private val buildTimestampKey = "build.timestamp"

  /** Returns the version of the build. */
  override lazy val buildVersion: String = properties.getProperty(buildVersionKey)

  /** Returns the version of the build. */
  override lazy val buildTimestamp: String = properties.getProperty(buildTimestampKey)

  /** Returns full version to refer to in the user interface. */
  override def getFullVersion: String = {
    val version = buildVersion
    if (version.contains("SNAPSHOT")) {
      val builtAt = buildTimestamp
      s"$version built $builtAt"
    } else {
      version
    }
  }

  loadConfig()

  private def loadConfig(): Unit = {
    val is = getClass.getResourceAsStream("/pramen_build.properties")
    try properties.load(is)
    finally if (is != null) is.close()
  }
}

// This is needed for ease of access to the class definition from components that do not include 'pramen-core' as a dependency.
// See za.co.absa.pramen.buildinfo.BuildPropertiesRetriever
object BuildPropertyUtils {
  val instance = new BuildPropertyUtils()
}
