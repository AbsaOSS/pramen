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

package za.co.absa.pramen.core.databricks

import com.typesafe.config.Config

trait DatabricksClient {
  def createFile(content: String, destination: String, overwrite: Boolean = false): Unit

  def runTransientJob(jobDefinition: Map[String, Any]): Unit
}

object DatabricksClient {
  val PRAMEN_PY_DATABRICKS_HOST_KEY = "pramen.py.databricks.host"
  val PRAMEN_PY_DATABRICKS_TOKEN_KEY = "pramen.py.databricks.token"

  def canCreate(conf: Config): Boolean = {
    val hasHost = conf.hasPath(PRAMEN_PY_DATABRICKS_HOST_KEY)
    val hasToken = conf.hasPath(PRAMEN_PY_DATABRICKS_TOKEN_KEY)

    hasHost && hasToken
  }

  def fromConfig(conf: Config): DatabricksClient = {
    val host = conf.getString(PRAMEN_PY_DATABRICKS_HOST_KEY)
    val token = conf.getString(PRAMEN_PY_DATABRICKS_TOKEN_KEY)

    new DatabricksClientImpl(host, token)
  }
}
