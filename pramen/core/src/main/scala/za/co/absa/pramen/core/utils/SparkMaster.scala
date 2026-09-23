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

sealed trait SparkMaster

object SparkMaster {
  case class Local(spec: String) extends SparkMaster

  case class Standalone(url: String) extends SparkMaster

  case class Yarn(deploymentMode: YarnDeploymentMode) extends SparkMaster

  case class Kubernetes(url: String) extends SparkMaster

  case object Databricks extends SparkMaster

  case class Unknown(master: String) extends SparkMaster
}
