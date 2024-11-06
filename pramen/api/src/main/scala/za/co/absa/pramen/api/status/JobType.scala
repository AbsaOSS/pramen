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

package za.co.absa.pramen.api.status

import com.typesafe.config.Config
import za.co.absa.pramen.api.jobdef.{SinkTable, SourceTable, TransferTable}

sealed trait JobType {
  def name: String
}

object JobType {
  case class Ingestion(
                        sourceName: String,
                        sourceTable: SourceTable,
                        sourceConfig: Config
                      ) extends JobType {
    override def name: String = "ingestion"
  }

  case class Transformation(
                             factoryClass: String
                           ) extends JobType {
    override def name: String = "transformation"
  }

  case class PythonTransformation(
                                   pythonClass: String
                                 ) extends JobType {
    override def name: String = "python_transformation"
  }

  case class Sink(
                   sinkName: String,
                   sinkTable: SinkTable,
                   sinkConfig: Config
                 ) extends JobType {
    override def name: String = "sink"
  }

  case class Transfer(
                       sourceName: String,
                       sourceConfig: Config,
                       sinkName: String,
                       sinkConfig: Config,
                       transferTable: TransferTable
                     ) extends JobType {
    override def name: String = "transfer"
  }
}
