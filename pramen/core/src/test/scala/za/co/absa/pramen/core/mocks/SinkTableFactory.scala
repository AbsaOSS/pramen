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

package za.co.absa.pramen.core.mocks

import com.typesafe.config.{Config, ConfigFactory}
import za.co.absa.pramen.core.pipeline.{SinkTable, TransformExpression}

object SinkTableFactory {
  def getDummySinkTable(metaTableName: String = "table1",
                        outputTableName: Option[String] = None,
                        conf: Config = ConfigFactory.empty(),
                        sinkFromExpr: Option[String] = None,
                        sinkToExpr: Option[String] = None,
                        warnMaxExecutionTimeSeconds: Option[Int] = None,
                        transformations: Seq[TransformExpression] = Nil,
                        filters: Seq[String] = Nil,
                        columns: Seq[String] = Nil,
                        options: Map[String, String] = Map.empty[String, String],
                        overrideConf: Option[Config] = None): SinkTable = {
    SinkTable(metaTableName, outputTableName, conf, sinkFromExpr, sinkToExpr, warnMaxExecutionTimeSeconds, transformations, filters, columns, options, overrideConf)
  }
}
