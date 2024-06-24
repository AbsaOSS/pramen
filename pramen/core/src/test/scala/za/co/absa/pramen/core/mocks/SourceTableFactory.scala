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
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.pipeline.{SourceTable, TransformExpression}

object SourceTableFactory {
  def getDummySourceTable(metaTableName: String = "table1",
                          query: Query = Query.Table("dummy"),
                          conf: Config = ConfigFactory.empty(),
                          rangeFromExpr: Option[String] = None,
                          rangeToExpr: Option[String] = None,
                          warnMaxExecutionTimeSeconds: Option[Int] = None,
                          transformations: Seq[TransformExpression] = Nil,
                          filters: Seq[String] = Nil,
                          columns: Seq[String] = Nil,
                          overrideConf: Option[Config] = None): SourceTable = {
    SourceTable(metaTableName, query, conf, rangeFromExpr, rangeToExpr, warnMaxExecutionTimeSeconds, transformations, filters, columns, overrideConf)
  }
}
