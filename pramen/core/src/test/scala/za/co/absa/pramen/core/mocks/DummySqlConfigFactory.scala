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

import com.typesafe.config.ConfigFactory
import za.co.absa.pramen.api.sql.{QuotingPolicy, SqlColumnType, SqlConfig}

object DummySqlConfigFactory {
  def getDummyConfig(infoDateColumn: String = "col",
                     infoDateType: SqlColumnType = SqlColumnType.DATE,
                     dateFormatApp: String = "yyyy-MM-dd",
                     identifierQuotingPolicy: QuotingPolicy = QuotingPolicy.Auto,
                     sqlGeneratorClass: Option[String] = None
                    ): SqlConfig = SqlConfig(
    infoDateColumn = infoDateColumn,
    infoDateType = infoDateType,
    dateFormatApp = dateFormatApp,
    identifierQuotingPolicy = identifierQuotingPolicy,
    sqlGeneratorClass = sqlGeneratorClass,
    ConfigFactory.empty())
}
