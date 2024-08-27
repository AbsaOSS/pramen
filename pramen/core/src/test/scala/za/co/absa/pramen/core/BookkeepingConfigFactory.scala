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

package za.co.absa.pramen.core

import za.co.absa.pramen.core.app.config.{BookkeeperConfig, HadoopFormat}
import za.co.absa.pramen.core.reader.model.JdbcConfig

object BookkeepingConfigFactory {
  def getDummyBookkeepingConfig(bookkeepingEnabled: Boolean = false,
                                bookkeepingLocation: Option[String] = None,
                                bookkeepingHadoopFormat: HadoopFormat = HadoopFormat.Text,
                                bookkeepingConnectionString: Option[String] = None,
                                bookkeepingDbName: Option[String] = None,
                                bookkeepingJdbcConfig: Option[JdbcConfig] = None,
                                deltaDatabase: Option[String] = None,
                                deltaTablePrefix: Option[String] = None): BookkeeperConfig = {
    BookkeeperConfig(
      bookkeepingEnabled,
      bookkeepingLocation,
      bookkeepingHadoopFormat,
      bookkeepingConnectionString,
      bookkeepingDbName,
      bookkeepingJdbcConfig,
      deltaDatabase,
      deltaTablePrefix
    )
  }

}
