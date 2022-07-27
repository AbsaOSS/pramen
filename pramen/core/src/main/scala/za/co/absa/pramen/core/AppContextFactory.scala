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

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.core.app.{AppContext, AppContextImpl}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.journal.Journal

object AppContextFactory {
  private var appContext: AppContext = _

  def get: AppContext = this.synchronized {
    appContext
  }

  def getOrCreate(conf: Config)(implicit spark: SparkSession): AppContext = this.synchronized {
    if (appContext == null) {
      appContext = AppContextImpl.apply(conf)
      appContext
    } else {
      appContext
    }
  }

  private [pramen] def createMockAppContext(conf: Config,
                                            bookkeeper: Bookkeeper,
                                            journal: Journal
                                              )(implicit spark: SparkSession): AppContext = this.synchronized {
    appContext = AppContextImpl.getMock(conf, bookkeeper, journal)
    appContext
  }

  def close(): Unit = this.synchronized {
    if (appContext != null) {
      appContext.close()
      appContext = null
    }
  }
}