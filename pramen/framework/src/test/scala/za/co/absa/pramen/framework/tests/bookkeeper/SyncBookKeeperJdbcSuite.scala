/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.tests.bookkeeper

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.framework.bookkeeper.{SyncBookKeeper, SyncBookKeeperJdbc}
import za.co.absa.pramen.framework.fixtures.RelationalDbFixture
import za.co.absa.pramen.framework.rdb.SyncWatcherDb
import za.co.absa.pramen.framework.reader.model.JdbcConfig

class SyncBookKeeperJdbcSuite extends SyncBookKeeperCommonSuite with RelationalDbFixture with BeforeAndAfter with BeforeAndAfterAll {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, user, password, Map.empty[String, String])
  val syncWatcherDb: SyncWatcherDb = SyncWatcherDb(jdbcConfig)

  before {
    syncWatcherDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    syncWatcherDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    syncWatcherDb.close()
    super.afterAll()
  }

  def getBookkeeper: SyncBookKeeper = {
    new SyncBookKeeperJdbc(syncWatcherDb.slickDb)
  }

  "BookkeeperJdbc" when {
    "initialized" should {
      "Initialize an empty database" in {
        getBookkeeper

        assert(getTables.exists(_.equalsIgnoreCase("bookkeeping")))
        assert(getTables.exists(_.equalsIgnoreCase("schemas")))
      }
    }

    testBookKeeper(() => getBookkeeper)
  }
}
