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

package za.co.absa.pramen.core.tests.bookkeeper

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, BookkeeperJdbc}
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.model.JdbcConfig

class BookkeeperJdbcSuite extends BookkeeperCommonSuite with RelationalDbFixture with BeforeAndAfter with BeforeAndAfterAll {

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  def getBookkeeper: Bookkeeper = {
    new BookkeeperJdbc(pramenDb.slickDb)
  }

  "BookkeeperJdbc" when {
    "initialized" should {
      "Initialize an empty database" in {
        getBookkeeper

        assert(getTables.exists(_.equalsIgnoreCase("bookkeeping")))
        assert(getTables.exists(_.equalsIgnoreCase("schemas")))
        assert(getTables.exists(_.equalsIgnoreCase("metadata")))
      }
    }

    testBookKeeper(() => getBookkeeper)
  }
}
