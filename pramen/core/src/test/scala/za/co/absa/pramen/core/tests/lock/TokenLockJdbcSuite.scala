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

package za.co.absa.pramen.core.tests.lock

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.fixtures.RelationalDbFixture
import za.co.absa.pramen.core.lock.{TokenLock, TokenLockJdbc}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.model.JdbcConfig

class TokenLockJdbcSuite extends AnyWordSpec with RelationalDbFixture with BeforeAndAfter with BeforeAndAfterAll {
  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Option(user), Option(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()
  }

  override def afterAll(): Unit = {
    pramenDb.close()
    super.afterAll()
  }

  "Token lock" should {
    "be able to acquire and release locks" in {
      val lock1 = getLock("token1")

      assert(lock1.tryAcquire())
      assert(!lock1.tryAcquire())

      val lock2 = getLock("token1")
      assert(!lock2.tryAcquire())

      lock1.release()

      assert(lock2.tryAcquire())
      assert(!lock2.tryAcquire())

      lock2.release()
    }

    "multiple token locks should not affect each other" in {
      val lock1 = getLock("token1")
      val lock2 = getLock("token2")

      assert(lock1.tryAcquire())
      assert(lock2.tryAcquire())

      assert(!lock1.tryAcquire())
      assert(!lock2.tryAcquire())

      lock1.release()

      assert(lock1.tryAcquire())
      assert(!lock2.tryAcquire())

      lock1.release()
      lock2.release()
    }

    "lock pramen should constantly update lock ticket" in {
      val lock1 = new TokenLockJdbc("token1", pramenDb.slickDb) {
        override val TOKEN_EXPIRES_SECONDS = 2L
      }
      val lock2 = new TokenLockJdbc("token1", pramenDb.slickDb)
      assert(lock1.tryAcquire())
      // Give it enough time to update
      Thread.sleep(3000)
      assert(!lock2.tryAcquire())
      assert(!lock1.tryAcquire())
      lock1.release()
    }
  }

  private def getLock(token: String): TokenLock = {
    new TokenLockJdbc(token, pramenDb.slickDb)
  }

}
