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

import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.MongoDbFixture
import za.co.absa.pramen.core.lock.TokenLockMongoDb
import za.co.absa.pramen.core.lock.TokenLockMongoDb.collectionName

class TokenLockMongoDbSuite extends AnyWordSpec with MongoDbFixture with BeforeAndAfter {

  before {
    if (db != null) {
      if (db.doesCollectionExists(collectionName)) {
        db.dropCollection(collectionName)
      }
    }
  }


  if (db != null) {
    "Token Lock" should {
      "be able to acquire and release locks" in {
        val lock1 = new TokenLockMongoDb("token1", connection)

        assert(lock1.tryAcquire())
        assert(!lock1.tryAcquire())

        val lock2 = new TokenLockMongoDb("token1", connection)
        assert(!lock2.tryAcquire())

        lock1.release()

        assert(lock2.tryAcquire())
        assert(!lock2.tryAcquire())

        lock2.release()
      }

      "multiple token locks should not affect each other" in {
        val lock1 = new TokenLockMongoDb("token1", connection)
        val lock2 = new TokenLockMongoDb("token2", connection)

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
        val lock1 = new TokenLockMongoDb("token1", connection) {
          override val TOKEN_EXPIRES_SECONDS = 3L
        }
        val lock2 = new TokenLockMongoDb("token1", connection)
        assert(lock1.tryAcquire())
        Thread.sleep(4000)
        assert(!lock2.tryAcquire())
        assert(!lock1.tryAcquire())
        lock1.release()
      }
    }
  } else {
    "Token Lock" ignore {
      // Skip incompatible platform
    }
  }

}
