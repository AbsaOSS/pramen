/*
 * Copyright 2022 ABSA Group Limited
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

import org.scalatest.BeforeAndAfter
import za.co.absa.pramen.framework.bookkeeper.{SyncBookKeeper, SyncBookKeeperMongoDb}
import za.co.absa.pramen.framework.fixtures.MongoDbFixture

class SyncBookKeeperMongoDbLongSuite extends SyncBookKeeperCommonSuite with MongoDbFixture with BeforeAndAfter {

  import SyncBookKeeperMongoDb._
  import za.co.absa.pramen.framework.dao.ScalaMongoImplicits._

  before {
    if (db.doesCollectionExists(collectionName)) {
      db.dropCollection(collectionName)
    }
    if (db.doesCollectionExists(schemaCollectionName)) {
      db.dropCollection(schemaCollectionName)
    }
  }

  def getBookkeeper: SyncBookKeeper = {
    new SyncBookKeeperMongoDb(connection)
  }

  "BookkeeperMongoDb" when {
    "initialized" should {
      "Initialize an empty database" in {
        getBookkeeper

        assert(db.doesCollectionExists(collectionName))

        val indexes = dbRaw.getCollection(collectionName).listIndexes().execute()
        assert(indexes.size == 2)
      }
    }

    testBookKeeper(() => getBookkeeper)
  }
}
