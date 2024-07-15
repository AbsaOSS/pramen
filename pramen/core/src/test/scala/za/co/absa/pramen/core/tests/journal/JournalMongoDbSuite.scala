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

package za.co.absa.pramen.core.tests.journal

import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.MongoDbFixture
import za.co.absa.pramen.core.journal.{Journal, JournalMongoDb}

class JournalMongoDbSuite extends AnyWordSpec with MongoDbFixture with BeforeAndAfter {
  import TestCases._
  import za.co.absa.pramen.core.dao.ScalaMongoImplicits._
  import za.co.absa.pramen.core.journal.JournalMongoDb._

  var journal: Journal = _

  before {
    if (db != null) {
      if (db.doesCollectionExists(collectionName)) {
        db.dropCollection(collectionName)
      }
      journal = new JournalMongoDb(connection)
    }
  }

  if (db != null) {
    "Journal" should {
      "Initialize an empty database" in {
        db.doesCollectionExists("collectionName")

        assert(db.doesCollectionExists(collectionName))

        val indexes = dbRaw.getCollection(collectionName).listIndexes().execute()
        assert(indexes.size == 3)
      }

      "addEntry()" should {
        "return Nil if there are no entries" in {
          assert(journal.getEntries(instant1, instant3).isEmpty)
        }

        "return entries if there are entries" in {
          journal.addEntry(task1)
          journal.addEntry(task2)
          journal.addEntry(task3)


          val entries = journal.getEntries(instant2, instant3).sortBy(_.informationDate.toString)

          assert(entries.nonEmpty)
          assert(entries == task2 :: task3 :: Nil)
        }
      }
    }
  } else {
    "Journal" ignore {
      // Ignored on an incompatible platform
    }
  }

}
