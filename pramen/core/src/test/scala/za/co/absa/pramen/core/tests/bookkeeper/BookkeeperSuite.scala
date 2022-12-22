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
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.RuntimeConfigFactory
import za.co.absa.pramen.core.app.config.{BookkeeperConfig, HadoopFormat, RuntimeConfig}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, BookkeeperJdbc, BookkeeperMongoDb, BookkeeperText}
import za.co.absa.pramen.core.fixtures.{MongoDbFixture, RelationalDbFixture, TempDirFixture}
import za.co.absa.pramen.core.journal.{JournalHadoop, JournalJdbc, JournalMongoDb}
import za.co.absa.pramen.core.lock.{TokenLockFactoryHadoop, TokenLockFactoryJdbc, TokenLockFactoryMongoDb}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.reader.model.JdbcConfig

class BookkeeperSuite extends AnyWordSpec
  with MongoDbFixture
  with RelationalDbFixture
  with TempDirFixture
  with SparkTestBase
  with BeforeAndAfter
  with BeforeAndAfterAll {

  import za.co.absa.pramen.core.bookkeeper.BookkeeperMongoDb._

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, user, password, Map.empty[String, String])
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()

    if (db.doesCollectionExists(collectionName)) {
      db.dropCollection(collectionName)
    }
    if (db.doesCollectionExists(schemaCollectionName)) {
      db.dropCollection(schemaCollectionName)
    }
  }


  val runtimeConfig: RuntimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(
    useLocks = true
  )

  "config factory" should {
    "build bookkeeper, token lock, journal, and closable object for JDBC" in {
      val bookkeepingConfig = BookkeeperConfig(
        bookkeepingEnabled = true,
        None,
        HadoopFormat.Text,
        None,
        None,
        Some(jdbcConfig)
      )

      val (bk, tf, journal, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

      assert(bk.isInstanceOf[BookkeeperJdbc])
      assert(tf.isInstanceOf[TokenLockFactoryJdbc])
      assert(journal.isInstanceOf[JournalJdbc])
      closable.close()
    }

    "build bookkeeper, token lock, journal, and closable object for MongoDB" in {
      val bookkeepingConfig = BookkeeperConfig(
        bookkeepingEnabled = true,
        None,
        HadoopFormat.Text,
        Some(connectionString),
        Some(dbName),
        None
      )

      val (bk, tf, journal, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

      assert(bk.isInstanceOf[BookkeeperMongoDb])
      assert(tf.isInstanceOf[TokenLockFactoryMongoDb])
      assert(journal.isInstanceOf[JournalMongoDb])
      closable.close()
    }

    "build bookkeeper, token lock, journal, and closable object for Hadoop" in {
      withTempDirectory("bk_hadoop") { tempDir =>
        val bookkeepingConfig = BookkeeperConfig(
          bookkeepingEnabled = true,
          Some(tempDir),
          HadoopFormat.Text,
          None,
          None,
          None
        )

        val (bk, tf, journal, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

        assert(bk.isInstanceOf[BookkeeperText])
        assert(tf.isInstanceOf[TokenLockFactoryHadoop])
        assert(journal.isInstanceOf[JournalHadoop])
        closable.close()
      }
    }
  }


}
