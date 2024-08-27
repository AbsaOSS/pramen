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

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import za.co.absa.pramen.core.{BookkeepingConfigFactory, RuntimeConfigFactory}
import za.co.absa.pramen.core.app.config.{BookkeeperConfig, HadoopFormat, RuntimeConfig}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, BookkeeperDeltaPath, BookkeeperJdbc, BookkeeperMongoDb, BookkeeperText}
import za.co.absa.pramen.core.fixtures.{MongoDbFixture, RelationalDbFixture, TempDirFixture}
import za.co.absa.pramen.core.journal.{JournalHadoopCsv, JournalHadoopDeltaPath, JournalJdbc, JournalMongoDb}
import za.co.absa.pramen.core.lock.{TokenLockFactoryHadoopPath, TokenLockFactoryJdbc, TokenLockFactoryMongoDb}
import za.co.absa.pramen.core.metadata.{MetadataManagerJdbc, MetadataManagerNull}
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

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Option(user), Option(password))
  val pramenDb: PramenDb = PramenDb(jdbcConfig)

  before {
    pramenDb.rdb.executeDDL("DROP SCHEMA PUBLIC CASCADE;")
    pramenDb.setupDatabase()

    if (db != null) {
      if (db.doesCollectionExists(collectionName)) {
        db.dropCollection(collectionName)
      }
      if (db.doesCollectionExists(schemaCollectionName)) {
        db.dropCollection(schemaCollectionName)
      }
    }
  }


  val runtimeConfig: RuntimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(
    useLocks = true
  )

  "config factory" should {
    "build bookkeeper, token lock, journal, and closable object for JDBC" in {
      val bookkeepingConfig = BookkeepingConfigFactory.getDummyBookkeepingConfig(
        bookkeepingEnabled = true,
        bookkeepingJdbcConfig = Some(jdbcConfig)
      )

      val (bk, tf, journal, metadataManager, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

      assert(bk.isInstanceOf[BookkeeperJdbc])
      assert(tf.isInstanceOf[TokenLockFactoryJdbc])
      assert(journal.isInstanceOf[JournalJdbc])
      assert(metadataManager.isInstanceOf[MetadataManagerJdbc])
      closable.close()
    }

    if (db != null) {
      "build bookkeeper, token lock, journal, and closable object for MongoDB" in {
        val bookkeepingConfig = BookkeepingConfigFactory.getDummyBookkeepingConfig(
          bookkeepingEnabled = true,
          bookkeepingConnectionString = Some(connectionString),
          bookkeepingDbName = Some(dbName)
        )

        val (bk, tf, journal, metadataManager, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

        assert(bk.isInstanceOf[BookkeeperMongoDb])
        assert(tf.isInstanceOf[TokenLockFactoryMongoDb])
        assert(journal.isInstanceOf[JournalMongoDb])
        assert(metadataManager.isInstanceOf[MetadataManagerNull])
        closable.close()
      }
    } else {
      "build bookkeeper, token lock, journal, and closable object for MongoDB" ignore {
        // Skip on incompatible platform
      }
    }

    "build bookkeeper, token lock, journal, and closable object for Hadoop CSV" in {
      withTempDirectory("bk_hadoop") { tempDir =>
        val bookkeepingConfig = BookkeepingConfigFactory.getDummyBookkeepingConfig(
          bookkeepingEnabled = true,
          bookkeepingLocation = Some(tempDir)
        )

        val (bk, tf, journal, metadataManager, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

        assert(bk.isInstanceOf[BookkeeperText])
        assert(tf.isInstanceOf[TokenLockFactoryHadoopPath])
        assert(journal.isInstanceOf[JournalHadoopCsv])
        assert(metadataManager.isInstanceOf[MetadataManagerNull])
        closable.close()
      }
    }

    "build bookkeeper, token lock, journal, and closable object for Hadoop Delta Path" in {
      withTempDirectory("bk_hadoop_delta_path") { tempDir =>
        val bookkeepingConfig = BookkeepingConfigFactory.getDummyBookkeepingConfig(
          bookkeepingEnabled = true,
          bookkeepingLocation = Some(tempDir),
          bookkeepingHadoopFormat = HadoopFormat.Delta
        )

        val (bk, tf, journal, metadataManager, closable) = Bookkeeper.fromConfig(bookkeepingConfig, runtimeConfig)

        assert(bk.isInstanceOf[BookkeeperDeltaPath])
        assert(tf.isInstanceOf[TokenLockFactoryHadoopPath])
        assert(journal.isInstanceOf[JournalHadoopDeltaPath])
        assert(metadataManager.isInstanceOf[MetadataManagerNull])
        closable.close()
      }
    }
  }


}
