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

package za.co.absa.pramen.runner.migrations

import com.typesafe.config.{Config, ConfigFactory}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Sorts
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperJdbc
import za.co.absa.pramen.framework.config.WatcherConfig
import za.co.absa.pramen.framework.dao.ScalaMongoImplicits.FindObservableTraversable
import za.co.absa.pramen.framework.journal.JournalJdbc
import za.co.absa.pramen.framework.model.{DataChunk, TableSchema}
import za.co.absa.pramen.framework.mongo.MongoDbConnection
import za.co.absa.pramen.framework.notify.TaskCompleted
import za.co.absa.pramen.framework.rdb.SyncWatcherDb

import java.io.File
import java.time.LocalDate


/**
  * Command line:
  *
  * java -cp pipeline-runner.jar:scala-library-2.11.12.jar:spark-libs-2.4.8-2.11.12.jar za.co.absa.pramen.runner.migrations.MongoDBtoJDBCMigration config.conf
  */
object MongoDBtoJDBCMigration {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    implicit val conf: Config = getConfig(args)

    val syncConfig: WatcherConfig = WatcherConfig.load(conf)

    if (syncConfig.bookkeepingConfig.bookkeepingConnectionString.isEmpty || syncConfig.bookkeepingConfig.bookkeepingDbName.isEmpty || syncConfig.bookkeepingConfig.bookkeepingJdbcConfig.isEmpty) {
      throw new RuntimeException("You must define both MongoDB connection and JDBC bookkeeping connection in order to run MongoDb to JDBC migration")
    }

    val jdbcConfig = syncConfig.bookkeepingConfig.bookkeepingJdbcConfig.get
    val syncDb = SyncWatcherDb(jdbcConfig)

    val mongoDbConnection: MongoDbConnection = MongoDbConnection.getConnection(syncConfig.bookkeepingConfig.bookkeepingConnectionString.get, syncConfig.bookkeepingConfig.bookkeepingDbName.get)

    if (syncDb.rdb.getVersion() != 0) {
      throw new RuntimeException("Output database already exists. Migration is supported only to an empty database")
    }

    syncDb.setupDatabase()

    migrateBookkeeping(syncDb, mongoDbConnection)
    migrateJournal(syncDb, mongoDbConnection)
  }

  private def migrateBookkeeping(syncDb: SyncWatcherDb, mongoConnection: MongoDbConnection): Unit = {
    import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperMongoDb.{collectionName, schemaCollectionName}

    val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DataChunk], classOf[TableSchema]), DEFAULT_CODEC_REGISTRY)
    val collection = mongoConnection
      .getDatabase
      .getCollection[DataChunk](collectionName)
      .withCodecRegistry(codecRegistry)

    val bookkeeper = new SyncBookKeeperJdbc(syncDb.slickDb)

    log.info(s"Migrating schemas...")
    val schemaCollection = mongoConnection
      .getDatabase
      .getCollection[TableSchema](schemaCollectionName)
      .withCodecRegistry(codecRegistry)

    schemaCollection.find()
      .sort(Sorts.ascending("infoDate"))
      .syncForeach(t => {
        bookkeeper.saveSchemaRaw(t.tableName, t.infoDate, t.schemaJson)
      })
    log.info(s"Schemas migrated successfully!")

    log.info(s"Migrating bookkeeping...")
    collection.find()
      .sort(Sorts.ascending("jobFinished"))
      .syncForeach(d => {
        val infoDate = LocalDate.parse(d.infoDate, DataChunk.dateFormatter)
        val infoDateBegin = LocalDate.parse(d.infoDateBegin, DataChunk.dateFormatter)
        val infoDateEnd = LocalDate.parse(d.infoDateEnd, DataChunk.dateFormatter)
        bookkeeper.setRecordCount(
          d.tableName,
          infoDate,
          infoDateBegin,
          infoDateEnd,
          d.inputRecordCount,
          d.outputRecordCount,
          d.jobStarted,
          d.jobFinished)
      })
    log.info(s"Bookkeeping data migrated successfully!")
  }

  private def migrateJournal(syncDb: SyncWatcherDb, mongoConnection: MongoDbConnection): Unit = {
    import za.co.absa.pramen.framework.journal.JournalMongoDb.collectionName

    val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[TaskCompleted]), DEFAULT_CODEC_REGISTRY)

    val collection = mongoConnection
      .getDatabase
      .getCollection[TaskCompleted](collectionName)
      .withCodecRegistry(codecRegistry)

    val journal = new JournalJdbc(syncDb.slickDb)

    log.info(s"Migrating journal...")

    collection.find()
      .sort(Sorts.ascending("jobFinished"))
      .syncForeach(t => journal.addEntry(t))
    log.info(s"Bookkeeping data migrated successfully!")
  }

  private def getConfig(args: Array[String]): Config = {
    val originalConfig = ConfigFactory.load()

    if (args.length < 1) {
      log.warn("No Pramen pipeline configuration is provided. Using the default application.conf\n")
      originalConfig
    } else {
      val confFile = args.head
      log.info(s"Loading $confFile...\n")
      ConfigFactory.parseFile(new File(confFile))
        .withFallback(originalConfig)
        .resolve()
    }
  }
}
