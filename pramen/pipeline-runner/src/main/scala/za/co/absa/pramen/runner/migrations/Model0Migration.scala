package za.co.absa.pramen.runner.migrations

import com.typesafe.config.{Config, ConfigFactory}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperMongoDb
import za.co.absa.pramen.framework.config.WatcherConfig
import za.co.absa.pramen.framework.dao.MongoDb
import za.co.absa.pramen.framework.model.DataChunk
import za.co.absa.pramen.runner.migrations.model0.DataChunk0

import java.io.File

/**
  * This is a migration from JSON bookkeeping store to MongoDB.
  */
object Model0Migration {
  import za.co.absa.pramen.framework.dao.ScalaMongoImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    implicit val conf: Config = getConfig(args)

    val codecRegistry: CodecRegistry = fromRegistries(
      fromProviders(classOf[DataChunk0], classOf[DataChunk]), DEFAULT_CODEC_REGISTRY)

    val syncConfig: WatcherConfig = WatcherConfig.load(conf)

    val mongoClient = MongoClient(syncConfig.bookkeepingConfig.bookkeepingConnectionString.get)
    val database = mongoClient.getDatabase(syncConfig.bookkeepingConfig.bookkeepingDbName.get)

    val db = new MongoDb(database)

    db.cloneCollection(SyncBookKeeperMongoDb.collectionName, "migrated")
    db.emptyCollection("migrated")

    val c1 = database.getCollection[DataChunk0](SyncBookKeeperMongoDb.collectionName).withCodecRegistry(codecRegistry)
    val c2 = database.getCollection[DataChunk]("migrated").withCodecRegistry(codecRegistry)

    c1.find()
      .syncForeach(chunk0 => {
        val chunk2 = DataChunk(chunk0.tableName, chunk0.infoDate, chunk0.infoDate, chunk0.infoDate,
          chunk0.inputRecordCount, chunk0.outputRecordCount, chunk0.jobStarted, chunk0.jobFinished)
        c2.insertOne(chunk2).execute()
      })

    mongoClient.close()
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
