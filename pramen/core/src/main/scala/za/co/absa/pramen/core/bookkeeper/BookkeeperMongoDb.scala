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

package za.co.absa.pramen.core.bookkeeper

import com.mongodb.client.model.ReplaceOptions
import org.apache.spark.sql.types.StructType
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.{Filters, Sorts}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.dao.MongoDb
import za.co.absa.pramen.core.dao.model.{ASC, IndexField}
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.mongo.MongoDbConnection

import java.time.LocalDate
import scala.util.control.NonFatal

object BookkeeperMongoDb {
  val collectionName = "bookkeeping"
  val schemaCollectionName = "schemas"

  val MODEL_VERSION = 2
}

class BookkeeperMongoDb(mongoDbConnection: MongoDbConnection) extends BookkeeperBase(true) {

  import BookkeeperMongoDb._
  import za.co.absa.pramen.core.dao.ScalaMongoImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DataChunk], classOf[TableSchema]), DEFAULT_CODEC_REGISTRY)
  private val db = mongoDbConnection.getDatabase

  initCollection()

  private val collection = db.getCollection[DataChunk](collectionName).withCodecRegistry(codecRegistry)
  private val schemaCollection = db.getCollection[TableSchema](schemaCollectionName).withCodecRegistry(codecRegistry)

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate]): Option[LocalDate] = {
    val filter = until match {
      case Some(endDate) =>
        val endDateStr = DataChunk.dateFormatter.format(endDate)
        Filters.and(
          Filters.eq("tableName", table),
          Filters.lte("infoDate", endDateStr))
      case None =>
        Filters.eq("tableName", table)
    }

    val chunks = collection.find(filter).execute()

    if (chunks.isEmpty) {
      None
    } else {
      val chunk = chunks.maxBy(_.infoDateEnd)
      Option(LocalDate.parse(chunk.infoDateEnd, DataChunk.dateFormatter))
    }
  }

  override def getLatestDataChunkFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    getDataChunksFromStorage(table, dateBegin, dateEnd).lastOption
  }

  override def getDataChunksFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = {
    val chunks = collection.find(getFilter(table, Option(dateBegin), Option(dateEnd))).execute()
      .sortBy(_.jobFinished)
    log.info(s"For $table $dateBegin - $dateEnd : ${chunks.mkString("[ ", ", ", " ]")}")
    chunks
  }

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    collection.countDocuments(getFilter(table, dateBeginOpt, dateEndOpt)).execute()
  }

  private[pramen] override def saveRecordCountToStorage(table: String,
                                                        infoDate: LocalDate,
                                                        infoDateBegin: LocalDate,
                                                        infoDateEnd: LocalDate,
                                                        inputRecordCount: Long,
                                                        outputRecordCount: Long,
                                                        jobStarted: Long,
                                                        jobFinished: Long): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)
    val dateBeginStr = DataChunk.dateFormatter.format(infoDateBegin)
    val dateEndStr = DataChunk.dateFormatter.format(infoDateEnd)

    val chunk = DataChunk(table, dateStr, dateBeginStr, dateEndStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished)

    val opts = (new ReplaceOptions).upsert(true)
    collection.replaceOne(getFilter(table, Option(infoDate), Option(infoDate)), chunk, opts).execute()
  }

  private def getFilter(tableName: String, infoDateBeginOpt: Option[LocalDate], infoDateEndOpt: Option[LocalDate]): Bson = {
    (infoDateBeginOpt, infoDateEndOpt) match {
      case (Some(infoDateBegin), Some(infoDateEnd)) =>

        val date0Str = DataChunk.dateFormatter.format(infoDateBegin)
        val date1Str = DataChunk.dateFormatter.format(infoDateEnd)

        if (date0Str == date1Str) {
          Filters.and(
            Filters.eq("tableName", tableName),
            Filters.eq("infoDate", date0Str))
        } else {
          Filters.and(
            Filters.eq("tableName", tableName),
            Filters.and(
              Filters.gte("infoDate", date0Str),
              Filters.lte("infoDate", date1Str))
          )
        }
      case (Some(infoDateBegin), None) =>
        val date0Str = DataChunk.dateFormatter.format(infoDateBegin)
        Filters.and(
          Filters.eq("tableName", tableName),
          Filters.gte("infoDate", date0Str))
      case (None, Some(infoDateEnd)) =>
        val date1Str = DataChunk.dateFormatter.format(infoDateEnd)
        Filters.and(
          Filters.eq("tableName", tableName),
          Filters.lte("infoDate", date1Str))
      case (None, None) =>
        Filters.eq("tableName", tableName)
    }

  }

  private def getSchemaGetFilter(tableName: String, until: LocalDate): Bson = {
    Filters.and(
      Filters.eq("tableName", tableName),
      Filters.lte("infoDate", until.toString))
  }

  private def getSchemaPutFilter(tableName: String, infoDate: LocalDate): Bson = {
    Filters.and(
      Filters.eq("tableName", tableName),
      Filters.eq("infoDate", infoDate.toString))
  }

  private def initCollection(): Unit = {
    try {
      val d = new MongoDb(db)
      val dbVersion = d.getVersion()
      if (!d.doesCollectionExists(collectionName)) {
        d.createCollection(collectionName)
        d.createIndex(collectionName, IndexField("tableName", ASC) :: IndexField("infoDate", ASC) :: Nil, unique = true)
      }
      if (dbVersion < 2) {
        d.createCollection(schemaCollectionName)
        d.createIndex(schemaCollectionName, IndexField("tableName", ASC) :: IndexField("infoDate", ASC) :: Nil, unique = true)
      }
      if (dbVersion < MODEL_VERSION) {
        d.setVersion(MODEL_VERSION)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to connect to MongoDb instance: ${mongoDbConnection.getConnectionString}", ex)
    }
  }

  override def getLatestSchema(tableName: String, until: LocalDate): Option[(StructType, LocalDate)] = {
    schemaCollection.find(getSchemaGetFilter(tableName, until))
      .sort(Sorts.descending("infoDate"))
      .limit(1)
      .execute()
      .flatMap(tableSchema =>
        TableSchema.toSchemaAndDate(tableSchema)
      )
      .headOption
  }

  private[pramen] override def saveSchema(tableName: String, infoDate: LocalDate, schema: StructType): Unit = {
    val opts = (new ReplaceOptions).upsert(true)
    schemaCollection.replaceOne(getSchemaPutFilter(tableName, infoDate), TableSchema(
      tableName,
      infoDate.toString,
      schema.json
    ), opts).execute()
  }
}
