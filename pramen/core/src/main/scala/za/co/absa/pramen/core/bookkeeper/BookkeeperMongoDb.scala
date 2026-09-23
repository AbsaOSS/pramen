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
import org.mongodb.scala.model.{Accumulators, Aggregates, Filters, Sorts}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.bookkeeper.model.{DataAvailability, DataAvailabilityAggregation}
import za.co.absa.pramen.core.dao.MongoDb
import za.co.absa.pramen.core.dao.model.{ASC, IndexField}
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.mongo.MongoDbConnection
import za.co.absa.pramen.core.utils.{AlgorithmUtils, TimeUtils}

import java.time.LocalDate
import scala.util.control.NonFatal

object BookkeeperMongoDb {
  val collectionName = "bookkeeping"
  val schemaCollectionName = "schemas"

  val MODEL_VERSION = 3
}

class BookkeeperMongoDb(mongoDbConnection: MongoDbConnection, batchId: Long) extends BookkeeperBase(true, batchId) {
  import BookkeeperMongoDb._
  import za.co.absa.pramen.core.dao.ScalaMongoImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val queryWarningTimeoutMs = 10000L
  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DataChunk], classOf[TableSchema], classOf[DataAvailabilityAggregation]), DEFAULT_CODEC_REGISTRY)
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

  override def getLatestDataChunkFromStorage(table: String, infoDate: LocalDate): Option[DataChunk] = {
    val infoDateFilter = getFilter(table, Option(infoDate), Option(infoDate), None)

    collection.find(infoDateFilter)
      .sort(Sorts.descending("jobFinished"))
      .limit(1)
      .execute()
      .headOption
  }

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    collection.countDocuments(getFilter(table, dateBeginOpt, dateEndOpt, None)).execute()
  }

  override def getDataChunksFromStorage(table: String, infoDate: LocalDate, batchId: Option[Long]): Seq[DataChunk] = {
    val chunks = collection.find(getFilter(table, Option(infoDate), Option(infoDate), batchId)).execute()
      .sortBy(_.jobFinished)
    log.debug(s"For $table ($infoDate) : ${chunks.mkString("[ ", ", ", " ]")}")
    chunks
  }

  override def getDataAvailabilityFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability] = {
    val filterByDates = getFilter(table, Option(dateBegin), Option(dateEnd), None)

    val pipeline = Seq(
      Aggregates.`match`(filterByDates),
      Aggregates.group(
        "$infoDate",
        Accumulators.first("infoDate", "$infoDate"),
        Accumulators.sum("chunks", 1),
        Accumulators.sum("totalRecords", "$outputRecordCount")
      ),
      Aggregates.sort(Sorts.ascending("infoDate"))
    )

    val tuples = collection.aggregate[DataAvailabilityAggregation](pipeline).execute()

    tuples.map(t => DataAvailability(LocalDate.parse(t.infoDate), t.chunks, t.totalRecords))
  }

  override def saveRecordCountToStorage(table: String,
                                        infoDate: LocalDate,
                                        inputRecordCount: Long,
                                        outputRecordCount: Long,
                                        recordsAppended: Option[Long],
                                        jobStarted: Long,
                                        jobFinished: Long): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)

    val record = DataChunk(table, dateStr, dateStr, dateStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished, Option(batchId), recordsAppended)

    collection.insertOne(record).execute()
  }

  override def deleteNonCurrentBatchRecords(table: String, infoDate: LocalDate): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)

    val filter = Filters.and(
      Filters.eq("tableName", table),
      Filters.eq("infoDate", dateStr),
      Filters.ne("batchId", batchId)
    )

    AlgorithmUtils.runActionWithElapsedTimeEvent(queryWarningTimeoutMs) {
      collection.deleteMany(filter).execute()
    }{ actualTimeMs =>
      val elapsedTime = TimeUtils.prettyPrintElapsedTimeShort(actualTimeMs)
      log.warn(s"MongoDB query took too long ($elapsedTime) while deleting from $collectionName, tableName='$table', infoDate='$infoDate', batchId!=$batchId")
    }
  }

  override def deleteTable(tableWithWildcard: String): Seq[String] = ???

  private def getFilter(tableName: String, infoDateBeginOpt: Option[LocalDate], infoDateEndOpt: Option[LocalDate], batchId: Option[Long]): Bson = {
    val baseFilter = (infoDateBeginOpt, infoDateEndOpt) match {
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

    batchId match {
      case Some(id) => Filters.and(baseFilter, Filters.eq("batchId", id))
      case None => baseFilter
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
        d.createIndex(collectionName, IndexField("tableName", ASC) :: IndexField("infoDate", ASC) :: Nil)
      }
      if (dbVersion < 2) {
        d.createCollection(schemaCollectionName)
        d.createIndex(schemaCollectionName, IndexField("tableName", ASC) :: IndexField("infoDate", ASC) :: Nil, unique = true)
      }
      if (dbVersion < 3 && dbVersion > 0) {
        val keys = IndexField("tableName", ASC) :: IndexField("infoDate", ASC) :: Nil
        // Make the bookkeeping index non-unique
        d.dropIndex(collectionName, keys)
        d.createIndex(collectionName, keys)
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
