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

package za.co.absa.pramen.core.journal

import java.time.Instant

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Filters
import za.co.absa.pramen.core.dao.MongoDb
import za.co.absa.pramen.core.dao.model.{ASC, IndexField}
import za.co.absa.pramen.core.journal.model.TaskCompleted
import za.co.absa.pramen.core.mongo.MongoDbConnection

import scala.util.control.NonFatal

object JournalMongoDb {
  val collectionName = "journal"
}

class JournalMongoDb(mongoDbConnection: MongoDbConnection) extends Journal {

  import JournalMongoDb._
  import za.co.absa.pramen.core.dao.ScalaMongoImplicits._

  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[TaskCompleted]), DEFAULT_CODEC_REGISTRY)
  private val db = mongoDbConnection.getDatabase

  initCollection()

  private val collection = db.getCollection[TaskCompleted](collectionName).withCodecRegistry(codecRegistry)

  override def addEntry(entry: TaskCompleted): Unit = {
    collection.insertOne(entry).execute()
  }

  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {

    val instant0 = if (to.isBefore(from)) to else from
    val instant1 = if (to.isBefore(from)) from else to

    val filter = Filters.and(
      Filters.gte("finishedAt", instant0.getEpochSecond),
      Filters.lte("finishedAt", instant1.getEpochSecond)
    )
    collection.find(filter).execute()
  }

  private def initCollection(): Unit = {
    try {
      val d = new MongoDb(db)
      if (!d.doesCollectionExists(collectionName)) {
        d.createCollection(collectionName)
        d.createIndex(collectionName, IndexField("startedAt", ASC) :: Nil)
        d.createIndex(collectionName, IndexField("finishedAt", ASC) :: Nil)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to connect to MongoDb instance: ${mongoDbConnection.getConnectionString}", ex)
    }
  }
}
