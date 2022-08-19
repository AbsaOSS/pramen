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

package za.co.absa.pramen.core.dao

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.IndexOptions
import org.mongodb.scala.{MongoCollection, MongoDatabase, MongoNamespace}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.pramen.core.dao.model.{DbVersion, IndexField}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * This is a MongoDB implementation of the document DB API needed for migration framework to run.
  *
  * The implementation depends on MongoDB Scala driver. An instance of Scala Mongo Database is expected to be
  * passed as a constructor parameter.
  */
class MongoDb(db: MongoDatabase) extends DocumentDb {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val DatabaseVersionCollectionName = "db_version"

  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DbVersion]), DEFAULT_CODEC_REGISTRY)

  // This adds .execute() method to observables
  import ScalaMongoImplicits._

  /**
    * Returns the version of the database. Version of a database determines the schema used for writes.
    *
    * For MongoDB databases the version number is kept in 'db_version' collection.
    * If there is no such a collection the version of the database is assumed to be 0.
    */
  override def getVersion(): Int = {
    if (!doesCollectionExists(DatabaseVersionCollectionName)) {
      setVersion(0)
    }

    val versions = getTypedCollection(DatabaseVersionCollectionName)
      .find[DbVersion]()
      .execute()

    if (versions.isEmpty) {
      getTypedCollection[DbVersion](DatabaseVersionCollectionName)
        .insertOne(DbVersion(0))
        .execute()
      0
    } else if (versions.lengthCompare(1) != 0) {
      val len = versions.length
      throw new IllegalStateException(
        s"Unexpected number of documents in '$DatabaseVersionCollectionName'. Expected 1, got $len")
    } else {
      versions.head.version
    }
  }

  /**
    * Sets the version of the database. When a version is set it implies a migration to that version is completed
    * successfully and the database can be used with the specified version of the schema.
    *
    * For MongoDB databases the version number is kept in 'db_version' collection.
    * If there is no such a collection the version of the database is assumed to be 0.
    */
  override def setVersion(version: Int): Unit = {
    if (!doesCollectionExists(DatabaseVersionCollectionName)) {
      createCollection(DatabaseVersionCollectionName)
      getTypedCollection[DbVersion](DatabaseVersionCollectionName)
        .insertOne(DbVersion(version))
        .execute()
    } else {
      getTypedCollection[DbVersion](DatabaseVersionCollectionName)
        .replaceOne(new BsonDocument, DbVersion(version))
        .execute()
    }
  }

  /**
    * Returns true if the specified collection exists in the database.
    */
  override def doesCollectionExists(collectionName: String): Boolean = {
    val collections = db.listCollectionNames().execute()
    collections.contains(collectionName)
  }

  /**
    * Creates a collection with the given name.
    */
  override def createCollection(collectionName: String): Unit = {
    log.info(s"Creating $collectionName collection...")
    db.createCollection(collectionName).execute()
  }

  /**
    * Drop a collection.
    */
  override def dropCollection(collectionName: String): Unit = {
    log.info(s"Dropping $collectionName collection...")
    getCollection(collectionName)
      .drop()
      .execute()
  }

  /**
    * Removes all documents from a collection.
    */
  override def emptyCollection(collectionName: String): Unit = {
    log.info(s"Emptying $collectionName collection...")
    getCollection(collectionName)
      .deleteMany(new BsonDocument())
      .execute()
  }

  /**
    * Renames a collection
    */
  override def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit = {
    log.info(s"Renaming $collectionNameOld to $collectionNameNew...")
    getCollection(collectionNameOld)
      .renameCollection(MongoNamespace(db.name, collectionNameNew))
      .execute()
  }

  /**
    * Copies contents of a collection to a collection with a different name in the same database.
    *
    * Copies indexed as well.
    */
  override def cloneCollection(collectionFrom: String, collectionTo: String): Unit = {
    log.info(s"Copying $collectionFrom to $collectionFrom...")
    ensureCollectionExists(collectionFrom)
    val cmd =
      s"""{
         |  aggregate: "$collectionFrom",
         |  pipeline: [
         |    {
         |      $$match: {}
         |    },
         |    {
         |      $$out: "$collectionTo"
         |    }
         |  ],
         |  cursor: {}
         |}
         |""".stripMargin
    executeCommand(cmd)
    copyIndexes(collectionFrom, collectionTo)
  }

  /**
    * Creates an index for a given list of fields.
    */
  override def createIndex(collectionName: String, keys: Seq[IndexField], unique: Boolean = false, sparse: Boolean = false): Unit = {
    log.info(s"Creating an index for $collectionName, unique: $unique, keys: ${keys.mkString(", ")}...")
    val collection = getCollection(collectionName)
    try {
      collection.createIndex(fieldsToBsonKeys(keys), IndexOptions().unique(unique).sparse(sparse))
        .execute()
    } catch {
      case NonFatal(e) => log.warn(s"Unable to create an index for $collectionName, keys: ${keys.mkString(", ")}: "
        + e.getMessage)
    }
  }

  /**
    * Drops an index for a given list of fields.
    */
  override def dropIndex(collectionName: String, keys: Seq[IndexField]): Unit = {
    log.info(s"Dropping an index for $collectionName, keys: ${keys.mkString(", ")}...")
    val collection = getCollection(collectionName)
    try {
      collection.dropIndex(fieldsToBsonKeys(keys))
        .execute()
    } catch {
      case NonFatal(e) => log.warn(s"Unable to drop an index for $collectionName, keys: ${keys.mkString(", ")}: "
        + e.getMessage)
    }
  }

  /**
    * Returns the number of documents in the specified collection.
    */
  override def getDocumentsCount(collectionName: String): Long = {
    getCollection(collectionName)
      .countDocuments()
      .execute()
  }

  /**
    * Inserts a document into a collection.
    */
  override def insertDocument(collectionName: String, document: String): Unit = {
    getCollection(collectionName)
      .insertOne(BsonDocument(document))
      .execute()
  }

  /**
    * Returns an iterator on all documents in the specified collection.
    *
    * Note. This method loads all documents into memory. Use this method only for small collections.
    *       Most of the time `forEachDocument()` is preferred.
    * @param collectionName A collection name to load documents from.
    * @return An iterator to documents in the collection.
    */
  override def getDocuments(collectionName: String): Iterator[String] = {
    log.info(s"Getting all documents for $collectionName...")
    getCollection(collectionName)
      .find()
      .execute()
      .toIterator
      .map(_.toJson())
  }

  /**
    * Traverses a collection and executes a function on each document.
    *
    * @param collectionName A collection name to load documents from.
    * @param f              A function to apply for each document in the collection.
    */
  def forEachDocument(collectionName: String)(f: String => Unit): Unit = {
    log.info(s"Processing all documents of $collectionName...")
    getCollection(collectionName)
      .find()
      .syncForeach(document => f(document.toJson()))
  }

  /**
    * Executes a command expressed in the database-specific language/format on the database.
    *
    * In MongoDB commands are expected to be in JSON format.
    *
    * Example:
    * {{{
    *   {
    *   aggregate: "foo",
    *   pipeline: [
    *     {
    *       \$match: {}
    *     },
    *     {
    *       \$out: "bar"
    *     }
    *   ],
    *   cursor: {},
    *   \$readPreference: {
    *     mode: "secondaryPreferred"
    *   },
    *   \$db: "blog"
    * }
    * }}}
    */
  override def executeCommand(cmd: String): Unit = {
    log.info(s"Executing MongoDB command: $cmd...")
    db.runCommand(BsonDocument(cmd)).execute()
  }

  /**
    * Returns a collection by name and ensures it exists.
    */
  private def getCollection(collectionName: String): MongoCollection[Document] = {
    if (!doesCollectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    db.getCollection(collectionName)
  }

  /**
    * Makes sure a collection exists, throws an exception otherwise.
    */
  private def ensureCollectionExists(collectionName: String): Unit = {
    if (!doesCollectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
  }

  /**
    * Returns a collection that is serialized as a case class.
    */
  private def getTypedCollection[T](collectionName: String)(implicit ct: ClassTag[T]): MongoCollection[T] = {
    db.getCollection[T](DatabaseVersionCollectionName)
      .withCodecRegistry(codecRegistry)
  }

  /**
    * Returns a Bson document from the list of index key fields
    */
  private def fieldsToBsonKeys(keys: Seq[IndexField]): Document = {
    Document(keys.map(_.toPair))
  }

  /**
    * Copies indexes from one collection to another. Skips the index on '_id' that is created automatically
    */
  private def copyIndexes(collectionFrom: String, collectionTo: String): Unit = {
    val indexes = getCollection(collectionFrom).listIndexes().execute()
    indexes.foreach(idxBson => {
      val indexDocument = idxBson("key").asDocument()
      if (!indexDocument.containsKey("_id")) {
        val indexOptions = idxBson.get("unique") match {
          case None         => IndexOptions()
          case Some(unique) => IndexOptions().unique(unique.asBoolean().getValue)
        }

        db.getCollection(collectionTo)
          .createIndex(indexDocument, indexOptions)
          .execute()
      }
    })
  }


}
