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

import za.co.absa.pramen.core.dao.model.IndexField


/**
  * This abstract class is the contract a document database need to implement in order to run migrations.
  */
abstract class DocumentDb {

  /**
    * Returns the version of the database. Version of a database determines the schema used for writes.
    */
  def getVersion(): Int

  /**
    * Sets the version of the database. When a version is set it implies a migration to that version is completed
    * successfully and the database can be used with the specified version of the schema.
    */
  def setVersion(version: Int): Unit

  /**
    * Returns true if the specified collection exists in the database.
    */
  def doesCollectionExists(collectionName: String): Boolean

  /**
    * Creates a collection with the given name.
    */
  def createCollection(collectionName: String): Unit

  /**
    * Drop a collection.
    */
  def dropCollection(collectionName: String): Unit

  /**
    * Removes all documents from a collection.
    */
  def emptyCollection(collectionName: String): Unit

  /**
    * Renames a collection
    */
  def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit

  /**
    * Copies contents of a collection to a collection with a different name in the same database.
    */
  def cloneCollection(collectionName: String, newCollectionName: String): Unit

  /**
    * Creates an index for a given list of keys.
    */
  def createIndex(collectionName: String, keys: Seq[IndexField], unique: Boolean = false, sparse: Boolean = false): Unit

  /**
    * Drop an index for a given list of keys.
    */
  def dropIndex(collectionName: String, keys: Seq[IndexField]): Unit

  /**
    * Returns the number of documents in the specified collection.
    */
  def getDocumentsCount(collectionName: String): Long

  /**
    * Inserts a document into a collection.
    */
  def insertDocument(collectionName: String, document: String): Unit

  /**
    * Returns an iterator on all documents in the specified collection.
    *
    * Note. This method loads all documents into memory. Use this method only for small collections.
    *       Most of the time `forEachDocument()` is preferred.
    * @param collectionName A collection name to load documents from.
    * @return An iterator to documents in the collection.
    */
  def getDocuments(collectionName: String): Iterator[String]

  /**
    * Traverses a collection and executes a function on each document.
    *
    * @param collectionName A collection name to load documents from.
    * @param f              A function to apply for each document in the collection.
    */
  def forEachDocument(collectionName: String)(f: String => Unit): Unit

  /**
    * Executes a command expressed in the database-specific language/format on the database.
    */
  def executeCommand(cmd: String): Unit
}
