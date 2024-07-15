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

package za.co.absa.pramen.core.fixtures

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.pramen.core.dao.MongoDb
import za.co.absa.pramen.core.mocks.dao.MongoDbSingleton
import za.co.absa.pramen.core.mongo.MongoDbConnection

trait MongoDbFixture extends BeforeAndAfterAll {

  this: Suite =>

  import za.co.absa.pramen.core.dao.ScalaMongoImplicits._

  private val (mongoDbExecutable, mongoPort) = MongoDbSingleton.embeddedMongoDb

  def connectionString: String = s"mongodb://localhost:$mongoPort"

  protected val dbName: String = "unit_test_database"

  protected var connection: MongoDbConnection = _
  protected var db: MongoDb = _
  protected var dbRaw: MongoDatabase = _

  private var mongoClient: MongoClient = _

  override protected def beforeAll(): Unit = {
    if (mongoDbExecutable.nonEmpty) {
      mongoClient = MongoClient(connectionString)

      connection = MongoDbConnection.getConnection(mongoClient, connectionString, dbName)

      val dbs = mongoClient.listDatabaseNames().execute()
      if (dbs.contains(dbName)) {
        throw new IllegalStateException(s"MongoDB migration db tools integration test database " +
          s"'$dbName' already exists at '$dbName'.")
      }

      dbRaw = mongoClient.getDatabase(dbName)
      db = new MongoDb(dbRaw)
    }

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      if (mongoClient != null)
        mongoClient.getDatabase(dbName).drop().execute()
    }
  }
}

