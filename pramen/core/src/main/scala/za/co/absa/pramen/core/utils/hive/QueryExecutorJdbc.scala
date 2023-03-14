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

package za.co.absa.pramen.core.utils.hive

import org.slf4j.LoggerFactory

import java.sql.{Connection, ResultSet, SQLSyntaxErrorException}
import scala.util.Try

class QueryExecutorJdbc(connection: Connection) extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def doesTableExist(dbName: String, tableName: String): Boolean = {
    val query = s"SELECT 1 FROM $tableName WHERE 0 = 1"

    Try {
      execute(query)
    }.isSuccess
  }

  @throws[SQLSyntaxErrorException]
  override def execute(query: String): Unit = {
    log.info(s"Executing SQL: $query")
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val resultSet = statement.executeQuery(query)

    resultSet.close()
  }
}
