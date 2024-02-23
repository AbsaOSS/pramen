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

package za.co.absa.pramen.core.samples

import java.sql.{Connection, SQLException}

trait RdbExampleTable {
  val tableName: String
  val ddl: String
  val comments: Seq[String]
  val inserts: Seq[String]

  @throws[SQLException]
  def initTable(connection: Connection): Unit = {
    val statement = connection.createStatement

    statement.execute(ddl)
    connection.commit()

    comments.foreach(sql => statement.executeUpdate(sql))
    connection.commit()

    inserts.foreach(sql => statement.executeUpdate(sql))
    connection.commit()
  }

  @throws[SQLException]
  def dropTable(connection: Connection): Unit = {
    val statement = connection.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $tableName")
      connection.commit()
    } finally {
      if (statement != null) statement.close()
    }
  }
}

object RdbExampleTable {
  object Company extends RdbExampleTable {
    val tableName: String = "COMPANY"
    val schemaName: String = "PUBLIC"
    val databaseName: String = "PUBLIC"

    val ddl: String =
      s"""
         |CREATE TABLE $tableName (
         |  id INT NOT NULL,
         |  name VARCHAR(50) NOT NULL,
         |  description VARCHAR NOT NULL,
         |  email VARCHAR(50) NOT NULL,
         |  founded DATE NOT NULL,
         |  last_updated TIMESTAMP NOT NULL,
         |  info_date VARCHAR(10) NOT NULL,
         |  PRIMARY KEY (id))
         |""".stripMargin

    val comments: Seq[String] = Seq(
      s"COMMENT ON COLUMN $tableName.id IS 'This is the record id'",
      s"COMMENT ON COLUMN $tableName.name IS 'This is company name'"
    )

    val inserts: Seq[String] = Seq(
      s"INSERT INTO $tableName VALUES (1,'Company1', 'description1', 'company1@example.com', DATE '2000-10-11', TIMESTAMP '2020-11-04 10:11:00+02:00', '2022-02-18')",
      s"INSERT INTO $tableName VALUES (2,'Company2', 'description2', 'company2@example.com', DATE '2005-03-29', TIMESTAMP '2020-11-04 10:22:33+02:00', '2022-02-18')",
      s"INSERT INTO $tableName VALUES (3,'Company3', 'description3', 'company3@example.com', DATE '2016-12-30', TIMESTAMP '2020-11-04 10:33:59+02:00', '2022-02-18')",
      s"INSERT INTO $tableName VALUES (4,'Company4', 'description4', 'company4@example.com', DATE '2016-12-31', TIMESTAMP '2020-11-04 10:34:22+02:00', '2022-02-19')"
    )
  }

  object Empty extends RdbExampleTable {
    val tableName: String = "empty"

    val ddl: String =
      s"""
         |CREATE TABLE $tableName (
         |  id INT NOT NULL,
         |  PRIMARY KEY (id))
         |""".stripMargin

    val comments = Seq.empty[String]

    val inserts = Seq.empty[String]
  }

}
