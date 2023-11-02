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

import org.scalatest.{BeforeAndAfterAll, Suite}

import java.sql._
import scala.collection.mutable.ListBuffer

trait RelationalDbFixture extends BeforeAndAfterAll {

  this: Suite =>

  val driver = "org.hsqldb.jdbc.JDBCDriver"
  val url = "jdbc:hsqldb:mem:mydb;sql.enforce_size=false"
  val user = "user"
  val password = "user"
  val database = "mydb"

  @throws[SQLException]
  def getConnection: Connection = DriverManager.getConnection(url, user, password)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName(driver)
  }

  override protected def afterAll(): Unit = {
    val connection = getConnection
    if (connection != null) connection.close()
    super.afterAll()
  }

  /* HSQLDB specific way of getting the list if tables */
  def getTables: Seq[String] = {
    val conn = getConnection
    val st: Statement = conn.createStatement()
    val rs: ResultSet = st.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_TYPE='TABLE'")

    val tables = new ListBuffer[String]
    while(rs.next()) {
      tables += rs.getString(1)
    }
    rs.close()
    st.close()
    conn.close()
    tables.toSeq
  }

  def withQuery(sql: String)(action: ResultSet => Unit): Unit = {
    val conn = getConnection
    val st: Statement = conn.createStatement()

    try {
      val rs: ResultSet = st.executeQuery(sql)
      try {
        action(rs)
      } finally {
        rs.close()
      }
    } finally {
      st.close()
      conn.close()
    }
  }
}
