package za.co.absa.pramen.framework.samples

import java.sql.{Connection, SQLException}

trait RdbExampleTable {
  val tableName: String
  val ddl: String
  val inserts: Seq[String]

  @throws[SQLException]
  def initTable(connection: Connection): Unit = {
    val statement = connection.createStatement

    statement.execute(ddl)
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
    val tableName: String = "company"

    val ddl: String =
      s"""
         |CREATE TABLE $tableName (
         |  id INT NOT NULL,
         |  name VARCHAR(50) NOT NULL,
         |  email VARCHAR(50) NOT NULL,
         |  founded DATE NOT NULL,
         |  last_updated TIMESTAMP NOT NULL,
         |  PRIMARY KEY (id))
         |""".stripMargin

    val inserts = Seq(
      s"INSERT INTO $tableName VALUES (1,'Company1', 'company1@example.com', DATE '2000-10-11', TIMESTAMP '2020-11-04 10:11:00+02:00')",
      s"INSERT INTO $tableName VALUES (2,'Company2', 'company2@example.com', DATE '2005-03-29', TIMESTAMP '2020-11-04 10:22:33+02:00')",
      s"INSERT INTO $tableName VALUES (3,'Company3', 'company3@example.com', DATE '2016-12-30', TIMESTAMP '2020-11-04 10:33:59+02:00')"
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

    val inserts = Seq.empty[String]
  }

}
