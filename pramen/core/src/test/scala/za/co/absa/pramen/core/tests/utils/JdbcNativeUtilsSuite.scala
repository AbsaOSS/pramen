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

package za.co.absa.pramen.core.tests.utils

import org.apache.spark.sql.types.IntegerType
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TextComparisonFixture}
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.impl.ResultSetToRowIterator
import za.co.absa.pramen.core.utils.{JdbcNativeUtils, SparkUtils}

import java.sql._
import java.time.{Instant, ZoneId}
import java.util.{Calendar, GregorianCalendar, TimeZone}

class JdbcNativeUtilsSuite extends AnyWordSpec with RelationalDbFixture with SparkTestBase with TextComparisonFixture {
  private val tableName = RdbExampleTable.Company.tableName
  private val jdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Option(user), Option(password))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "In memory JDBC table" should {
    "be available" in {
      val df = spark
        .read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("user", user)
        .option("password", password)
        .option("dbtable", s"(SELECT * FROM $tableName) tbl")
        .load()

      assert(df.schema.fields.nonEmpty)
      assert(df.schema.fields.head.name == "ID")
      assert(df.schema.fields.head.dataType == IntegerType)
      assert(df.count() == 4)
    }
  }

  "getConnection()" should {
    "select working connection when provided a connection pool" in {
      val jdbcConfig = JdbcConfig(driver, Some("bogus_url"), "bogus_url2" :: url :: Nil, None, Option(user), Option(password))

      val (actualUrl, conn) = JdbcNativeUtils.getConnection(jdbcConfig)
      conn.close()

      assert(actualUrl == url)
    }

  }

  "getJdbcNativeRecordCount()" should {
    val conf = JdbcConfig(driver, Some(url), Nil, None, Option(user), Option(password))

    "return record count when data is available" in {
      val count = JdbcNativeUtils.getJdbcNativeRecordCount(conf, conf.primaryUrl.get, s"SELECT id FROM $tableName WHERE id = 1")

      assert(count == 1)
    }

    "return record count even if scrollable cursor is not  available" in {
      val connection = DriverManager.getConnection(jdbcConfig.primaryUrl.get, jdbcConfig.user.get, jdbcConfig.password.get)
      val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val resultSet = statement.executeQuery(s"SELECT * FROM $tableName")

      val count = JdbcNativeUtils.getResultSetCount(resultSet)

      assert(count == 4)
    }

    "throw an exception on error" in {
      intercept[SQLSyntaxErrorException] {
        JdbcNativeUtils.getJdbcNativeRecordCount(conf, conf.primaryUrl.get, s"SELECT id FROM no_such_table")
      }
    }
  }

  "getJdbcNativeDataFrame()" should {
    "return proper schema from a JDBC query" in {
      val df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, jdbcConfig.primaryUrl.get, s"SELECT * FROM $tableName WHERE id = 1")
      val expected =
        """{
          |  "type" : "struct",
          |  "fields" : [ {
          |    "name" : "ID",
          |    "type" : "integer",
          |    "nullable" : true,
          |    "metadata" : { }
          |  }, {
          |    "name" : "NAME",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : {
          |      "maxLength" : 50
          |    }
          |  }, {
          |    "name" : "DESCRIPTION",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : { }
          |  }, {
          |    "name" : "EMAIL",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : {
          |      "maxLength" : 50
          |    }
          |  }, {
          |    "name" : "FOUNDED",
          |    "type" : "date",
          |    "nullable" : true,
          |    "metadata" : { }
          |  }, {
          |    "name" : "LAST_UPDATED",
          |    "type" : "timestamp",
          |    "nullable" : true,
          |    "metadata" : { }
          |  }, {
          |    "name" : "INFO_DATE",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : {
          |      "maxLength" : 10
          |    }
          |  } ]
          |}""".stripMargin

      val actual = df.schema.prettyJson

      compareText(actual, expected)
    }

    "return proper data from a JDBC query" in {
      val expected =
        """[ {
          |  "ID" : 1,
          |  "NAME" : "Company1",
          |  "EMAIL" : "company1@example.com",
          |  "FOUNDED" : "2000-10-11"
          |}, {
          |  "ID" : 2,
          |  "NAME" : "Company2",
          |  "EMAIL" : "company2@example.com",
          |  "FOUNDED" : "2005-03-29"
          |}, {
          |  "ID" : 3,
          |  "NAME" : "Company3",
          |  "EMAIL" : "company3@example.com",
          |  "FOUNDED" : "2016-12-30"
          |}, {
          |  "ID" : 4,
          |  "NAME" : "Company4",
          |  "EMAIL" : "company4@example.com",
          |  "FOUNDED" : "2016-12-31"
          |} ]""".stripMargin

      val df = JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, jdbcConfig.primaryUrl.get, s"SELECT id, name, email, founded FROM $tableName")
      val actual = SparkUtils.convertDataFrameToPrettyJSON(df)

      compareText(actual, expected)
    }

    "throw an exception on error" in {
      intercept[SQLSyntaxErrorException] {
        JdbcNativeUtils.getJdbcNativeDataFrame(jdbcConfig, jdbcConfig.primaryUrl.get, s"SELECT id FROM no_such_table")
      }
    }
  }

  "sanitizeTimestamp" should {
    // From Spark:
    // https://github.com/apache/spark/blob/ad8ac17dbdfa763236ab3303eac6a3115ba710cc/connector/docker-integration-tests/src/test/scala/org/apache/spark/sql/jdbc/PostgresIntegrationSuite.scala#L457
    val minTimestamp = -62135596800000L
    val maxTimestamp = 253402300799999L

    // Variable names come from PostgreSQL "constant field docs":
    // https://jdbc.postgresql.org/documentation/publicapi/index.html?constant-values.html
    val POSTGRESQL_DATE_NEGATIVE_INFINITY: Long = -9223372036832400000L
    val POSTGRESQL_DATE_POSITIVE_INFINITY: Long = 9223372036825200000L

    val resultSet = mock(classOf[ResultSet])
    val resultSetMetaData = mock(classOf[ResultSetMetaData])

    when(resultSetMetaData.getColumnCount).thenReturn(1)
    when(resultSet.getMetaData).thenReturn(resultSetMetaData)

    "convert PostgreSql positive infinity value" in {
      val iterator = new ResultSetToRowIterator(resultSet, true)
      val timestamp = Timestamp.from(Instant.ofEpochMilli(POSTGRESQL_DATE_POSITIVE_INFINITY))

      val fixedTs = iterator.sanitizeTimestamp(timestamp)

      assert(fixedTs.getTime == maxTimestamp)
    }

    "convert PostgreSql negative infinity value" in {
      val iterator = new ResultSetToRowIterator(resultSet, true)
      val timestamp = Timestamp.from(Instant.ofEpochMilli(POSTGRESQL_DATE_NEGATIVE_INFINITY))

      val fixedTs = iterator.sanitizeTimestamp(timestamp)

      assert(fixedTs.getTime == minTimestamp)
    }

    "convert overflowed value to the maximum value supported" in {
      val iterator = new ResultSetToRowIterator(resultSet, true)
      val timestamp = Timestamp.from(Instant.ofEpochMilli(1000000000000000L))

      val actual = iterator.sanitizeTimestamp(timestamp)

      val calendar = new GregorianCalendar(TimeZone.getTimeZone(ZoneId.of("UTC")))
      calendar.setTime(actual)
      val year = calendar.get(Calendar.YEAR)

      assert(year == 9999)
      assert(actual.getTime == maxTimestamp)
    }

    "do nothing if the feature is turned off" in {
      val iterator = new ResultSetToRowIterator(resultSet, false)
      val timestamp = Timestamp.from(Instant.ofEpochMilli(1000000000000000L))

      val actual = iterator.sanitizeTimestamp(timestamp)

      val calendar = new GregorianCalendar(TimeZone.getTimeZone(ZoneId.of("UTC")))
      calendar.setTime(actual)
      val year = calendar.get(Calendar.YEAR)

      assert(year == 33658)
    }
  }

}
