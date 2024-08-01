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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{DecimalType, MetadataBuilder, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TextComparisonFixture}
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.utils.JdbcSparkUtils
import za.co.absa.pramen.core.utils.JdbcSparkUtils.getJdbcOptions
import za.co.absa.pramen.core.utils.impl.JdbcFieldMetadata

class JdbcSparkUtilsSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with RelationalDbFixture with TextComparisonFixture {

  import spark.implicits._

  val jdbcConfig: JdbcConfig = JdbcConfig(driver, Some(url), Nil, None, Some(user), Some(password))

  override def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    super.afterAll()
  }

  "addMetadataFromJdbc" should {
    "add varchar metadata to Spark fields" in {
      val connectionOptions = JdbcSparkUtils.getJdbcOptions(url, jdbcConfig, RdbExampleTable.Company.tableName, Map.empty)

      val df = spark
        .read
        .format("jdbc")
        .options(connectionOptions)
        .load()

      val schema = StructType(df.schema.fields.map { field =>
        val metadata = new MetadataBuilder
        metadata.withMetadata(field.metadata)
        metadata.putLong("test_metadata", 0L)
        field.copy(metadata = metadata.build())
      })

      withQuery(s"SELECT * FROM ${RdbExampleTable.Company.tableName}") { rs =>
        val newSchema = JdbcSparkUtils.addMetadataFromJdbc(schema, rs.getMetaData)

        assert(newSchema.fields(1).name == "NAME")
        assert(newSchema.fields(1).metadata.getLong("maxLength") == 50L)
        assert(newSchema.fields(1).metadata.getLong("test_metadata") == 0L)
        assert(newSchema.fields(2).name == "DESCRIPTION")
        assert(!newSchema.fields(2).metadata.contains("maxLength"))
        assert(newSchema.fields(3).name == "EMAIL")
        assert(newSchema.fields(3).metadata.getLong("maxLength") == 50)
        assert(newSchema.fields(4).name == "FOUNDED")
        assert(!newSchema.fields(4).metadata.contains("maxLength"))
      }
    }
  }

  "addColumnDescriptionsFromJdbc" should {
    "add comments and keep custom metadata intact" in {
      val df = spark.read
        .format("jdbc")
        .options(getJdbcOptions(url, jdbcConfig, RdbExampleTable.Company.tableName, Map.empty))
        .load()

      var newSchema: StructType = null

      JdbcSparkUtils.withJdbcMetadata(jdbcConfig, s"SELECT * FROM ${RdbExampleTable.Company.tableName}") { (connection, metadataRs) =>
        newSchema = JdbcSparkUtils.addColumnDescriptionsFromJdbc(
          JdbcSparkUtils.addMetadataFromJdbc(df.schema, metadataRs),
          RdbExampleTable.Company.tableName,
          connection)
      }

      assert(newSchema.fields(0).name == "ID")
      assert(newSchema.fields(0).metadata.getString("comment") == "This is the record id")
      assert(newSchema.fields(1).name == "NAME")
      assert(newSchema.fields(1).metadata.getString("comment") == "This is company name")
      assert(newSchema.fields(1).metadata.getLong("maxLength") == 50)
    }
  }

  "getColumnMetadata" should {
    "work when just table name is specified" in {
      val metadataRs = JdbcSparkUtils.getColumnMetadata(RdbExampleTable.Company.tableName, getConnection)

      assert(metadataRs.next())
      metadataRs.close()
    }

    "work when just table name is specified in wrong case" in {
      val metadataRs = JdbcSparkUtils.getColumnMetadata("company", getConnection)

      assert(metadataRs.next())
      metadataRs.close()
    }

    "work when schema and table name are specified" in {
      val tableName = s"${RdbExampleTable.Company.schemaName}.${RdbExampleTable.Company.tableName}"
      val metadataRs = JdbcSparkUtils.getColumnMetadata(tableName, getConnection)

      assert(metadataRs.next())
      metadataRs.close()
    }

    "work when database, schema, and table name are specified" in {
      val tableName = s"${RdbExampleTable.Company.databaseName}.${RdbExampleTable.Company.schemaName}.${RdbExampleTable.Company.tableName}"
      val metadataRs = JdbcSparkUtils.getColumnMetadata(tableName, getConnection)

      assert(metadataRs.next())
      metadataRs.close()
    }
  }

  "withJdbcMetadata" should {
    "provide the metadata object for the query" in {
      JdbcSparkUtils.withJdbcMetadata(jdbcConfig, s"SELECT * FROM ${RdbExampleTable.Company.tableName}") { (connection, metadata) =>
        assert(!connection.isClosed)
        assert(metadata.getColumnName(2) == "NAME")
      }
    }
  }

  "withMetadataResultSet" should {
    "provide the resultset object for the query" in {
      JdbcSparkUtils.withMetadataResultSet(getConnection, s"SELECT * FROM ${RdbExampleTable.Company.tableName}") { rs =>
        assert(rs.getMetaData.getColumnName(2) == "NAME")
      }
    }
  }

  "convertTimestampToDates" should {
    "convert timestamp columns as dates" in {
      val expected =
        """{"long":1649319691,"str":"2022-01-18","date":"2022-01-18","ts":"2022-04-07"}
          |{"long":1649318691,"str":"2022-02-28","date":"2022-02-28","ts":"2022-04-07"}
          |""".stripMargin
      val df: DataFrame = Seq(
        (1649319691L, "2022-01-18"),
        (1649318691L, "2022-02-28")
      ).toDF("long", "str")
        .withColumn("date", $"str".cast("date"))
        .withColumn("ts", to_timestamp($"long"))

      val convertedDf = JdbcSparkUtils.convertTimestampToDates(df)

      val actual = convertedDf.toJSON.collect().mkString("\n")

      compareText(actual, expected)
    }

    "do nothing if the data frame does not contain date columns" in {
      val df: DataFrame = Seq(
        (1649319691L, "2022-01-18"),
        (1649318691L, "2022-02-28")
      ).toDF("long", "str")

      val convertedDf = JdbcSparkUtils.convertTimestampToDates(df)

      assert(df.eq(convertedDf))
    }
  }

  "getCorrectedDecimalsSchema" should {
    "correct decimal to int" in {
      val df: DataFrame = Seq("12345").toDF("value")
        .withColumn("value", $"value".cast("decimal(9, 0)"))

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = false)

      assert(customFields.contains("value integer"))
    }

    "correct decimal to long" in {
      val df: DataFrame = Seq("1234567890").toDF("value")
        .withColumn("value", $"value".cast("decimal(18, 0)"))

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = false)

      assert(customFields.contains("value long"))
    }

    "correct too big scale" in {
      val schema = StructType(Array(StructField("value", DecimalType(38, 20))))

      val dfOrig: DataFrame = Seq("1234567890").toDF("value")
        .withColumn("value", $"value".cast("decimal(28, 2)"))

      val df = spark.createDataFrame(dfOrig.rdd, schema)

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = false)

      assert(customFields.contains("value decimal(38, 18)"))
    }

    "correct invalid precision" in {
      val schema = StructType(Array(StructField("value", DecimalType(28, 20))))

      val dfOrig: DataFrame = Seq("1234567890").toDF("value")
        .withColumn("value", $"value".cast("decimal(28, 12)"))

      val df = spark.createDataFrame(dfOrig.rdd, schema)

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = true)

      assert(customFields.contains("value decimal(38, 18)"))
    }

    "correct invalid precision with small scale" in {
      val schema = StructType(Array(StructField("value", DecimalType(30, 16))))

      val dfOrig: DataFrame = Seq("1234567890").toDF("value")
        .withColumn("value", $"value".cast("decimal(38, 18)"))

      val df = spark.createDataFrame(dfOrig.rdd, schema)

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = true)

      assert(customFields.contains("value decimal(38, 16)"))
    }

    "do nothing if the field is okay" in {
      val df: DataFrame = Seq("12345").toDF("value")
        .withColumn("value", $"value".cast("int"))

      val customFields = JdbcSparkUtils.getCorrectedDecimalsSchema(df, fixPrecision = true)

      assert(customFields.isEmpty)
    }
  }

  "getFieldMetadata" should {
    "convert sized varchar to the internal model" in {
      val expectedVarcharN = JdbcFieldMetadata(
        name = "NAME",
        label = "NAME",
        sqlType = java.sql.Types.VARCHAR,
        sqlTypeName = "VARCHAR",
        displaySize = 50,
        precision = 50,
        scale = 0,
        nullable = false
      )

      withQuery("SELECT name FROM COMPANY WHERE 0=1") { rs =>
        val actualVarcharN = JdbcSparkUtils.getFieldMetadata(rs.getMetaData, 1)

        assert(actualVarcharN == expectedVarcharN)
      }
    }

    "work with unsized data types" in {
      val expectedVarchar = JdbcFieldMetadata(
        name = "DESCRIPTION",
        label = "DESCRIPTION",
        sqlType = java.sql.Types.VARCHAR,
        sqlTypeName = "VARCHAR",
        displaySize = 32768,
        precision = 32768,
        scale = 0,
        nullable = false
      )

      withQuery("SELECT description FROM COMPANY WHERE 0=1") { rs =>
        val actualVarchar = JdbcSparkUtils.getFieldMetadata(rs.getMetaData, 1)

        assert(actualVarchar == expectedVarchar)
      }
    }
  }

  "getJdbcOptions" should {
    "combine all provided options to a single map" in {
      val jdbcConfig = JdbcConfig(
        primaryUrl = Option(url),
        user = Option("user"),
        password = Option("password1"),
        database = Option("mydb"),
        driver = "org.hsqldb.jdbc.JDBCDriver",
        extraOptions = Map("key1" -> "value1", "key2" -> "value2")
      )

      val extraOptions = Map("key1" -> "value11", "key3" -> "value3")

      val options = JdbcSparkUtils.getJdbcOptions(url, jdbcConfig, "my_table", extraOptions)

      assert(options.size == 9)
      assert(options("url") == url)
      assert(options("user") == "user")
      assert(options("password") == "password1")
      assert(options("dbtable") == "my_table")
      assert(options("database") == "mydb")
      assert(options("driver") == "org.hsqldb.jdbc.JDBCDriver")
      assert(options("key1") == "value11")
      assert(options("key2") == "value2")
      assert(options("key3") == "value3")
    }

    "work with minimum configuration" in {
      val jdbcConfig = JdbcConfig(
        driver = "org.hsqldb.jdbc.JDBCDriver",
        primaryUrl = None
      )

      val extraOptions = Map.empty[String, String]

      val options = JdbcSparkUtils.getJdbcOptions(url, jdbcConfig, "my_table", extraOptions)

      assert(options.size == 3)
      assert(options("url") == url)
      assert(options("dbtable") == "my_table")
      assert(options("driver") == "org.hsqldb.jdbc.JDBCDriver")
    }
  }
}
