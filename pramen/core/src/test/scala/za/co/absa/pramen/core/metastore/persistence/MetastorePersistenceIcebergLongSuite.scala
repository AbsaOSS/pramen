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

package za.co.absa.pramen.core.metastore.persistence

import org.apache.hadoop.fs.Path
import org.apache.iceberg.hadoop.HadoopTables
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{CatalogTable, DataFormat, PartitionScheme, Query}
import za.co.absa.pramen.core.base.SparkTestIcebergBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistence
import za.co.absa.pramen.core.mocks.MetaTableFactory

import java.time.LocalDate
import scala.collection.JavaConverters._
import scala.util.Random

class MetastorePersistenceIcebergLongSuite extends AnyWordSpec
  with SparkTestIcebergBase
  with TempDirFixture
  with TextComparisonFixture {

  import spark.implicits._

  "Iceberg" when {
    val df1 = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
    val df2 = List(("D", 4)).toDF("a", "b")

    val infoDate1 = LocalDate.parse("2021-02-18")
    val infoDate2 = LocalDate.parse("2022-03-19")

    def runBasicTests(mt: MetastorePersistence, query: Query): Assertion = {
      // double write should not duplicate partitions
      mt.saveTable(infoDate1, df1, None)
      mt.saveTable(infoDate1, df1, None)

      assert(mt.loadTable(Some(infoDate1), Some(infoDate2)).count() == 3)

      mt.saveTable(infoDate2, df2, None)

      assert(mt.loadTable(None, None).count() == 4)
    }

    "table targets" should {
      "create, read, and append daily partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_iceberg_part_table1" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByDay)

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 1)
        assert(partitionColumns.head == "info_date")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append monthly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_iceberg_part_table2" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByMonth("info_month", "info_year"))

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 2)
        assert(partitionColumns.head == "info_date_year")
        assert(partitionColumns(1) == "info_date_month")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append year-month partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_iceberg_part_table3" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByYearMonth("info_month"))

        assertThrows[UnsupportedOperationException] {
          runBasicTests(mt, Query.Table(tableName))
        }
      }

      "create, read, and append yearly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_iceberg_part_table4" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByYear("info_year"))

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 1)
        assert(partitionColumns.head == "info_date_year")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append non-partitioned tables" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_iceberg_part_table5" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.NotPartitioned)

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.isEmpty)

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }
    }
  }

  def getTablePartitions(tableName: String): Seq[String] = {
    val location = new Path(hadoopTempDir, s"pramen/iceberg_catalog/default/$tableName").toString
    val ht = new HadoopTables
    val table = ht.load(location)

    table.spec()
      .fields()
      .asScala
      .map(_.name())
      .toSeq

    /** Alternative way. Does not work on all iceberg versions since describe format depends on it. */
    /*spark.sql(s"DESCRIBE EXTENDED $tableName")
      .filter(col("col_name").startsWith("Part "))
      .withColumn("num", regexp_extract(col("col_name"), "Part\\s+(\\d+)", 1).cast(IntegerType))
      .orderBy("num")
      .select(col("data_type"))
      .collect()
      .map(_(0).toString)
      .toSeq*/
  }

  def getDeltaMtPersistence(query: Query,
                            partitionScheme: PartitionScheme = PartitionScheme.PartitionByDay): MetastorePersistence = {
    val catalogTable = CatalogTable(None, None, query.query)
    val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
      format = DataFormat.Iceberg(catalogTable, None),
      partitionScheme = partitionScheme,
      infoDateColumn = "info_date"
    )

    spark.sql(s"DROP TABLE IF EXISTS ${catalogTable.getFullTableName}").count()

    MetastorePersistence.fromMetaTable(mt, null, batchId = 0)
  }
}
