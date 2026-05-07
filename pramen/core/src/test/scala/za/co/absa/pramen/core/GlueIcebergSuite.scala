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

package za.co.absa.script

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream

/**
  * This is the Glue job to use for running Pramen on Glue.
  *
  * See documentation of AqueductPramenJob in 'pramen-components' module for the info on usage.
  */

object MyApp {
  private val log = LoggerFactory.getLogger(this.getClass)

  def showString(df: DataFrame, numRows: Int = 20): String = {
    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(numRows, truncate = false)
    }
    new String(outCapture.toByteArray).replace("\r\n", "\n")
  }

  // spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  // --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  // --conf spark.sql.catalog.glue_catalog.warehouse=s3://aqueduct-pipelines-dev-application-data/iceberg-warehouse
  // --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  // --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  // --conf spark.sql.defaultCatalog=glue_catalog
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("test")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
      .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.glue_catalog.warehouse", "s3://aqueduct-pipelines-uat-application-data/iceberg-warehouse")
      .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.glue_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.glue_catalog.s3.access-points.ursamajor-afs1-dev-edla-aqdt-za", "arn:aws:s3:af-south-1:131405913869:accesspoint/aqdt-za-npintdeaqueduct")
      .config("spark.sql.defaultCatalog", "glue_catalog")
      .getOrCreate()

    //log.error(showString(spark.catalog.listTables("edla_dev_aqdt_za_publish_rl").toDF()))
    //log.error(showString(spark.table("glue_catalog.edla_dev_aqdt_za_publish_rl.aq_journal_iceberg4")))

    //spark.sql(
    //  """
    //    | ALTER TABLE glue_catalog.edla_dev_aqdt_za_publish_rl.aq_journal_iceberg4 CREATE TAG my_tag AS OF VERSION 7314587597447473054
    //    |""".stripMargin).collect()

    val df1 = spark.sql("SELECT * FROM glue_catalog.edla_dev_aqdt_za_publish_rl.aq_journal_iceberg4.refs")
    log.error(showString(df1))
  }
}
