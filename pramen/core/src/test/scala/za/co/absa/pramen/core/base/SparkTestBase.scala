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

package za.co.absa.pramen.core.base

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkTestBase {
  // Turn off as much as possible Spark logging in tests
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress","127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.session.timeZone", "Africa/Johannesburg")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()


  def stripLineEndings(str: String): String = {
    //make testing compatible for windows
    str.stripMargin.linesIterator.mkString("").trim
  }
}
