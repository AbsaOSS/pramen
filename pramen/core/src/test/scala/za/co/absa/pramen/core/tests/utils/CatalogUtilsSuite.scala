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

import org.apache.spark.sql.Row
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.utils.CatalogUtils

import scala.collection.JavaConverters._

class CatalogUtilsSuite extends AnyWordSpec with SparkTestBase{
  "doesTableExist" should {
    "return false for non-existing table" in {
      val tableName = "non_existing_table_xyz_123"

      val exists = CatalogUtils.doesTableExist(tableName)

      assert(!exists)
    }

    "return true for existing temporary view" in {
      val tableName = "test_temp_view"
      val data = Seq(Row(1, "a"), Row(2, "b"))
      val schema = "id INT, name STRING"

      val df = spark.createDataFrame(data.asJava, spark.sessionState.sqlParser.parseTableSchema(schema))
      df.createTempView(tableName)

      val exists = CatalogUtils.doesTableExist(tableName)

      assert(exists)

      spark.catalog.dropTempView(tableName)
    }

    "return false after dropping temporary view" in {
      val tableName = "test_temp_view_to_drop"
      val data = Seq(Row(1, "a"))
      val schema = "id INT, name STRING"

      val df = spark.createDataFrame(data.asJava, spark.sessionState.sqlParser.parseTableSchema(schema))
      df.createTempView(tableName)
      spark.catalog.dropTempView(tableName)

      val exists = CatalogUtils.doesTableExist(tableName)

      assert(!exists)
    }

    "return false for non-existing table with schema prefix" in {
      val tableName = "default.non_existing_table_with_schema"

      val exists = CatalogUtils.doesTableExist(tableName)

      assert(!exists)
    }
  }

}
