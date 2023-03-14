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

package za.co.absa.pramen.core.tests.utils.hive

import org.apache.spark.sql.AnalysisException
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.utils.hive.QueryExecutorSpark

class QueryExecutorSparkSuite extends AnyWordSpec with SparkTestBase {

  import spark.implicits._

  "QueryExecutorSpark" should {
    "execute Spark queries" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      df.createOrReplaceTempView("temp")

      val qe = new QueryExecutorSpark()

      qe.execute("SELECT * FROM temp")
    }

    "throw an exception on errors" in {
      val qe = new QueryExecutorSpark()

      val ex = intercept[AnalysisException] {
        qe.execute("SELECT dummy from dummy")
      }

      assert(ex.getMessage.contains("Table or view not found"))
    }

    "throw an exception if Hive is not initialized" in {
      val qe = new QueryExecutorSpark()

      val ex = intercept[IllegalArgumentException] {
        qe.doesTableExist("dummyDb", "dummyTable")
      }

      assert(ex.getMessage.contains("Database 'dummyDb' not found"))
    }

    "return false if the table is not found" in {
      val qe = new QueryExecutorSpark()

      val exist = qe.doesTableExist("default", "dummyTable")

      assert(!exist)
    }
  }
}
