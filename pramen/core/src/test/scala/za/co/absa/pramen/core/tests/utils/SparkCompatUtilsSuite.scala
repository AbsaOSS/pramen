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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.utils.SparkCompatUtils

class SparkCompatUtilsSuite extends AnyWordSpec with SparkTestBase {
  "col2expr" should {
    "convert a column reference to a Catalyst expression" in {
      val expr = SparkCompatUtils.col2expr(org.apache.spark.sql.functions.col("a"))

      assert(expr != null)
      assert(expr.isInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
      assert(expr.sql.contains("a"))
    }

    "convert a literal to a Catalyst expression" in {
      val expr = SparkCompatUtils.col2expr(org.apache.spark.sql.functions.lit(42))

      assert(expr != null)
      assert(expr.sql.contains("42"))
    }

    "convert a complex expression" in {
      val column = (org.apache.spark.sql.functions.col("a") + 1) * 2
      val expr = SparkCompatUtils.col2expr(column)

      assert(expr != null)
      assert(expr.sql.contains("a"))
      assert(expr.sql.contains("1"))
      assert(expr.sql.contains("2"))
    }

    "be usable multiple times since it is a lazy val" in {
      val expr1 = SparkCompatUtils.col2expr(org.apache.spark.sql.functions.col("a"))
      val expr2 = SparkCompatUtils.col2expr(org.apache.spark.sql.functions.col("b"))

      assert(expr1.sql.contains("a"))
      assert(expr2.sql.contains("b"))
    }
  }

  "expr2col" should {
    "convert a Catalyst expression to a column" in {
      val expr = org.apache.spark.sql.catalyst.expressions.Literal(42)
      val column = SparkCompatUtils.expr2col(expr)

      assert(column != null)
      assert(column.isInstanceOf[org.apache.spark.sql.Column])
      assert(column.toString.contains("42"))
    }

    "convert an unresolved attribute to a column" in {
      val expr = org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute(Seq("a"))
      val column = SparkCompatUtils.expr2col(expr)

      assert(column != null)
      assert(column.toString.contains("a"))
    }

    "produce a column that can be used in a query" in {
      import spark.implicits._

      val df = List((1, "x"), (2, "y")).toDF("id", "name")
      val column = SparkCompatUtils.expr2col(org.apache.spark.sql.catalyst.expressions.Literal(7))

      val actual = df.select(column.as("value")).collect().map(_.getInt(0)).toSeq

      assert(actual == Seq(7, 7))
    }
  }

  "col2expr and expr2col" should {
    "round trip a simple column" in {
      val original = org.apache.spark.sql.functions.col("a")
      val actual = SparkCompatUtils.expr2col(SparkCompatUtils.col2expr(original))

      assert(actual.toString == original.toString)
    }

    "round trip a literal column" in {
      val original = org.apache.spark.sql.functions.lit("abc")
      val actual = SparkCompatUtils.expr2col(SparkCompatUtils.col2expr(original))

      assert(actual.toString.contains("abc"))
    }

    "round trip a complex column used in a query" in {
      import spark.implicits._

      val df = List((1, "x"), (2, "y")).toDF("id", "name")
      val original = (org.apache.spark.sql.functions.col("id") + 1) * 2
      val roundTripped = SparkCompatUtils.expr2col(SparkCompatUtils.col2expr(original))

      val expected = df.select(original.as("v")).collect().map(_.getInt(0)).toSeq
      val actual = df.select(roundTripped.as("v")).collect().map(_.getInt(0)).toSeq

      assert(actual == expected)
      assert(actual == Seq(4, 6))
    }

    "round trip an expression" in {
      val original: org.apache.spark.sql.catalyst.expressions.Expression =
        org.apache.spark.sql.catalyst.expressions.Literal(10)
      val actual = SparkCompatUtils.col2expr(SparkCompatUtils.expr2col(original))

      assert(actual.sql == original.sql)
    }
  }

}

