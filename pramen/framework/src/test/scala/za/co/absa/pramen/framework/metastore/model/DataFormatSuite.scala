/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.metastore.model

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.framework.metastore.model.DataFormat._

class DataFormatSuite extends WordSpec {
  "fromConfig()" should {
    "use 'parquet' as the default format" in {
      val conf = ConfigFactory.parseString("""path = /a/b/c""")

      val format = DataFormat.fromConfig(conf)

      assert(format.name == "parquet")
      assert(format.isInstanceOf[Parquet])
      assert(format.asInstanceOf[Parquet].path == "/a/b/c")
      assert(format.asInstanceOf[Parquet].recordsPerPartition.isEmpty)
    }

    "use 'parquet' when specified explicitly" in {
      val conf = ConfigFactory.parseString(
      """
          |format = parquet
          |path = /a/b/c
          |records.per.partition = 100
          |""".stripMargin)

      val format = DataFormat.fromConfig(conf)

      assert(format.name == "parquet")
      assert(format.isInstanceOf[Parquet])
      assert(format.asInstanceOf[Parquet].path == "/a/b/c")
      assert(format.asInstanceOf[Parquet].recordsPerPartition.contains(100))
    }

    "use 'delta' when specified explicitly" in {
      val conf = ConfigFactory.parseString(
        """
          |format = delta
          |path = /a/b/c
          |records.per.partition = 200
          |""".stripMargin)

      val format = DataFormat.fromConfig(conf)

      assert(format.name == "delta")
      assert(format.isInstanceOf[Delta])
      assert(format.asInstanceOf[Delta].query.isInstanceOf[Query.Path])
      assert(format.asInstanceOf[Delta].query.query == "/a/b/c")
      assert(format.asInstanceOf[Delta].recordsPerPartition.contains(200))
    }

    "throw an exception on unknown format" in {
      val conf = ConfigFactory.parseString(
        """
          |format = isberg
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        DataFormat.fromConfig(conf)
      }

      assert(ex.getMessage.contains("Unknown format: isberg"))
    }

    "throw an exception on mandatory options missing" in {
      val conf = ConfigFactory.parseString(
        """
          |format = parquet
          |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        DataFormat.fromConfig(conf)
      }

      assert(ex.getMessage.contains("Mandatory option missing: path"))
    }
  }

}
