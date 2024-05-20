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

package za.co.absa.pramen.core.sink

import org.apache.hadoop.fs.Path
import org.scalatest.wordspec.AnyWordSpec

class SparkSinkFormatSuite extends AnyWordSpec {
  "toString" should {
    "work for a path format" in {
      val format = SparkSinkFormat.PathFormat(new Path("/a/b/c"))

      assert(format.toString == "path: /a/b/c")
    }

    "work for a table format" in {
      val format = SparkSinkFormat.TableFormat("my_table1")

      assert(format.toString == "table: my_table1")
    }

    "work for a connection format" in {
      val format = SparkSinkFormat.ConnectionFormat("JDBC")

      assert(format.toString == "the JDBC connection")
    }
  }

}
