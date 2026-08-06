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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistenceRaw.{RAW_COPY_FIELD_KEY, RAW_PATH_FIELD_KEY}
import za.co.absa.pramen.core.metastore.peristence.RawFile

class RawFileSuite extends AnyWordSpec with SparkTestBase {
  import spark.implicits._

  "fromDf" should {
    "return a list of files with the default copy flag" in {
      val df = Seq("file1.txt", "file2.tst").toDF(RAW_PATH_FIELD_KEY)

      val files = RawFile.fromDf(df)

      assert(files.exists(_.filePath == "file1.txt"))
      assert(files.exists(_.filePath == "file2.tst"))
      assert(files.forall(_.needsCopying))
    }

    "return a list of files with the explicit copy flag" in {
      val df = Seq(("file1.txt", false), ("file2.tst", true)).toDF(RAW_PATH_FIELD_KEY, RAW_COPY_FIELD_KEY)

      val files = RawFile.fromDf(df)

      assert(files.exists(f => f.filePath == "file1.txt" && !f.needsCopying))
      assert(files.exists(f => f.filePath == "file2.tst" && f.needsCopying))
    }

    "throw an exception when the DataFrame is missing required columns" in {
      val df = Seq("file1.txt", "file2.tst").toDF("wrong_column")

      assertThrows[IllegalArgumentException] {
        RawFile.fromDf(df)
      }
    }
  }
}
