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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class CsvConversionParamsSuite extends AnyWordSpec {
  "fromConfig" should {
    "construct from config with default values" in {
      val conf = ConfigFactory.parseString(
        """file.name.pattern = OUTPUT_%Y%m%d_%H%M%S.csv
          |temp.hadoop.path = /dummy/temp
          |""".stripMargin)

      val params = CsvConversionParams.fromConfig(conf)

      assert(params.dateFormat == "yyyy-MM-dd")
      assert(params.timestampFormat == "yyyy-MM-dd HH:mm:ss Z")
      assert(params.fileNamePattern == "OUTPUT_%Y%m%d_%H%M%S.csv")
      assert(params.tempHadoopPath == "/dummy/temp")
      assert(params.columnNameTransform == ColumnNameTransform.NoChange)
    }

    "construct from config with custom values" in {
      val conf = ConfigFactory.parseString(
        """file.name.pattern = OUTPUT.csv
          |temp.hadoop.path = /dummy/temp2
          |date.format = "dd-MM-yyyy"
          |timestamp.format = "yyyy-MM-dd HH:mm"
          |column.name.transform = "make_upper"
          |option {
          |  header = true
          |  sep = ","
          |}
          |""".stripMargin)

      val params = CsvConversionParams.fromConfig(conf)

      assert(params.dateFormat == "dd-MM-yyyy")
      assert(params.timestampFormat == "yyyy-MM-dd HH:mm")
      assert(params.fileNamePattern == "OUTPUT.csv")
      assert(params.tempHadoopPath == "/dummy/temp2")
      assert(params.columnNameTransform == ColumnNameTransform.MakeUpper)
      assert(params.csvOptions("header") == "true")
      assert(params.csvOptions("sep") == ",")
    }

    "throw an exception when mandatory options missing" in {
      val conf = ConfigFactory.empty()

      val ex = intercept[IllegalArgumentException] {
        CsvConversionParams.fromConfig(conf)
      }

      assert(ex.getMessage.contains("Mandatory configuration options are missing: temp.hadoop.path"))
    }
  }
}
