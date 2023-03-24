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

package za.co.absa.pramen.core.tests.reader

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{Query, TableReader}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.reader.TableReaderSpark
import za.co.absa.pramen.core.utils.FsUtils

import java.time.LocalDate

class TableReaderSparkSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  private val fs = new FsUtils(spark.sparkContext.hadoopConfiguration, "/tmp")

  private val infoDate1 = LocalDate.of(2022, 8, 5)

  "getRecordCount" when {
    "the table has info date" should {
      "work when the is the same date and there is partition" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          assert(reader.getRecordCount(query, infoDate1, infoDate1) == 2)
        }
      }

      "work when the is the same date and there is no partition" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          assert(reader.getRecordCount(query, infoDate1.minusDays(1), infoDate1.minusDays(1)) == 0)
        }
      }

      "work for info date ranges with data" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          assert(reader.getRecordCount(query, infoDate1.minusDays(1), infoDate1.plusDays(1)) == 2)
        }
      }

      "work for info date ranges with data, different range" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          assert(reader.getRecordCount(query, infoDate1.minusDays(1), infoDate1.plusDays(10)) == 5)
        }
      }

      "work for info date ranges with no data" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          assert(reader.getRecordCount(query, infoDate1.minusDays(10), infoDate1.minusDays(5)) == 0)
        }
      }

    }

    "the table doesn't have an info date" should {
      "return all data no matter date range" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, hasInfoDate = false)

          assert(reader.getRecordCount(query, infoDate1.minusDays(10), infoDate1.minusDays(5)) == 5)
        }
      }

      "throw an exception if data directory does not exist" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, hasInfoDate = false, createData = false)

          val ex = intercept[AnalysisException] {
            reader.getRecordCount(query, infoDate1.minusDays(10), infoDate1.minusDays(5))
          }
          assert(ex.getMessage().contains("Path does not exist"))
        }
      }
    }
  }

  "getData" should {
    "the table has info date" should {
      "work when the is the same date and there is partition" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          val df = reader.getData(query, infoDate1, infoDate1, Nil)

          val actual = df.toJSON.collect().mkString(",")

          assert(actual == """{"a":1,"b":"2","c":3,"info_date":"2022-08-05"},{"a":4,"b":"5","c":6,"info_date":"2022-08-05"}""")
        }
      }

      "work when the is the same date and there is partition and no schema" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, noSchema = true)

          val df = reader.getData(query, infoDate1, infoDate1, Nil)

          val actual = df.toJSON.collect().mkString(",")

          assert(actual == """{"_c0":"1","_c1":"2","_c2":"3","info_date":"2022-08-05"},{"_c0":"4","_c1":"5","_c2":"6","info_date":"2022-08-05"}""")
        }
      }

      "work when the is the same date and there is no partition" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          val dfOpt = reader.getData(query, infoDate1.minusDays(1), infoDate1.minusDays(1), Nil)

          assert(dfOpt.isEmpty)
        }
      }

      "work for info date ranges with data" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          val df = reader.getData(query, infoDate1.minusDays(1), infoDate1.plusDays(1), Nil)

          val actual = df.toJSON.collect().mkString(",")
          assert(actual == """{"a":1,"b":"2","c":3,"info_date":"2022-08-05"},{"a":4,"b":"5","c":6,"info_date":"2022-08-05"}""")
        }
      }

      "work for info date ranges with data, different range" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          val df = reader.getData(query, infoDate1.minusDays(1), infoDate1.plusDays(10), Nil)

          assert(df.count() == 5)

          assert(df.schema.fields.head.name == "a")
          assert(df.schema.fields.head.dataType == IntegerType)
          assert(df.schema.fields(1).name == "b")
          assert(df.schema.fields(1).dataType == StringType)
        }
      }

      "work for info date ranges with no data" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir)

          val df = reader.getData(query, infoDate1.minusDays(10), infoDate1.minusDays(5), Nil)

          assert(df.count() == 0)

          assert(df.schema.fields.head.name == "a")
          assert(df.schema.fields.head.dataType == IntegerType)
          assert(df.schema.fields(1).name == "b")
          assert(df.schema.fields(1).dataType == StringType)
        }
      }

    }

    "the table doesn't have an info date" should {
      "return all data no matter date range" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, hasInfoDate = false)

          val df = reader.getData(query, infoDate1.minusDays(10), infoDate1.minusDays(5), Nil)

          assert(df.count() == 5)

          assert(df.schema.fields.head.name == "a")
          assert(df.schema.fields.head.dataType == IntegerType)
          assert(df.schema.fields(1).name == "b")
          assert(df.schema.fields(1).dataType == StringType)
        }
      }

      "return all data no matter date range and no schema" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, hasInfoDate = false, noSchema = true)

          val df = reader.getData(query, infoDate1.minusDays(10), infoDate1.minusDays(5), Nil)

          assert(df.count() == 5)

          assert(df.schema.fields.head.name == "_c0")
          assert(df.schema.fields.head.dataType == StringType)
          assert(df.schema.fields(1).name == "_c1")
          assert(df.schema.fields(1).dataType == StringType)
        }
      }

      "throw an exception if data directory does not exist" in {
        withTempDirectory("spark_source") { tempDir =>
          val (reader, query) = getUseCase(tempDir, hasInfoDate = false, createData = false)

          val ex = intercept[AnalysisException] {
            reader.getData(query, infoDate1.minusDays(10), infoDate1.minusDays(5), Nil)
          }
          assert(ex.getMessage().contains("Path does not exist"))
        }
      }
    }
  }

  private def getUseCase(tempDir: String,
                         createData: Boolean = true,
                         hasInfoDate: Boolean = true,
                         noSchema: Boolean = false,
                         options: Option[Map[String, String]] = None): (TableReader, Query) = {
    val pathBase = new Path(tempDir, "test")
    val path1 = new Path(pathBase, "info_date=2022-08-05")
    val path2 = new Path(pathBase, "info_date=2022-08-08")

    if (createData) {
      fs.writeFile(new Path(path1, "test.csv"), "1,2,3\n4,5,6\n")
      fs.writeFile(new Path(path2, "test.csv"), "6,7,8\n9,0,1\n2,3,4\n")
    }

    val effectiveOptions = options.getOrElse(Map("delimiter" -> ","))

    val schemaOpt = if (noSchema) None else Some("a int, b string, c int")

    val query = Query.Path(pathBase.toString)

    (new TableReaderSpark("csv", schemaOpt, hasInfoDate, "info_date", options = effectiveOptions), query)
  }

}
